import chalk from 'chalk'
import utils from './utils.js'
import { spawn } from 'child_process'
import readline from 'readline'
import yaml from 'js-yaml'
import { dirname } from 'path'
import { fileURLToPath } from 'url'
import writeFileAtomic from 'write-file-atomic'
import debugModule from 'debug'
const debug = debugModule('ring-rtsp')
const preloadApiPort = 1984

export default new class Go2RTC {
    constructor() {
        this.started = false
        this.go2rtcProcess = false
        this.cameras = []
        this.preloadCameras = []
        this.preloadTimer = null
        this.preloadBusy = false
    }

    // Writes the configuration and starts go2rtc. started only becomes true once both have
    // succeeded. It used to be set on the first line, while a failed config write was logged and
    // swallowed and a failed spawn had no listener at all, so a partial start could leave go2rtc
    // without camera streams, or not running, while the guard in ring.js told every later
    // discovery there was nothing left to do. Failures now reject so discovery retries this.
    async init(cameras) {
        debug(chalk.green('-'.repeat(90)))
        debug('Creating go2rtc configuration and starting go2rtc process...')

        if (cameras) {
            this.cameras = cameras
            this.preloadCameras = this.getPreloadCameras()
        }

        const configFile = (process.env.RUNMODE === 'standard')
            ? dirname(fileURLToPath(new URL('.', import.meta.url)))+'/config/go2rtc.yaml'
            : '/data/go2rtc.yaml'

        let config = {
            log: {
                level: 'debug',
                hass: 'info'
            },
            api: {
                // The API is only needed to manage preloaded streams, and then only on loopback
                listen: this.preloadCameras.length > 0 ? `127.0.0.1:${preloadApiPort}` : ''
            },
            srtp: {
                listen: ''
            },
            rtsp: {
                listen: ':8554',
                ...(utils.config().livestream_user && utils.config().livestream_pass)
                    ? {
                        username: utils.config().livestream_user,
                        password: utils.config().livestream_pass
                    } : {},
                default_query: 'video&audio=aac&audio=opus'
            },
            webrtc: {
                listen: ''
            }
        }

        if (cameras) {
            config.streams = {}
            for (const camera of cameras) {
                config.streams[`${camera.deviceId}_live`] =
                    `exec:${dirname(fileURLToPath(new URL('.', import.meta.url)))}/scripts/start-stream.sh ${camera.deviceId} live ${camera.deviceTopic} {output}#killsignal=15`
                config.streams[`${camera.deviceId}_event`] =
                    `exec:${dirname(fileURLToPath(new URL('.', import.meta.url)))}/scripts/start-stream.sh ${camera.deviceId} event ${camera.deviceTopic} {output}#killsignal=15`
            }
            await writeFileAtomic(configFile, yaml.dump(config, { lineWidth: -1 }))
            debug('Successfully wrote go2rtc configuration file: '+configFile)
        }

        await this.startProcess(configFile)
        if (this.started === 'shutdown') {
            // shut down while it was starting: do not resurrect the flag or leave a process behind
            if (this.go2rtcProcess) {
                this.go2rtcProcess.kill()
                this.go2rtcProcess = false
            }
            return
        }
        this.started = true

        if (this.preloadCameras.length > 0 && !this.preloadTimer) {
            this.preloadTimer = setInterval(() => this.checkPreloads(), 30000)
        }
    }

    // Resolves once the process has actually spawned, rejects if it cannot be spawned
    startProcess(configFile) {
        return new Promise((resolve, reject) => {
            const go2rtcProcess = spawn('go2rtc', ['-config', configFile], {
                env: process.env,   // set env vars
                cwd: '.',           // set cwd
                stdio: 'pipe'       // forward stdio options
            })
            this.go2rtcProcess = go2rtcProcess
            let spawned = false

            go2rtcProcess.once('spawn', async () => {
                spawned = true
                debug('The go2rtc process was started successfully')
                resolve()
                await utils.sleep(2) // Give the process a second to start the API server
                debug(chalk.green('-'.repeat(90)))
            })

            // Without a listener a spawn failure, such as a missing binary, was thrown as an
            // uncaught exception. Hand it to the caller instead.
            go2rtcProcess.on('error', (err) => {
                if (spawned) {
                    debug(chalk.red('go2rtc process error: '+err.message))
                } else {
                    reject(err)
                }
            })

            go2rtcProcess.on('close', async () => {
                await utils.sleep(1) // Delay to avoid spurious messages if shutting down
                // A process that never spawned is the caller's failure to handle, not a crash to
                // restart, and nothing is restarted after shutdown
                if (!spawned || this.started === 'shutdown') {
                    return
                }
                debug('The go2rtc process exited unexpectedly, will restart in 5 seconds...')
                this.started = false  // it is not running, so the preload checks stand down until it is
                go2rtcProcess.kill(9)  // Sometimes rtsp-simple-server crashes and doesn't exit completely, try to force kill it
                await utils.sleep(5)
                this.restart()
            })

            const stdoutLine = readline.createInterface({ input: go2rtcProcess.stdout })
            stdoutLine.on('line', (line) => {
                // Replace time in go2rtc log messages with tag
                debug(line.replace(/^.*\d{2}:\d{2}:\d{2}\.\d{3} /, chalk.green('[go2rtc] ')))
            })

            const stderrLine = readline.createInterface({ input: go2rtcProcess.stderr })
            stderrLine.on('line', (line) => {
                // Replace time in go2rtc log messages with tag
                debug(line.replace(/^.*\d{2}:\d{2}:\d{2}\.\d{3} /, chalk.green('[go2rtc] ')))
            })
        })
    }

    // Restart after an unexpected exit, reusing the configuration already on disk. init() can
    // reject now, and an unhandled rejection is only logged, so keep trying rather than leave the
    // cameras with no stream server.
    async restart() {
        while (this.started !== 'shutdown') {
            try {
                await this.init()
                return
            } catch (err) {
                debug(chalk.red('Failed to restart go2rtc: '+err.message+', retrying in 30 seconds'))
                await utils.sleep(30)
            }
        }
    }

    // Cameras listed in the optional livestream_preload config (device id or camera name) get a
    // go2rtc preload: a permanent internal consumer that keeps the live stream running with or
    // without clients. go2rtc then never stops the Ring session just because the last RTSP client
    // disconnected for a moment (for example while an NVR restarts its ffmpeg), and it restarts
    // the stream by itself whenever the Ring session ends. Battery cameras are never preloaded.
    getPreloadCameras() {
        const wanted = utils.config().livestream_preload
        if (!Array.isArray(wanted) || wanted.length === 0) {
            return []
        }
        const wantedIds = wanted.map(w => String(w).toLowerCase())
        const preloadCameras = []
        for (const camera of this.cameras) {
            if (!wantedIds.includes(camera.deviceId.toLowerCase()) && !wantedIds.includes(camera.deviceData.name.toLowerCase())) {
                continue
            }
            if (camera.device.operatingOnBattery) {
                debug(chalk.yellow(`Not preloading live stream for battery camera ${camera.deviceData.name} (${camera.deviceId})`))
                continue
            }
            debug(`Live stream for ${camera.deviceData.name} (${camera.deviceId}) will be kept running (livestream_preload)`)
            preloadCameras.push(camera)
        }
        for (const w of wantedIds) {
            if (!this.cameras.some(c => c.deviceId.toLowerCase() === w || c.deviceData.name.toLowerCase() === w)) {
                debug(chalk.yellow(`livestream_preload entry "${w}" does not match any camera id or name`))
            }
        }
        return preloadCameras
    }

    // go2rtc only applies a preload once and never retries one that failed, so ring-mqtt adds
    // them through the local API instead, only after the camera is subscribed to its stream
    // command topic, and re-adds any that are missing (failed start, go2rtc restart)
    async checkPreloads() {
        if (this.preloadBusy || this.started !== true || !this.go2rtcProcess) {
            return
        }
        this.preloadBusy = true
        try {
            const response = await fetch(`http://127.0.0.1:${preloadApiPort}/api/preload`, { signal: AbortSignal.timeout(5000) })
            const active = response.ok ? await response.json() : {}
            for (const camera of this.preloadCameras) {
                const streamName = `${camera.deviceId}_live`
                if (active?.[streamName] || !camera.entity?.stream?.published || !camera.isOnline()) {
                    continue
                }
                debug(`Adding go2rtc preload for ${camera.deviceData.name} (${streamName})`)
                // go2rtc answers once the stream has started, which takes a few seconds
                const result = await fetch(`http://127.0.0.1:${preloadApiPort}/api/preload?src=${encodeURIComponent(streamName)}&video&audio`, {
                    method: 'PUT',
                    signal: AbortSignal.timeout(45000)
                })
                if (result.ok) {
                    debug(`go2rtc preload for ${camera.deviceData.name} is active`)
                } else {
                    debug(chalk.yellow(`go2rtc preload for ${camera.deviceData.name} failed (HTTP ${result.status}), will retry`))
                }
            }
        } catch (error) {
            debug(chalk.yellow(`go2rtc preload check failed, will retry: ${error.message}`))
        } finally {
            this.preloadBusy = false
        }
    }

    shutdown() {
        this.started = 'shutdown'
        if (this.preloadTimer) {
            clearInterval(this.preloadTimer)
            this.preloadTimer = null
        }
        if (this.go2rtcProcess) {
            this.go2rtcProcess.kill()
            this.go2rtcProcess = false
        }
        return
    }
}
