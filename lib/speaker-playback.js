import { access, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { isAbsolute, join } from 'node:path'
import { tmpdir } from 'node:os'
import { setTimeout as delay } from 'node:timers/promises'
import debugModule from 'debug'
import mqtt from './mqtt.js'
import ring from './ring.js'
import utils from './utils.js'

const debug = debugModule('ring-speaker')
const pressPayload = 'PRESS'
const boundClients = new Map()
let playing = false

async function playButton(cameraName, audioFile) {
    const cameras = ring.devices.map(({ device }) => device).filter(device =>
        device?.name === cameraName && typeof device.startLiveCall === 'function'
    )
    if (cameras.length !== 1) {
        throw new Error('Expected exactly one camera matching speaker_playback.camera')
    }

    await play(cameras[0], audioFile)
}

async function play(camera, audioFile) {
    await access(audioFile)
    const opening = camera.startLiveCall()
    const connectTimeout = new AbortController()
    let session
    try {
        session = await Promise.race([
            opening,
            delay(15000, undefined, { signal: connectTimeout.signal }).then(() => {
                throw new Error('Timed out opening Ring Live View')
            })
        ])
    } catch (error) {
        // Close any call that connects after the request timed out.
        opening.then(lateSession => lateSession.stop()).catch(() => {})
        throw error
    } finally {
        connectTimeout.abort()
    }
    const ended = new Promise(resolve => session.onCallEnded.subscribe(() => resolve()))
    const controller = new AbortController()
    const limit = setTimeout(() => session.stop(), 20000)
    try {
        session.activateCameraSpeaker()
        await delay(1500)
        const usingOpus = await Promise.race([session.isUsingOpus, ended])
        if (usingOpus === undefined) throw new Error('Call ended before audio negotiation')
        // A 12-byte RTP header plus 160 PCMU samples gives a 20 ms packet.
        const packetOptions = usingOpus ? ['-frame_duration', '20'] : ['-pkt_size', '172']
        debug('Starting return audio with %s', usingOpus ? 'Opus' : 'PCMU')
        await Promise.race([
            session.transcodeReturnAudio({ input: [audioFile, ...packetOptions] }),
            ended
        ])
        await Promise.race([ended, delay(17500, undefined, { signal: controller.signal })])
    } finally {
        clearTimeout(limit)
        controller.abort()
        session.stop()
    }
}

function registerButton(config, registeredIds) {
    if (!config || typeof config !== 'object') return
    const { camera, file, id = 'ring_speaker_playback', name = 'Ring Speaker Playback' } = config
    if (typeof camera !== 'string' || !camera.trim() ||
        typeof file !== 'string' || !isAbsolute(file) ||
        typeof id !== 'string' || !/^[a-z0-9_]+$/.test(id) ||
        typeof name !== 'string' || !name.trim()) {
        debug('Invalid speaker_playback configuration')
        return
    }

    if (registeredIds.has(id)) {
        debug('Ignoring duplicate speaker_playback ID: %s', id)
        return
    }
    registeredIds.add(id)

    const command = `${utils.config().ring_topic}/speaker/${id}/press`
    const discovery = `homeassistant/button/${id}/config`
    const client = mqtt.client
    if (boundClients.get(id) !== client) {
        boundClients.set(id, client)
        client.on('message', async (topic, payload, packet) => {
            if (topic !== command || packet.retain || payload.toString() !== pressPayload || playing) return
            playing = true
            try {
                await playButton(camera, file)
            } catch (error) {
                debug('Playback failed: %s', error.message)
            } finally {
                playing = false
            }
        })
    }

    client.subscribe(command, (error, grants) => {
        if (error || grants?.[0]?.qos === 128) {
            debug('Could not subscribe to the speaker playback command topic')
            return
        }
        client.publish(discovery, JSON.stringify({
            unique_id: id,
            object_id: id,
            name,
            command_topic: command,
            payload_press: pressPayload,
            icon: 'mdi:bullhorn-outline'
        }), { qos: 1, retain: true })
    })
}

utils.event.on('mqtt_state', state => {
    if (state !== 'connected') return
    const config = utils.config().speaker_playback
    if (utils.config().enable_speaker_requests === true) registerRequests(mqtt.client)
    const registeredIds = new Set()
    for (const button of Array.isArray(config) ? config : [config]) {
        registerButton(button, registeredIds)
    }
})

const maxAudioBytes = 48_000 * 2 * 2 * 17 + 44
const requestIdPattern = /^[a-f0-9]{32}$/
const recentRequests = new Map()
let requestClient

function getCameras() {
    return ring.devices.map(({ device }) => device).filter(device =>
        device?.id != null && typeof device.startLiveCall === 'function'
    )
}

function decodeAudio(encoded) {
    if (typeof encoded !== 'string' ||
        !/^[A-Za-z0-9+/]+={0,2}$/.test(encoded) || encoded.length % 4 !== 0) {
        throw new Error('Invalid base64 audio')
    }
    const data = Buffer.from(encoded, 'base64')
    // Accept canonical 48 kHz stereo PCM WAV only. No URLs or alternate demuxers.
    if (data.length <= 44 || data.length > maxAudioBytes || data.length % 4 !== 0 ||
        data.toString('ascii', 0, 4) !== 'RIFF' || data.readUInt32LE(4) !== data.length - 8 ||
        data.toString('ascii', 8, 16) !== 'WAVEfmt ' || data.readUInt32LE(16) !== 16 ||
        data.readUInt16LE(20) !== 1 || data.readUInt16LE(22) !== 2 ||
        data.readUInt32LE(24) !== 48000 || data.readUInt32LE(28) !== 192000 ||
        data.readUInt16LE(32) !== 4 || data.readUInt16LE(34) !== 16 ||
        data.toString('ascii', 36, 40) !== 'data' || data.readUInt32LE(40) !== data.length - 44) {
        throw new Error('Expected a 48 kHz stereo PCM WAV of at most 17 seconds')
    }
    return data
}

function registerRequests(client) {
    const topic = `${utils.config().ring_topic}/speaker/v1`
    if (requestClient !== client) {
        requestClient = client
        client.on('message', async (received, payload, packet) => {
            if (packet.retain || ![`${topic}/discover`, `${topic}/play`].includes(received) ||
                payload.length > Math.ceil(maxAudioBytes / 3) * 4 + 512) return
            let request
            try {
                request = JSON.parse(payload.toString())
            } catch {
                return
            }
            if (typeof request?.id !== 'string' || !requestIdPattern.test(request.id)) return
            const reply = result => client.publish(
                `${topic}/result/${request.id}`, JSON.stringify(result), { qos: 1 }
            )
            if (received.endsWith('/discover')) {
                reply({ status: 'ready', cameras: getCameras().map(camera => ({
                    id: String(camera.id), name: camera.name
                })) })
                return
            }

            // QoS 1 can redeliver a command. Suppress duplicates for this process.
            if (recentRequests.has(request.id)) return
            recentRequests.set(request.id, true)
            if (recentRequests.size > 256) recentRequests.delete(recentRequests.keys().next().value)
            if (playing) {
                reply({ status: 'busy' })
                return
            }
            const camera = getCameras().find(device => String(device.id) === request.camera)
            if (!camera) {
                reply({ status: 'error', error: 'Unknown camera' })
                return
            }
            playing = true
            let directory
            try {
                const audio = decodeAudio(request.audio)
                directory = await mkdtemp(join(tmpdir(), 'ring-speaker-'))
                const file = join(directory, 'message.wav')
                await writeFile(file, audio, { mode: 0o600 })
                reply({ status: 'accepted' })
                await play(camera, file)
                reply({ status: 'ended' })
            } catch (error) {
                debug('Requested playback failed: %s', error.message)
                reply({ status: 'error', error: 'Playback failed. See Ring-MQTT logs.' })
            } finally {
                playing = false
                if (directory) await rm(directory, { recursive: true, force: true }).catch(error => {
                    debug('Could not remove temporary audio: %s', error.message)
                })
            }
        })
    }
    client.subscribe([`${topic}/discover`, `${topic}/play`], { qos: 1 })
}
