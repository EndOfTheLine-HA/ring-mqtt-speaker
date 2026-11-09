// This code is largely copied from ring-client-api, but converted from Typescript
// to native Javascript with custom logging for ring-mqtt and some unused code removed.
// Much thanks to @dgreif for the original code which is the basis for this work.

import { FfmpegProcess, reservePorts, RtpSplitter } from '@homebridge/camera-utils'
import { firstValueFrom, ReplaySubject, Subject } from 'rxjs'
import pathToFfmpeg from 'ffmpeg-for-homebridge'
import { concatMap, take } from 'rxjs/operators'
import { Subscribed } from './subscribed.js'

function getCleanSdp(sdp) {
    return sdp
        .split('\nm=')
        .slice(1)
        .map((section) => 'm=' + section)
        .join('\n')
}

export class StreamingSession extends Subscribed {
    constructor(camera, connection) {
        super()
        this.camera = camera
        this.connection = connection
        this.onCallEnded = new ReplaySubject(1)
        this.onUsingOpus = new ReplaySubject(1)
        this.onVideoRtp = new Subject()
        this.onAudioRtp = new Subject()
        this.audioSplitter = new RtpSplitter()
        this.videoSplitter = new RtpSplitter()
        this.hasEnded = false
        this.bindToConnection(connection)
    }

    bindToConnection(connection) {
        this.addSubscriptions(
            connection.onAudioRtp.subscribe(this.onAudioRtp),
            connection.onVideoRtp.subscribe(this.onVideoRtp),
            connection.onCallAnswered.subscribe((sdp) => {
                this.onUsingOpus.next(sdp.toLocaleLowerCase().includes(' opus/'))
            }),
            connection.onCallEnded.subscribe(() => this.callEnded('WebRTC connection ended')))
    }

    async reservePort(bufferPorts = 0) {
        const ports = await reservePorts({ count: bufferPorts + 1 })
        return ports[0]
    }

    get isUsingOpus() {
        return firstValueFrom(this.onUsingOpus)
    }

    async startTranscoding(ffmpegOptions) {
        if (this.hasEnded) {
            return
        }
        const videoPort = await this.reservePort(1)
        const audioPort = await this.reservePort(1)

        const ringSdp = await Promise.race([
            firstValueFrom(this.connection.onCallAnswered),
            firstValueFrom(this.onCallEnded),
        ])

        if (!ringSdp) {
            // Call ended before answered'
            return
        }
        const usingOpus = await this.isUsingOpus

        const ffmpegInputArguments = [
            '-hide_banner',
            // No periodic progress lines on stderr, only real messages
            '-nostats',
            // Machine readable progress on stdout every second, used by the stall watchdog below
            '-progress', 'pipe:1',
            '-stats_period', '1',
            '-protocol_whitelist',
            'pipe,udp,rtp,file,crypto',
            // Ring will answer with either opus or pcmu
            ...(usingOpus ? ['-acodec', 'libopus'] : []),
            '-f',
            'sdp',
            ...(ffmpegOptions.input || []),
            '-i',
            'pipe:'
        ]

        const inputSdp = getCleanSdp(ringSdp)
            .replace(/m=audio \d+/, `m=audio ${audioPort}`)
            .replace(/m=video \d+/, `m=video ${videoPort}`)

        // Keep the last few lines ffmpeg writes to stderr so that an unexpected exit can be
        // explained in the log, otherwise ffmpeg output is discarded
        const ffmpegOutput = []

        // Stall watchdog. ffmpeg can block forever writing its RTSP output when the server stops
        // reading the connection without closing it (go2rtc 1.9.14 does this to a publisher after a
        // read timeout when its reconnect fails). It then stops reading the RTP inputs, ignores
        // SIGTERM, and the session looks active forever. Neither -timeout nor -rw_timeout makes
        // ffmpeg give up on a blocked RTSP/TCP write, so ring-mqtt watches ffmpeg's own progress
        // report and kills it when frames written and output time stop advancing. (total_size is
        // always N/A for RTSP output; the report itself keeps coming every second while blocked.)
        const stallTimeout = (ffmpegOptions.stallTimeout || 20) * 1000
        const lastProgress = { frame: null, outTime: null, time: Date.now() }
        let stallReason = null
        const onProgress = (data) => {
            // stdout chunks can split a report, so each value is compared on its own
            const text = String(data)
            const frame = text.match(/frame=(\d+)/g)?.pop()
            const outTime = text.match(/out_time_us=(\d+)/g)?.pop()
            if (frame && frame !== lastProgress.frame) {
                lastProgress.frame = frame
                lastProgress.time = Date.now()
            }
            if (outTime && outTime !== lastProgress.outTime) {
                lastProgress.outTime = outTime
                lastProgress.time = Date.now()
            }
        }

        const ff = new FfmpegProcess({
            ffmpegArgs: ffmpegInputArguments.concat(
                ...(ffmpegOptions.audio || ['-acodec', 'aac']),
                ...(ffmpegOptions.video || ['-vcodec', 'copy']),
                ...(ffmpegOptions.output || [])),
            ffmpegPath: pathToFfmpeg,
            logger: {
                info: (data) => {
                    ffmpegOutput.push(...String(data).split(/[\r\n]+/).filter(line => line.trim()).map(line => line.slice(0, 300)))
                    ffmpegOutput.splice(0, Math.max(0, ffmpegOutput.length - 5))
                },
                error: () => {}
            },
            stdoutCallback: onProgress,
            exitCallback: (code, signal) => {
                clearInterval(stallTimer)
                if (!this.hasEnded) {
                    this.callEnded(`${stallReason || 'ffmpeg exited'} (code ${code}, signal ${signal})${ffmpegOutput.length ? ': '+ffmpegOutput.join(' | ') : ''}`)
                }
            }
        })

        const stallTimer = setInterval(() => {
            if (!this.hasEnded && !ff.exited && Date.now() - lastProgress.time > stallTimeout) {
                stallReason = `ffmpeg made no progress for ${Math.round((Date.now() - lastProgress.time) / 1000)} seconds, killed it`
                clearInterval(stallTimer)
                ff.ff.kill('SIGKILL')
            }
        }, 2000)

        this.addSubscriptions(this.onAudioRtp.pipe(concatMap((rtp) => {
            return this.audioSplitter.send(rtp.serialize(), { port: audioPort })
        })).subscribe())

        this.addSubscriptions(this.onVideoRtp.pipe(concatMap((rtp) => {
            return this.videoSplitter.send(rtp.serialize(), { port: videoPort })
        })).subscribe())

        this.onCallEnded.pipe(take(1)).subscribe(() => {
            clearInterval(stallTimer)
            ff.stop()
            // A blocked ffmpeg ignores SIGTERM, make sure it cannot outlive the session
            setTimeout(() => {
                if (!ff.exited) {
                    ff.ff.kill('SIGKILL')
                }
            }, 5000)
        })

        ff.writeStdin(inputSdp)

        // Request a key frame now that ffmpeg is ready to receive
        this.requestKeyFrame()
    }

    callEnded(reason) {
        if (this.hasEnded) {
            return
        }
        this.hasEnded = true
        this.unsubscribe()
        // Stop the connection first so the end reason is logged before the disconnect
        this.connection.stop(reason)
        this.onCallEnded.next()
        this.audioSplitter.close()
        this.videoSplitter.close()
    }

    stop(reason = 'stop requested') {
        this.callEnded(reason)
    }

    requestKeyFrame() {
        this.connection.requestKeyFrame()
    }
}
