import { parentPort, workerData } from 'worker_threads'
import { WebrtcConnection } from '../lib/streaming/webrtc-connection.js'
import { StreamingSession } from '../lib/streaming/streaming-session.js'

const deviceName = workerData.deviceName
const doorbotId = workerData.doorbotId
let liveStream = false
let streamStopping = false
let startWaiting = false
let startCancelled = false

parentPort.on("message", async(data) => {
    const streamData = data.streamData
    switch (data.command) {
        case 'start':
            // go2rtc often reconnects within milliseconds of a stop, so give a stop that is
            // still in progress up to 3 seconds to finish instead of failing the new request
            if (streamStopping) {
                startWaiting = true
                startCancelled = false
                for (let i = 0; streamStopping && i < 30; i++) {
                    await new Promise(res => setTimeout(res, 100))
                }
                startWaiting = false
            }
            if (startCancelled) {
                // A stop arrived while this start was waiting, so the start is no longer wanted
                startCancelled = false
                parentPort.postMessage({type: 'log_info', data: 'Live stream start was cancelled by a stop request'})
                parentPort.postMessage({type: 'state', data: 'inactive'})
            } else if (streamStopping) {
                parentPort.postMessage({type: 'log_error', data: "Live stream could not be started because it is in stopping state"})
                parentPort.postMessage({type: 'state', data: 'failed'})
            } else if (!liveStream) {
                startLiveStream(streamData)
            } else {
                parentPort.postMessage({type: 'log_error', data: "Live stream could not be started because there is already an active stream"})
                parentPort.postMessage({type: 'state', data: 'active'})
            }
            break;
        case 'stop':
            if (liveStream) {
                stopLiveStream()
            } else if (startWaiting) {
                startCancelled = true
            }
            break;
    }
})

async function startLiveStream(streamData) {
    parentPort.postMessage({type: 'log_info', data: 'Live stream WebRTC worker received start command'})
    let session = false
    try {
        const cameraData = {
            name: deviceName,
            id: doorbotId
        }

        const streamConnection = new WebrtcConnection(streamData.ticket, cameraData)
        // Keep a local reference: the callbacks below must only ever touch this session, never
        // a newer one that was started after this one ended
        session = new StreamingSession(cameraData, streamConnection)
        liveStream = session

        // Subscribe before anything can end the session (onCallEnded is a ReplaySubject)
        session.onCallEnded.subscribe(() => {
            parentPort.postMessage({type: 'log_info', data: 'Live stream WebRTC session has disconnected'})
            if (liveStream === session) {
                parentPort.postMessage({type: 'state', data: 'inactive'})
                liveStream = false
            }
        })

        session.connection.pc.onConnectionState.subscribe((data) => {
            switch(data) {
                case 'connected':
                    if (liveStream === session) {
                        parentPort.postMessage({type: 'state', data: 'active'})
                    }
                    parentPort.postMessage({type: 'log_info', data: 'Live stream WebRTC session is connected'})
                    break;
                case 'failed':
                    parentPort.postMessage({type: 'log_info', data: 'Live stream WebRTC connection has failed'})
                    if (liveStream === session) {
                        parentPort.postMessage({type: 'state', data: 'failed'})
                        liveStream = false
                    }
                    session.stop('peer connection failed')
                    break;
            }
        })

        parentPort.postMessage({type: 'log_info', data: 'Live stream transcoding process is starting'})
        await session.startTranscoding({
            // The native AVC video stream is copied to the RTSP server unmodified while the audio
            // stream is converted into two output streams using both AAC and Opus codecs.  This
            // provides a stream with wide compatibility across various media player technologies.
            audio: [
                '-map', '0:v',
                '-map', '0:a',
                '-map', '0:a',
                '-c:a:0', 'aac',
                '-c:a:1', 'copy',
            ],
            video: [
                '-c:v', 'copy'
            ],
            output: [
                '-flags', '+global_header',
                '-f', 'rtsp',
                '-rtsp_transport', 'tcp',
                streamData.rtspPublishUrl
            ]
        })

        if (!session.hasEnded) {
            parentPort.postMessage({type: 'log_info', data: 'Live stream transcoding process has started'})
        }
    } catch(error) {
        parentPort.postMessage({type: 'log_error', data: error?.stack || String(error)})
        parentPort.postMessage({type: 'state', data: 'failed'})
        if (session) {
            session.stop('error while starting live stream')
        }
        if (liveStream === session) {
            liveStream = false
        }
    }
}

async function stopLiveStream() {
    if (!streamStopping) {
        streamStopping = true
        let stopTimeout = 10
        liveStream.stop('stop requested by ring-mqtt (RTSP consumer gone or stream switched off)')
        do {
            await new Promise(res => setTimeout(res, 200))
            if (liveStream) {
                parentPort.postMessage({type: 'log_info', data: 'Live stream failed to stop on request, deleting anyway...'})
                parentPort.postMessage({type: 'state', data: 'inactive'})
                liveStream = false
            }
            stopTimeout--
        } while (liveStream && stopTimeout)
        streamStopping = false
    }
}