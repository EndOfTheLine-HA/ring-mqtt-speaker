// This code is largely copied from ring-client-api, but converted from Typescript
// to native Javascript with custom logging for ring-mqtt and some unused code removed.
// Much thanks to @dgreif for the original code which is the basis for this work.

import { RTCPeerConnection, RTCRtpCodecParameters } from 'werift'
import { interval, merge, ReplaySubject, Subject } from 'rxjs'
import { Subscribed } from './subscribed.js'

const ringIceServers = [
    'stun:stun.kinesisvideo.us-east-1.amazonaws.com:443',
    'stun:stun.kinesisvideo.us-east-2.amazonaws.com:443',
    'stun:stun.kinesisvideo.us-west-2.amazonaws.com:443',
    'stun:stun.l.google.com:19302',
    'stun:stun1.l.google.com:19302',
    'stun:stun2.l.google.com:19302',
    'stun:stun3.l.google.com:19302',
    'stun:stun4.l.google.com:19302',
]

export class WeriftPeerConnection extends Subscribed {
    constructor() {
        super()
        this.onAudioRtp = new Subject()
        this.onVideoRtp = new Subject()
        this.onIceCandidate = new Subject()
        this.onConnectionState = new ReplaySubject(1)
        this.onRequestKeyFrame = new Subject()
        const pc = (this.pc = new RTCPeerConnection({
            codecs: {
                audio: [
                    new RTCRtpCodecParameters({
                        mimeType: 'audio/opus',
                        clockRate: 48000,
                        channels: 2,
                    }),
                    new RTCRtpCodecParameters({
                        mimeType: 'audio/PCMU',
                        clockRate: 8000,
                        channels: 1,
                        payloadType: 0,
                    }),
                ],
                video: [
                    // Offer High profile Level 5.0 first, then fall back to the original
                    // Baseline Level 3.1 below.
                    //
                    // Ring answers with the level it is offered. With only 42001f on offer
                    // (Baseline, level 0x1f = 3.1) a 2K Floodlight Cam Pro came back as
                    // profile-level-id=42C01F and delivered 1274x720. Level 3.1 caps a frame at
                    // 3600 macroblocks, which is exactly 1280x720, so the 720p was not a side
                    // effect of Ring's HEVC->H.264 transcode, it was the ceiling of the level we
                    // asked for. 2560x1440 is 14400 macroblocks and needs level 0x32 (5.0).
                    //
                    // Ring may still answer 42C01F, either because its transcoder is pinned to
                    // 720p or because it just mirrors the first offer it can satisfy. Keeping the
                    // original entry as a fallback means a rejection degrades to today's
                    // behaviour instead of taking the camera offline.
                    //
                    // Each H.264 entry is followed immediately by its own rtx entry. werift
                    // numbers these codecs from 96 in list order and builds the retransmission
                    // association as apt = (own payload type - 1), so an rtx only repairs the
                    // entry directly above it. With one shared rtx at the end of the list it
                    // bound to the fallback, leaving the preferred stream Ring actually selects
                    // with no way to repair lost packets, and a sender that cannot repair loss
                    // tends to drop resolution instead.
                    new RTCRtpCodecParameters({
                        mimeType: 'video/H264',
                        clockRate: 90000,
                        rtcpFeedback: [
                            { type: 'transport-cc' },
                            { type: 'ccm', parameter: 'fir' },
                            { type: 'nack' },
                            { type: 'nack', parameter: 'pli' },
                            { type: 'goog-remb' },
                        ],
                        parameters: 'packetization-mode=1;profile-level-id=640032;level-asymmetry-allowed=1',
                    }),
                    new RTCRtpCodecParameters({
                        mimeType: "video/rtx",
                        clockRate: 90000,
                    }),
                    new RTCRtpCodecParameters({
                        mimeType: 'video/H264',
                        clockRate: 90000,
                        rtcpFeedback: [
                            { type: 'transport-cc' },
                            { type: 'ccm', parameter: 'fir' },
                            { type: 'nack' },
                            { type: 'nack', parameter: 'pli' },
                            { type: 'goog-remb' },
                        ],
                        parameters: 'packetization-mode=1;profile-level-id=42001f;level-asymmetry-allowed=1',
                    }),
                    new RTCRtpCodecParameters({
                        mimeType: "video/rtx",
                        clockRate: 90000,
                    })
                ],
            },
            iceServers: ringIceServers.map((server) => ({ urls: server })),
            iceTransportPolicy: 'all',
            bundlePolicy: 'disable'
        }))

        const audioTransceiver = pc.addTransceiver('audio', {
            direction: 'sendrecv',
        })

        const videoTransceiver = pc.addTransceiver('video', {
            direction: 'recvonly',
        })

        audioTransceiver.onTrack.subscribe((track) => {
            track.onReceiveRtp.subscribe((rtp) => {
                this.onAudioRtp.next(rtp)
            })
        })

        videoTransceiver.onTrack.subscribe((track) => {
            track.onReceiveRtp.subscribe((rtp) => {
                this.onVideoRtp.next(rtp)
            })
            track.onReceiveRtp.once(() => {
                // debug('received first video packet')
                this.addSubscriptions(merge(this.onRequestKeyFrame, interval(4000)).subscribe(() => {
                    videoTransceiver.receiver
                        .sendRtcpPLI(track.ssrc)
                        .catch()
                }))
                this.requestKeyFrame()
            })
        })

        this.pc.onIceCandidate.subscribe((iceCandidate) => {
            if (iceCandidate) {
                this.onIceCandidate.next(iceCandidate)
            }
        })

        pc.iceConnectionStateChange.subscribe(() => {
            // debug(`iceConnectionStateChange: ${pc.iceConnectionState}`)
            if (pc.iceConnectionState === 'closed') {
                this.onConnectionState.next('closed')
            }
        })

        pc.connectionStateChange.subscribe(() => {
            // debug(`connectionStateChange: ${pc.connectionState}`)
            this.onConnectionState.next(pc.connectionState)
        })
    }

    async createOffer() {
        const offer = await this.pc.createOffer()
        await this.pc.setLocalDescription(offer)
        return offer
    }

    async acceptAnswer(answer) {
        await this.pc.setRemoteDescription(answer)
    }

    addIceCandidate(candidate) {
        return this.pc.addIceCandidate(candidate)
    }

    requestKeyFrame() {
        this.onRequestKeyFrame.next()
    }

    close() {
        this.pc.close().catch()
        this.unsubscribe()
    }
}
