# Ring-MQTT Speaker Bridge

A standalone Docker distribution of [Ring-MQTT](https://github.com/tsightler/ring-mqtt)
with MQTT audio playback requests for [Ring Camera Messages](https://github.com/EndOfTheLine-HA/ring-camera-messages).
It uses Ring cloud services and requires a Ring account. This project is
independent of Ring and Home Assistant.

## Install on a Docker host

Use the image archive for your platform on the release page, or build the
image from the tagged source with `docker build -t ring-mqtt-speaker:0.1.0 .`.

```sh
docker load -i ring-mqtt-speaker-0.1.0-amd64.tar.gz
mkdir -p ring-mqtt-data
cp config.json ring-mqtt-data/config.json
```

In the copied configuration, set `mqtt_url` to the broker that Home Assistant
uses and set `"enable_speaker_requests": true`. If the broker requires
authentication, use the broker credentials in the configuration file and
keep that file private. Keep `enable_cameras` enabled.

```sh
docker run -d --name ring-mqtt-speaker --restart unless-stopped \
  -p 127.0.0.1:55123:55123 \
  -v "$PWD/ring-mqtt-data:/data" ring-mqtt-speaker:0.1.0
```

Open `http://localhost:55123` on the Docker host to authenticate with Ring.
For a remote host, use an SSH port forward to its loopback port 55123.
The mounted directory contains the Ring session. Keep it private and back it up.
If migrating an existing bridge, stop it and back up its data before starting
this image with that data. Run only one bridge with the same MQTT prefix.

See the upstream [Ring-MQTT wiki](https://github.com/tsightler/ring-mqtt/wiki)
for configuration and Ring account setup. Restrict MQTT write access to the
speaker request topics to trusted publishers.

## Source and license

Based on Ring-MQTT 5.9.3, with bridge fixes and the opt-in speaker request
implementation used by Ring Camera Messages. The original MIT license
and notices are included.
