#!/bin/bash
# Activate Ring camera video stream via ring-mqtt
#
# This script is intended for use only with ring-mqtt
# and go2rtc.
#
# Requires mosquitto MQTT clients package to be installed
# Uses ring-mqtt internal IPC broker for communication with
# ring-mqtt process
#
# Spawns stream control in background due to issues with
# process exit hanging go2rtc.  Script then just monitors
# for control script to exit or, if script is killed,
# sends commands to control script prior to exiting

# Required command line arguments
device_id=${1}     # Camera device Id
type=${2}          # Stream type ("live" or "event")
base_topic=${3}    # Command topic for Camera entity
rtsp_pub_url=${4}  # URL for publishing RTSP stream
client_id="${device_id}_${type}"  # Id used to connect to the MQTT broker, camera Id + event type

# Build the MQTT topics
[[ ${type} = "live" ]] && base_topic="${base_topic}/stream" || base_topic="${base_topic}/event_stream"
json_attribute_topic="${base_topic}/attributes"
command_topic="${base_topic}/command"
debug_topic="${base_topic}/debug"

# The lock file holds the PID of the run that owns it, so a lock left behind by a run that was
# killed (SIGKILL skips the cleanup trap) does not block this stream forever
lock_file=/tmp/ring-mqtt-${client_id}.lock
lock_held() {
    test -f ${lock_file} || return 1
    local lock_pid=$(cat ${lock_file} 2>/dev/null)
    [ -z "${lock_pid}" ] && return 0  # lock from an older version without a PID
    kill -0 ${lock_pid} 2>/dev/null && return 0
    rm -f ${lock_file}
    return 1
}

# If the previous run hasn't exited yet (go2rtc retries a producer immediately after it
# ends), wait up to 2 seconds for it to finish
for i in $(seq 1 20); do
    lock_held || break
    sleep .1
done

# Still running: go2rtc has started a new run while the old one is alive, so it has given up on
# the old run's publisher. go2rtc 1.9.14 does not close that connection or stop the old script
# when its reconnect fails, so the old Ring session stayed up (its ffmpeg blocked writing to a
# connection nobody reads) while every new run failed on this lock. Ask ring-mqtt to end the
# old session, which makes the old run exit, then carry on.
if lock_held; then
    mosquitto_pub -i "${client_id}_pub" -L "mqtt://127.0.0.1:51883/${debug_topic}" -m "Previous ${type} stream run is still active but go2rtc started a new one, ending the old session"
    mosquitto_pub -i "${client_id}_pub" -L "mqtt://127.0.0.1:51883/${command_topic}" -m "STOP-SESSION"
    for i in $(seq 1 100); do
        lock_held || break
        sleep .1
    done
fi
if lock_held; then
    exit 1
fi
echo $$ > ${lock_file}

script_dir=$(dirname "$0")
${script_dir}/monitor-stream.sh ${1} ${2} ${3} ${4} &

# Set some colors for debug output
red='\e[0;31m'
yellow='\e[0;33m'
green='\e[0;32m'
blue='\e[0;34m'
reset='\e[0m'

stop() {
    # Only send the stop commands when go2rtc interrupted us with a signal. When the monitor
    # script exits on its own the stream has already ended, and sending OFF then would also
    # kill a manually started keepalive stream on every Ring session rollover
    if [ "${1}" = "signal" ]; then
        # Send message to monitor script that stream was requested to stop so that it doesn't log a warning
        mosquitto_pub -i "${client_id}_pub" -L "mqtt://127.0.0.1:51883/${json_attribute_topic}" -m {\"status\":\"deactivate\"}

        # Send ring-mqtt the command to stop the stream
        mosquitto_pub -i "${client_id}_pub" -L "mqtt://127.0.0.1:51883/${debug_topic}" -m "Deactivating ${type} stream due to signal from RTSP server (no more active clients)"
        mosquitto_pub -i "${client_id}_pub" -L "mqtt://127.0.0.1:51883/${command_topic}" -m "OFF"
    fi

    # Send kill signal to monitor script and wait for it to exit
    local pids=$(jobs -pr)
    [ -n "$pids" ] && kill $pids
    wait
    cleanup
}

# If control script is still runnning send kill signal and exit
cleanup() {
    rm -f /tmp/ring-mqtt-${client_id}.lock
    # For some reason sleeping for 100ms seems to keep go2rtc from hanging
    exit 0
}

# Send debug logs via main process using MQTT messages
logger() {
    mosquitto_pub -i "${client_id}_pub" -L "mqtt://127.0.0.1:51883/${debug_topic}" -m "${1}"
}

# Trap signals so that the MQTT command to stop the stream can be published on exit
trap 'stop signal' INT TERM
trap 'stop' EXIT

logger "Sending command to activate ${type} stream ON-DEMAND"
mosquitto_pub -i "${client_id}_pub" -L "mqtt://127.0.0.1:51883/${command_topic}" -m "ON-DEMAND ${rtsp_pub_url}" &

wait
cleanup