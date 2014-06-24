#!/bin/bash

export LANG="en_US.UTF-8"

LOG_FILE='/tmp/STREAMING_AGENT/logs/streaming-agent.log'

tail -500f $LOG_FILE