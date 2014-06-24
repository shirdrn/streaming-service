#!/bin/bash

export LANG="en_US.UTF-8"
. scripts/env.sh

PID=`ps -ef | grep java | grep "$APP_NAME" | cut -d ' ' -f 3`
echo "Kill application: $APP_NAME, PID is: $PID"

kill -9 $PID
echo "Killed!"