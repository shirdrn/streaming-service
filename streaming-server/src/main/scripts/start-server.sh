#!/bin/bash

export LANG="en_US.UTF-8"
. scripts/env.sh

export JAVA_OPTS=$JAVA_OPTS' -Dflag='$APP_NAME

CURRENT=`pwd`
MAIN_CLASS='org.shirdrn.streaming.server.StreamingServer'
LIB_DIR=$CURRENT'/lib'
CLASSES=$CURRENT'/classes'
CONF=$CURRENT'/conf'

CLASS_PATH='.'
CLASS_PATH=$CLASS_PATH:$CLASSES
CLASS_PATH=$CLASS_PATH:$CONF


# Dependent jar files
for lib in `find $LIB_DIR`
do
	CLASS_PATH=$CLASS_PATH:$lib
done

echo "START command: nohup java -cp $CLASS_PATH $MAIN_CLASS >/dev/null &"
nohup java -cp $CLASS_PATH $JAVA_OPTS $MAIN_CLASS >/dev/null &