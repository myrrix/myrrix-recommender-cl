#!/bin/bash
# chkconfig: 2345 99 1
# description: Provides Myrrix Serving Layer

# Source function library.
. /etc/init.d/functions

base=${0##*/}

start() {
        echo -n "Starting Myrrix Serving Layer"

        curl -f -s http://169.254.169.254/latest/user-data > /tmp/user-data.txt
        USER_DATA=`cat /tmp/user-data.txt`
        rm -f /tmp/user-data.txt

        JVM_FLAGS=""
        PROGRAM_ARGS=""
        for ARG in ${USER_DATA}
        do
          if echo ${ARG} | grep -qxE "^-D[^ ;\`]+" ; then
            JVM_FLAGS="${JVM_FLAGS} ${ARG}"
          fi
          if echo ${ARG} | grep -qxE "^--[^ ;\`]+" ; then
            PROGRAM_ARGS="${PROGRAM_ARGS} ${ARG}"
          fi
        done

        mkdir -p /tmp/myrrix

        COMMAND_LINE="/usr/lib/jvm/jre-1.7.0/bin/java \
          -XX:DefaultMaxRAMFraction=1 -XX:NewRatio=20 -XX:+UseParallelOldGC ${JVM_FLAGS} \
          -jar /usr/local/myrrix-serving-layer/myrrix-serving-1.0.2.jar \
          --localInputDir=/tmp/myrrix --port=80 --securePort=443 ${PROGRAM_ARGS}"

        echo "${COMMAND_LINE}"
        ${COMMAND_LINE} &
}

stop() {
        echo -n "Stopping Myrrix Serving Layer"

        pkill java
}

case "$1" in
    restart)
        stop && success || failure
        start && success || failure
        echo
        ;;

    start)
        start && success || failure
        echo
        ;;

    stop)
        stop && success || failure
        echo
        ;;

    *)
        echo $"Usage: $0 {start|stop|restart}"
        exit 3
        ;;
esac