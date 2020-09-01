#!/bin/bash
#
# Caelum backnode entrypoint
#
set -e
MY_PATH=`dirname $0`
if [ -z "${APP_VERSION}" ]; then
    echo "Environment variable not defined: APP_VERSION"
    exit 1
fi
if [ -z "${APP_HOME}" ]; then
    echo "Environment variable not defined: APP_HOME"
    exit 1
fi
if [ -x "${APP_CONF_SCRIPT}" ]; then
    "${APP_CONF_SCRIPT}"
fi

echo "Dumping environment variables: "
env
exec java $JAVA_OPTS -jar "${APP_HOME}/caelum-backnode-${APP_VERSION}.jar" "${APP_CONF}"
