#!/bin/bash
#
# Caelum backnode entrypoint
#
set -e
MY_PATH=`dirname $0`
if [ -z "${APP_VERSION}" ]; then
    echo "Environment variable not defined: APP_VERSION"
    exit 1
else
    echo "APP_VERSION: ${APP_VERSION}"
fi
if [ -z "${APP_HOME}" ]; then
    echo "Environment variable not defined: APP_HOME"
    exit 1
else
    echo "   APP_HOME: ${APP_HOME}"
fi

if [ -z "${APP_CONF}" ]; then
    echo "Environment variable not defined: APP_CONF"
    exit 1
else
    echo "   APP_CONF: ${APP_CONF}"
fi

if [ -x "${APP_CONF_SCRIPT}" ]; then
    "${APP_CONF_SCRIPT}"
    RETVAL=$?
    if [ $RETVAL -ne 0 ]; then
        echo "Configuration script failed: ${RETVAL}"
        exit 1
    fi
else
    echo "Configuration script not exists or not executable: ${APP_CONF_SCRIPT}"
    exit 1
fi

if [ ! -f "${APP_CONF}" ]; then
    echo "Configuration file not exists: ${APP_CONF}"
    exit 1
fi

echo "Dumping environment variables: "
env
exec java $JAVA_OPTS -jar "${APP_HOME}/caelum-backnode-${APP_VERSION}.jar" "${APP_CONF}"
