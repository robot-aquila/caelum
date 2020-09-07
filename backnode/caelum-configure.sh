#!/bin/sh
#
# Configure Caelum node
#

if [ -z "${APP_CONF}" ]; then
	export APP_CONF="${APP_HOME}/app.backnode.properties"
fi
echo "# Autogenerated $(date)" > "${APP_CONF}"

if [ -n "${APP_MODE}" ]; then
	 echo "caelum.backnode.mode=${APP_MODE}" >> "${APP_CONF}"
fi
if [ -n "${APP_HTTP_HOST}" ]; then
	echo "caelum.backnode.rest.http.host=${APP_HTTP_HOST}" >> "${APP_CONF}"
fi
if [ -n "${APP_HTTP_PORT}" ]; then
	echo "caelum.backnode.rest.http.port=${APP_HTTP_PORT}" >> "${APP_CONF}"
fi

if [ -n "${APP_KAFKA_SERVERS}" ]; then
    echo "caelum.itemdb.kafka.bootstrap.servers=${APP_KAFKA_SERVERS}" >> "${APP_CONF}"
    echo "caelum.aggregator.kafka.bootstrap.servers=${APP_KAFKA_SERVERS}" >> "${APP_CONF}"
    echo "caelum.itesym.bootstrap.servers=${APP_KAFKA_SERVERS}" >> "${APP_CONF}"
fi

if [ -n "${APP_FDB_SERVERS}" ]; then
	echo "caelum.symboldb.fdb.cluster=docker:docker@${APP_FDB_SERVERS}" >> "${APP_CONF}"
fi

echo "caelum.aggregator.kafka.state.dir=/tmp/kafka-streams" >> "${APP_CONF}"
if [ -n "${APP_AGGR_INTERVAL}" ]; then
	echo "caelum.aggregator.interval=${APP_AGGR_INTERVAL}" >> "${APP_CONF}"
fi
if [ -z "${APP_ADVERTISED_HTTP_HOST}" ]; then
	APP_ADVERTISED_HTTP_HOST="$(hostname -i)"
fi
if [ -z "${APP_ADVERTISED_HTTP_PORT}" ]; then
	if [ -n "${APP_HTTP_PORT}" ]; then
		if [ -S /var/run/docker.sock ]; then
			APP_ADVERTISED_HTTP_PORT=$(docker port "$(hostname)" ${APP_HTTP_PORT} | sed -r 's/.*:(.*)/\1/g')
		else
			APP_ADVERTISED_HTTP_PORT="${APP_HTTP_PORT}"
		fi
	else
		APP_ADVERTISED_HTTP_PORT="9698"
	fi
fi
echo "caelum.aggregator.kafka.application.server=${APP_ADVERTISED_HTTP_HOST}:${APP_ADVERTISED_HTTP_PORT}" >> "${APP_CONF}"
echo "Generated configuration properties:"
echo "-----------------------------------"
cat "${APP_CONF}"
echo "-----------------------------------"

#exit 5
