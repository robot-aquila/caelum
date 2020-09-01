#!/bin/bash
#
# Create Caelum configuration file
#

echo "caelum.backnode.rest.http.host=${APP_HTTP_HOST}" > "${APP_CONF}"
echo "caelum.backnode.rest.http.port=${APP_HTTP_PORT}" >> "${APP_CONF}"
echo "caelum.backnode.mode=${APP_MODE}" >> "${APP_CONF}"
echo "caelum.itemdb.kafka.bootstrap.servers=${APP_KAFKA_SERVERS}" >> "${APP_CONF}"
echo "caelum.symboldb.fdb.cluster=docker:docker@${APP_FDB_SERVERS}" >> "${APP_CONF}"
echo "caelum.aggregator.aggregation.period=${APP_AGGR_PERIOD}" >> "${APP_CONF}"
echo "caelum.aggregator.kafka.bootstrap.servers=${APP_KAFKA_SERVERS}" >> "${APP_CONF}"
echo "caelum.aggregator.kafka.state.dir=/tmp/kafka-streams" >> "${APP_CONF}"
echo "caelum.aggregator.kafka.application.server=${APP_HTTP_HOST}:${APP_HTTP_PORT}" >> "${APP_CONF}"
echo "caelum.itesym.bootstrap.servers=${APP_KAFKA_SERVERS}" >> "${APP_CONF}"

echo "Caelum backnode configuration created: ${APP_CONF}"
