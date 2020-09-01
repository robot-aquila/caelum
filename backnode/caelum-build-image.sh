#!/bin/bash
#
# Builds docker image of Caelum backnode
#
APP_VERSION=`cat version.built`
if [[ -z "${APP_VERSION}" ]]; then
    echo "Cannot determine version number"
    exit 1
fi
docker build --build-arg APP_VERSION="${APP_VERSION}" -t "caelum/backnode:${APP_VERSION}" -f Dockerfile .
