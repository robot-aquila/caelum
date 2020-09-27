#!/bin/bash
#
# Builds docker image of Caelum backnode
#
APP_VERSION=`cat version.built`
if [[ -z "${APP_VERSION}" ]]; then
    echo "Cannot determine version number"
    exit 1
fi

# Just put docker-ce-* file to contrib subdirectory to speedup if you're building images often
DOCKER_CLI_FILE="docker-ce-cli_19.03.12~3-0~debian-buster_amd64.deb"
DOCKER_CLI_SRC="https://download.docker.com/linux/debian/dists/buster/pool/stable/amd64/${DOCKER_CLI_FILE}"
if [ -f "contrib/${DOCKER_CLI_FILE}" ]; then
    DOCKER_CLI_SRC="contrib/${DOCKER_CLI_FILE}"
fi

docker build --build-arg APP_VERSION="${APP_VERSION}" --build-arg DOCKER_CLI_SRC="${DOCKER_CLI_SRC}" -t "caelum/backnode:${APP_VERSION}" -f Dockerfile .
