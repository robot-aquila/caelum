FROM openjdk:8-slim-buster

ARG APP_VERSION

ENV APP_VERSION="${APP_VERSION}" \
    APP_HOME="/opt/caelum-backnode" \
    APP_KAFKA_SERVERS="localhost:9092" \
    APP_FDB_SERVERS="localhost:4500" \
    APP_HTTP_HOST="0.0.0.0" \
    APP_HTTP_PORT="9698" \
	APP_ADVERTISED_HTTP_HOST= \
	APP_ADVERTISED_HTTP_PORT= \
    APP_MODE="prod" \
    APP_AGGR_INTERVAL="M1,H1" \
    JAVA_OPTS="-Xms512m -Xmx1024m"
ENV APP_CONF_SCRIPT="${APP_HOME}/caelum-configure.sh" \
    APP_CONF="${APP_HOME}/app.backnode.properties"

WORKDIR $APP_HOME

COPY "target/caelum-backnode-${APP_VERSION}.jar" caelum-configure.sh caelum-entrypoint.sh "${APP_HOME}/"
COPY --from=foundationdb/foundationdb:6.2.22 /usr/lib/libfdb_c.so /usr/lib/libfdb_c.so

RUN mkdir -p "${APP_HOME}/logs" \
    && chmod 0755 caelum-configure.sh \
    && chmod 0755 caelum-entrypoint.sh \
    && chmod 0644 "caelum-backnode-${APP_VERSION}.jar"

RUN apt-get update \
	&& apt-get install -y --no-install-recommends curl \
	&& curl https://download.docker.com/linux/debian/dists/buster/pool/stable/amd64/docker-ce-cli_19.03.12~3-0~debian-buster_amd64.deb --output docker-ce-cli.deb \
	&& dpkg -i docker-ce-cli.deb \
	&& rm -f docker-ce-cli.deb \
	&& rm -fr /var/lib/apt/lists/*

VOLUME ["/tmp/kafka-streams"]

CMD ["./caelum-entrypoint.sh"]
