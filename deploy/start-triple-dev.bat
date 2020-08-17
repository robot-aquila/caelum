@echo off
setlocal EnableDelayedExpansion
set ENV_FILE=single-dev.env

if not exist %ENV_FILE% (
	echo Configuration variables file not exists: %ENV_FILE%
	echo Copy it from %ENV_FILE%.template, edit and run again
	exit 1
)
for /f "tokens=1,2 delims==" %%a in (%ENV_FILE%) do (
	set "%%a=%%b"
)

docker-compose -f docker-compose-triple-dev.yml -p caelum up --scale kafka=3 --no-recreate -d
docker exec -t caelum_fdb_1 /usr/bin/fdbcli --exec "configure new single memory"
docker exec -t caelum_kafka_1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 ^
	--create --topic caelum-item --partitions 16 --replication-factor 3 --config retention.ms=31536000000000

