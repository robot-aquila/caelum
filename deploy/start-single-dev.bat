docker-compose -f docker-compose-single-dev.yml -p caelum up -d
docker exec -t caelum_fdb_1 /usr/bin/fdbcli --exec "configure new single memory"
docker exec -t caelum_kafka_1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 ^
	--create --topic caelum-item --partitions 1
