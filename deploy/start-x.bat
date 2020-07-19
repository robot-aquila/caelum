docker-compose -p caelum up --scale kafka=2 -d
docker exec -t caelum_fdb_1 /usr/bin/fdbcli --exec "configure new single memory"
rem Set up retention period for 1000 years
docker exec -t caelum_kafka_1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 ^
	--create --topic caelum-item --partitions 4 --replication-factor 1 ^
	--config retention.ms=31536000000000
