docker-compose up -d
docker exec -t caelum_fdb_1 /usr/bin/fdbcli --exec "configure new single memory"
