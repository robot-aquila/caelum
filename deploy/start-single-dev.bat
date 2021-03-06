@echo off
setlocal EnableDelayedExpansion
set ENV_FILE=local.env

if not exist %ENV_FILE% (
	echo Configuration variables file not exists: %ENV_FILE%
	echo Copy it from %ENV_FILE%.template, edit and run again
	exit 1
)
for /f "tokens=1,2 delims==" %%a in (%ENV_FILE%) do (
	set "%%a=%%b"
)

docker-compose -f docker-compose-single-dev.yml -p caelum up -d
docker exec -t caelum_fdb_1 /usr/bin/fdbcli --exec "configure new single memory"
