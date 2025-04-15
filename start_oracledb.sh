#/bin/bash

docker run -d --name oradb --network pika_test -p 1521:1521 -e ORACLE_PWD=p123 container-registry.oracle.com/database/free:latest