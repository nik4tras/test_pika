#/bin/bash

docker run -d --name oradb -p 1521:1521 -e ORACLE_PWD=password container-registry.oracle.com/database/free:latest