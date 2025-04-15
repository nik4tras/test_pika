CREATE TABLESPACE myTablespace 
  DATAFILE 'myTablespace.dbf' 
  SIZE 250M 
  ONLINE; 

CREATE USER test
  IDENTIFIED BY p123
  DEFAULT TABLESPACE myTablespace 
  QUOTA unlimited
  ON myTablespace;

GRANT CONNECT TO test;
GRANT RESOURCE TO test;

Alter user test quota unlimited on myTablespace;

ALTER DATABASE DATAFILE 'myTablespace.dbf' resize 500m;