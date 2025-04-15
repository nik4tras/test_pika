CREATE TABLE test.data_landing
(
	id integer GENERATED ALWAYS AS IDENTITY
,	msg_nr integer
,	msg_content varchar2(4000)
,	processed_at varchar(32)
);

DROP TABLE test.DATA_LANDING ;

SELECT * FROM test.data_landing ORDER BY MSG_NR ;
SELECT count(*), count(DISTINCT msg_nr) FROM test.DATA_LANDING dl ;

TRUNCATE TABLE test.DATA_LANDING;

select user from dual;