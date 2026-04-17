``` 
YOUTUBE VIDEO
================
https://www.youtube.com/watch?v=9njCRydf4Yk
     
Steps to orc2pg
=======================

Step1 (config file)
=======
ORACLE_DSN dbi:Oracle:host=global-db.cb0mgacu6028.ap-south-1.rds.amazonaws.com;sid=ORCL;port=1521
ORACLE_USER global123
ORACLE_PWD global123

SCHEMA GLOBAL123

PG_DSN dbi:Pg:dbname=postgres;host=rahipostgresql.postgres.database.azure.com;port=5432
PG_USER amirrahi29
PG_PWD Test@1234

TYPE TABLE
OUTPUT tables.sql

PG_VERSION 15






Step 1 (Schema export)
====================
cd ~/migration
ora2pg -c ora2pg.conf -t TABLE

Step 2 (Schema PostgreSQL में डालो)
=============================
psql -h rahipostgresql.postgres.database.azure.com \
     -U amirrahi29 \
     -d postgres \
     -f tables.sql

```
