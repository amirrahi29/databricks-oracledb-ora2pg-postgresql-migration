## 🎥 YouTube Video

<p align="center">
  <a href="https://www.youtube.com/watch?v=9njCRydf4Yk">
    <img src="https://img.youtube.com/vi/9njCRydf4Yk/maxresdefault.jpg" width="700">
  </a>
</p>

---

## 🚀 Steps to ora2pg

### 🔧 Step 1: Config File

```ini
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
