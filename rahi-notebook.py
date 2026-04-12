from pyspark.sql.functions import col
import psycopg2

# ==============================
# CONFIG
# ==============================
CONFIG = {
    "oracle": {
        "url": "jdbc:oracle:thin:@//global-db.cb0mgacu6028.ap-south-1.rds.amazonaws.com:1521/ORCL",
        "user": "global123",
        "password": "global123",
        "driver": "oracle.jdbc.driver.OracleDriver"
    },
    "postgres": {
        "host": "rahipostgresql.postgres.database.azure.com",
        "database": "postgres",
        "user": "amirrahi29",
        "password": "Test@1234",
        "port": 5432
    }
}

PG_URL = "jdbc:postgresql://rahipostgresql.postgres.database.azure.com:5432/postgres?sslmode=require"

# ==============================
# CONNECT
# ==============================
conn = psycopg2.connect(
    host=CONFIG["postgres"]["host"],
    database=CONFIG["postgres"]["database"],
    user=CONFIG["postgres"]["user"],
    password=CONFIG["postgres"]["password"],
    port=CONFIG["postgres"]["port"],
    sslmode="require"
)
conn.autocommit = False
cursor = conn.cursor()

# ==============================
# AUDIT TABLE
# ==============================
cursor.execute("""
CREATE TABLE IF NOT EXISTS migration_audit (
    table_name TEXT,
    status TEXT,
    oracle_count INT,
    staging_count INT,
    final_count INT,
    error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()

# ==============================
# GET TABLES
# ==============================
df_tables = spark.read.jdbc(
    url=CONFIG["oracle"]["url"],
    table="(SELECT table_name FROM all_tables WHERE owner='GLOBAL123') t",
    properties=CONFIG["oracle"]
)

tables = [r["TABLE_NAME"].lower() for r in df_tables.collect()]

# ==============================
# CREATE STAGING TABLES
# ==============================
for table in tables:
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS staging_{table}
    AS SELECT * FROM {table} WHERE 1=0
    """)
conn.commit()

# ==============================
# PROCESS TABLE
# ==============================
def process_table(table):
    try:
        print(f"Processing {table}")

        # ----------------------
        # READ ORACLE
        # ----------------------
        df = spark.read.jdbc(
            url=CONFIG["oracle"]["url"],
            table=f"GLOBAL123.{table.upper()}",
            properties=CONFIG["oracle"]
        )

        if df.rdd.isEmpty():
            print(f"Skipping empty table {table}")
            return

        df = df.toDF(*[c.lower() for c in df.columns])

        # decimal fix
        for c, t in df.dtypes:
            if "decimal" in t:
                df = df.withColumn(c, col(c).cast("double"))

        df = df.repartition(4)

        # ----------------------
        # LOAD STAGING
        # ----------------------
        df.write.jdbc(
            url=PG_URL,
            table=f"staging_{table}",
            mode="overwrite",
            properties={
                "user": CONFIG["postgres"]["user"],
                "password": CONFIG["postgres"]["password"],
                "driver": "org.postgresql.Driver",
                "batchsize": "20000"
            }
        )

        # ----------------------
        # VALIDATION
        # ----------------------
        oracle_count = df.count()

        cursor.execute(f"SELECT COUNT(*) FROM staging_{table}")
        staging_count = cursor.fetchone()[0]

        if oracle_count != staging_count:
            raise Exception("Count mismatch")

        # ----------------------
        # TRANSACTION START
        # ----------------------
        conn.rollback()

        # ----------------------
        # DISABLE FK SAFELY
        # ----------------------
        cursor.execute("SET session_replication_role = 'replica'")

        # ----------------------
        # LOAD FINAL
        # ----------------------
        cursor.execute(f"TRUNCATE TABLE {table} CASCADE")
        cursor.execute(f"INSERT INTO {table} SELECT * FROM staging_{table}")

        # ----------------------
        # ENABLE FK BACK
        # ----------------------
        cursor.execute("SET session_replication_role = 'origin'")

        # ----------------------
        # FINAL COUNT
        # ----------------------
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        final_count = cursor.fetchone()[0]

        if final_count != staging_count:
            raise Exception("Final count mismatch")

        conn.commit()

        # ----------------------
        # AUDIT SUCCESS
        # ----------------------
        cursor.execute(f"""
        INSERT INTO migration_audit(table_name, status, oracle_count, staging_count, final_count)
        VALUES ('{table}', 'SUCCESS', {oracle_count}, {staging_count}, {final_count})
        """)
        conn.commit()

        print(f"SUCCESS {table}")

    except Exception as e:
        conn.rollback()

        # ensure FK mode back
        try:
            cursor.execute("SET session_replication_role = 'origin'")
        except:
            pass

        cursor.execute(f"""
        INSERT INTO migration_audit(table_name, status, error)
        VALUES ('{table}', 'FAILED', '{str(e)}')
        """)
        conn.commit()

        print(f"FAILED {table}: {e}")

# ==============================
# RUN PIPELINE
# ==============================
for table in tables:
    process_table(table)

cursor.close()
conn.close()

print("Migration Completed")
