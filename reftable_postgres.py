import table as tb
import helper

def create_table_historiek_nis_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        cur.execute("SET search_path TO reftab")
        cur.execute("""CREATE TABLE if not exists reftab."historieknis" (
                    "Community/NisCode" varchar,
                    "GerArro" varchar);""")

        cur.execute("""DELETE FROM reftab."historieknis";""")  # truncate
        helper.logger.info("Table reftab.historieknis has been created")
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (adres): %s, args=%s: ", e, e.args)