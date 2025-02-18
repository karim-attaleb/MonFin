import table as tb
import helper
import pandas as pd
import datetime


def export_ang(postgres_database, postgres_user, postgres_password,
               postgres_host, postgres_port, ang_export_path):
    try:
        helper.logger.info("Started ANG Export")
        helper.logger.info("-------------------------")

        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                                                  postgres_host, postgres_port)
        cur.execute("SET search_path TO munging")
        cur.execute("""Drop table if exists munging.tbl_ang_export""")
        conn.commit()
        cur.execute("""CREATE TABLE "munging"."tbl_ang_export" as
                        SELECT DISTINCT "ActiefRrn"::text as "Persoonsnummer functiehouder",
                       "ActiefFunctiehouderNaam" as "Naam functiehouder", 
                       "ActiefFunctiehouderVoornaam" as "Voornaam functiehouder"
                       FROM munging.tbl_basis3_actief 
                       WHERE "LastGerArro" IN ('Hasselt', 'Tongeren')
                       AND "ActiefRrn" <> '00000000000'""")
        conn.commit()

        current_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        export_file = f'{ang_export_path}/ExportAng_{current_time}.xlsx'
        # export_file = ang_export_path + '/' + '_ang.xlsx'
        df = pd.read_sql_query('SELECT * from munging.tbl_ang_export', conn)
        df.to_excel(export_file, index=False, sheet_name="ActieveBestuurders")

        # Close database connection
        conn.commit()
        conn.close()

        helper.logger.info("Ang file is exported to Excel")
        helper.logger.info(
            "==============================================================================================")
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (ang export): %s, args=%s: ", e, e.args)
