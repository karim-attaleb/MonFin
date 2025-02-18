import table as tb
import helper
import pandas as pd
import datetime


def export_ang(postgres_database, postgres_user, postgres_password,
               postgres_host, postgres_port, ang_export_path):
    try:
        helper.logger.info("Started Adres Export")
        helper.logger.info("-------------------------")

        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                                                  postgres_host, postgres_port)
        cur.execute("SET search_path TO munging")
        cur.execute("""Drop table if exists munging.tbl_adres_export""")
        conn.commit()
        cur.execute("""create table munging.tbl_adres_export as 
                    select distinct b."Enterprise_Nbr" as "Ondernemingsnummer",
                    b."LastStraat" as "Straat (NL)",
                    b."LastNummer" as "Nummer",
                    b."LastPostcode" as "Postcode",
                    b."LastGemeente" as "Gemeente (NL)",
                    b."LastAdrescode" as "Adrescode"
                    from munging.tbl_basis3_actief b
                    where b."LastGerArro" in ('Tongeren', 'Hasselt');""")
        conn.commit()

        current_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        export_file = f'{ang_export_path}/ExportAdres_{current_time}.xlsx'
        # export_file = ang_export_path + '/' + '_ang.xlsx'
        df = pd.read_sql_query('SELECT * from munging.tbl_adres_export', conn)
        df.to_excel(export_file, index=False, sheet_name="ActieveAdressen")

        # Close database connection
        conn.commit()
        conn.close()

        helper.logger.info("Adres file is exported to Excel")
        helper.logger.info(
            "==============================================================================================")
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (adres export): %s, args=%s: ", e, e.args)
