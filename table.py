import sqlite3
import psycopg2
import helper
import os


# detect encoding of file
# https://www.kaggle.com/code/paultimothymooney/how-to-resolve-a-unicodedecodeerror-for-a-csv-file
# import chardet
# with open(file, 'rb') as rawdata:
#     result = chardet.detect(rawdata.read(100000))
# result

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        curr = conn.cursor()
    except sqlite3.error as error:
        helper.logger.info(error)
    return conn, curr


def overview_tables(dbname):
    try:
        # dbname = os.path.join(path, database)
        helper.logger.info("INITILIZATION COUNT IMPORT FILES FOR EACH TABLE...")
        con = sqlite3.connect(dbname)
        cursor = con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        for tbl in tables:
            helper.logger.info("\n########  " + tbl[0] + "  ########")
            # cursor.execute("SELECT * FROM "+tbl[0]+";") #print ALLE kolommen
            cursor.execute("select count(FileBase), FileBase from " + tbl[0] + " group by FileBase") #print count per geimporteerde file
            rows = cursor.fetchall()
            for row in rows:
                helper.logger.info(row)
        helper.logger.info(cursor.fetchall())
    except KeyboardInterrupt:
        helper.logger.info("Clean Exit By user")
    finally:
        helper.logger.info("Finished looping SQLite tables")


def overview_tables_postgres(postgres_database, postgres_user, postgres_password,
                                                    postgres_host, postgres_port):
    pg_conn, pg_cur = create_connection_postgres(postgres_database, postgres_user, postgres_password,
                                                    postgres_host, postgres_port)
    try:
        pg_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='manual';")
        tables = pg_cur.fetchall()
        for tbl in tables:
            print("\n########  " + tbl[0] + "  ########")
            # pg_cur.execute("SELECT count(FileBase), FileBase FROM " + tbl[0] + " GROUP BY FileBase")
            # pg_cur.execute('SELECT count("FileBase"), "FileBase" FROM "' + tbl[0] + '" GROUP BY "FileBase"')
            pg_cur.execute('SELECT count("FileBase"), "FileBase" FROM manual."' + tbl[0] + '" GROUP BY "FileBase"')

            rows = pg_cur.fetchall()

            for row in rows:
                print(row)
        pg_cur.close()
    except psycopg2.DatabaseError as error:
        helper.logger.info("Error: ", error)
    finally:
        if pg_conn is not None:
            pg_conn.close()
            helper.logger.info("Finished looping PostgreSQL tables")


def create_connection_postgres(db_name, user, password, host, port):
    conn = None
    try:
        conn = psycopg2.connect(
            database=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )
        curr = conn.cursor()
    except psycopg2.Error as error:
        helper.logger.info(error)
    return conn, curr


# def create_fk_constraint(table_name, column_name, fk_table_name, fk_column_name, postgres_database, postgres_user, postgres_password, postgres_host, postgres_port):
#     try:
#         conn, cur = create_connection_postgres(postgres_database, postgres_user, postgres_password,
#                                                   postgres_host, postgres_port)
#
#         # Check if constraint exists
#         cur.execute(f'SELECT 1 FROM pg_constraint WHERE conname = "{table_name}_{column_name}_fkey"')
#         exists = cur.fetchone()
#
#         if not exists:
#             # Create constraint
#             table_name = table_name.replace('.', '_')
#             cur.execute(
#                 f'ALTER TABLE "{table_name}" ADD CONSTRAINT "{table_name}_{column_name}_fkey" FOREIGN KEY ("{column_name}") REFERENCES "{fk_table_name}" ("{fk_column_name}");')
#             conn.commit()
#             helper.logger.info(f"Foreign key constraint created for {table_name}.{column_name}")
#
#         cur.close()
#         conn.close()
#     except Exception as e:
#         helper.logger.info("Error: Oeps, something went wrong (create constraint): %s, args=%s: ", e, e.args)


def create_relationships(postgres_database, postgres_user, postgres_password,
                                                  postgres_host, postgres_port):
    #Geen manier gevonden om te checken of een relatie reeds bestaat in sql
    #Dus via code
    try:
        helper.logger.info("Started creating relationships")
        helper.logger.info("-------------------------")
        conn, cur = create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        sql_statements = [
            'ALTER TABLE final."Tbl_Final_Limburg" ADD CONSTRAINT fk_final_limburg_score_rechtsvorm FOREIGN KEY ("ScoreRechtsvorm") REFERENCES setup."Tbl_ScoreRechtsvorm"("Score");',
            'ALTER TABLE final."Tbl_Final_Limburg" ADD CONSTRAINT fk_final_limburg_score_doorhaling FOREIGN KEY ("ScoreDoorhaling") REFERENCES setup."Tbl_ScoreDoorhaling"("Score");',
            'ALTER TABLE final."Tbl_Final_Limburg" ADD CONSTRAINT fk_final_limburg_score_verzameladres FOREIGN KEY ("ScoreVerzameladres") REFERENCES setup."Tbl_ScoreVerzameladres"("Score");',
            'ALTER TABLE final."Tbl_Final_Limburg" ADD CONSTRAINT fk_final_limburg_score_referentieadres FOREIGN KEY ("ScoreReferentieAdres") REFERENCES setup."Tbl_ScoreReferentieadres"("Score");',
            'ALTER TABLE final."Tbl_Final_Limburg" ADD CONSTRAINT fk_final_limburg_score_faillissementadres FOREIGN KEY ("ScoreFaillissementenAdres") REFERENCES setup."Tbl_ScoreFaillissementadres"("Score");',
            'ALTER TABLE final."Tbl_Final_Limburg" ADD CONSTRAINT fk_final_limburg_score_geocoderen FOREIGN KEY ("ScoreGeocoderen") REFERENCES setup."Tbl_ScoreGeocoderen"("Score");',
            'ALTER TABLE final."Tbl_Final_Limburg" ADD CONSTRAINT fk_final_limburg_score_anderedoorhalingen FOREIGN KEY ("ScoreAndereDoorhalingen") REFERENCES setup."Tbl_ScoreAndereDoorhalingen"("Score");',
            'ALTER TABLE final."Tbl_Final_Limburg" ADD CONSTRAINT fk_final_limburg_score_meervoudigzaakvoerderschap FOREIGN KEY ("ScoreMeervoudigZaakvoerderschap") REFERENCES setup."Tbl_ScoreMeervoudigZaakvoerderschap"("Score");',
            'ALTER TABLE final."Tbl_Final_Limburg" ADD CONSTRAINT fk_final_limburg_score_faillissementbestuurder FOREIGN KEY ("ScoreFaillissementenBestuurder") REFERENCES setup."Tbl_ScoreFaillissementBestuurder"("Score");',
            'ALTER TABLE final."Tbl_Final_Limburg" ADD CONSTRAINT fk_final_limburg_score_anggekend FOREIGN KEY ("ScoreAngGekend") REFERENCES setup."Tbl_ScoreAngGekend"("Score");',
            'ALTER TABLE final."Tbl_Final_Limburg" ADD CONSTRAINT fk_final_limburg_score_beroepsverbod FOREIGN KEY ("ScoreBeroepsverbod") REFERENCES setup."Tbl_ScoreBeroepsverbod"("Score");',
            'ALTER TABLE final."Tbl_Final_Limburg" ADD CONSTRAINT fk_final_limburg_score_bisnummer FOREIGN KEY ("ScoreBisnummer") REFERENCES setup."Tbl_ScoreBisnummer"("Score");',
            'ALTER TABLE final."Tbl_Final_Limburg" ADD CONSTRAINT fk_final_limburg_score_contactgegevens FOREIGN KEY ("ScoreContactgegevens") REFERENCES setup."Tbl_ScoreContactgegevens"("Score");',
            'ALTER TABLE final."Tbl_Final_Limburg" ADD CONSTRAINT fk_final_limburg_score_wijzigingadres FOREIGN KEY ("ScoreWijzigingAdres") REFERENCES setup."Tbl_ScoreWijzigingAdres"("Score");',
            'ALTER TABLE final."Tbl_Final_Limburg" ADD CONSTRAINT fk_final_limburg_score_wijzigingadresvoorfail FOREIGN KEY ("ScoreWijzigingAdresVoorFaillissement") REFERENCES setup."Tbl_ScoreWijzigingAdresVoorFail"("Score");',
            'ALTER TABLE final."Tbl_Final_Limburg" ADD CONSTRAINT fk_final_limburg_score_wijzigingbestuur FOREIGN KEY ("ScoreWijzigingBestuur") REFERENCES setup."Tbl_ScoreWijzigingBestuur"("Score");',
            'ALTER TABLE final."Tbl_Final_Limburg" ADD CONSTRAINT fk_final_limburg_score_wijzigingbestuurvoorfail FOREIGN KEY ("ScoreWijzigingBestuurFaillisement") REFERENCES setup."Tbl_ScoreWijzigingBestuurVoorFail"("Score");',
            ]

        # Loop over the list of SQL statements
        for statement in sql_statements:
            # Check if the constraint already exists
            # constraint_name = statement.split()[3].strip('"')
            constraint_name = statement.split("CONSTRAINT ")[1].split()[0].strip('"')
            sql_query = f"SELECT COUNT(*) FROM pg_constraint WHERE conname = '{constraint_name}';"
            cur.execute(sql_query)
            count = cur.fetchone()[0]
            # If the constraint doesn't exist, create it
            if count == 0:
                cur.execute(statement)
                helper.logger.info(f"Created constraint: {statement}")
            else:
                helper.logger.info(f"Constraint already exists: {statement}")
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (creating relationships): %s, args=%s: ", e, e.args)
