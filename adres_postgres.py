import os
import filemanager as fm
import helper
import table as tb
import pandas as pd
import re
from sqlalchemy import create_engine


def determine_import_table(file_without_ext, search_adres):
    # if fm.left(file_without_ext, 7) == "Geocode":
    # match = re.search(r"Geocode", file_without_ext)
    match = re.search(search_adres, file_without_ext)
    if match:
        table = "Tbl_RawData_Adres"
        return table
    else:
        pass


def create_table_adres_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        cur.execute("SET search_path TO manual")
        cur.execute("""CREATE TABLE if not exists manual."Tbl_RawData_Adres" (
                    "X" float8,
                    "Y" float8,
                    "Onderne" varchar,
                    "Nummer" varchar,
                    "Straat" varchar,
                    "Postcode" int, 
                    "Gemeente" varchar,
                    "ACQ" text,
                    "PRE" int,
                    "FileBase" varchar);""")

        cur.execute("""DELETE FROM manual."Tbl_RawData_Adres";""")  # truncate
        helper.logger.info("Table manual.Tbl_RawData_Adres has been created")
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (adres): %s, args=%s: ", e, e.args)


def df_adres_to_db_postgres(df, postgres_database, postgres_user, postgres_password,
                            postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')
        cur.execute("SET search_path TO manual")
        df.to_sql(name='Tbl_RawData_Adres', schema='manual', con=engine, if_exists='append', index=False)
        cur.execute('SELECT count(*) as records FROM "Tbl_RawData_Adres"')
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_Adres has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (adres): %s, args=%s: ",e, e.args)


def import_adres_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port, adrespath, search_adres):
    try:
        helper.logger.info("Started Adres")
        helper.logger.info("-------------------------")
        # db_path = os.path.join(path, database)
        # list_files = fm.walk(os.path.join(path, dir_in), '.xlsx')
        list_files = fm.walk(os.path.join(adrespath), '.xlsx')
        helper.logger.info(list_files)
        conn = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        create_table_adres_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        for full_path in list_files:
            helper.logger.info("Behandel file: %s", full_path)
            file_without_ext = fm.get_filename_without_extension(full_path)
            table = determine_import_table(file_without_ext, search_adres)
            if table == "Tbl_RawData_Adres":
                df = pd.read_excel(full_path, sheet_name='Outputgeocode')
                df['FileBase'] = full_path
                # df['Onderne'] = '0' + df['Onderne'].astype(str)
                helper.logger.info("Import %s records in df from file %s", len(df), full_path)
                # print("Print df Ang: ", df)
                df_adres_to_db_postgres(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
                helper.logger.info("Adres file is imported into db")
                helper.logger.info("==============================================================================================")
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (adres): %s, args=%s: ",e, e.args)