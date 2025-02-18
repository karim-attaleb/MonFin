# created 01/02/2023
import os
import filemanager as fm
import helper
import table as tb
import pandas as pd
import re
from sqlalchemy import create_engine
import traceback


def determine_import_table(file_without_ext, searchstring):
    try:
        match = re.search(searchstring, file_without_ext)
        # check = fm.mid(file_without_ext, 6,6)
        # if check == "beroep":
        if match:
            table = "Tbl_RawData_DRI_Ang_Beroepsverbod"
            return table
        else:
            pass

    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (ang beroepsverbod getting tablename based on filename): %s, args=%s: ", e, e.args)


def create_table_ang_beroepsverbod_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        cur.execute("SET search_path TO manual")
        cur.execute("""CREATE TABLE if not exists manual."Tbl_RawData_DRI_Ang_Beroepsverbod" (
                    "NAAM" varchar,
                    "VOORNAAM" varchar,
                    "GEBDATUM" timestamp,
                    "WOONPLAATS" varchar,
                    "LOKPOL" varchar,
                    "EINDDATUM" timestamp,
                    "ARTIKEL" varchar,
                    "RRN" varchar,
                    "FileBase" varchar);""")

        cur.execute("""DELETE FROM manual."Tbl_RawData_DRI_Ang_Beroepsverbod";""")  # truncate
        helper.logger.info("Table manual.Tbl_RawData_DRI_Ang_Beroepsverbod has been created")
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (ang beroepsverbod postgres create): %s, args=%s: ", e, e.args)


def df_ang_beroepsverbod_to_db_postgres(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        engine = create_engine(
            f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')
        cur.execute("SET search_path TO manual")
        df.to_sql(name='Tbl_RawData_DRI_Ang_Beroepsverbod', schema='manual', con=engine, if_exists='append', index=False)
        cur.execute('SELECT count(*) as records FROM "Tbl_RawData_DRI_Ang_Beroepsverbod"')
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_DRI_Ang_Beroepsverbod has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        traceback_str = traceback.format_exc()
        helper.logger.info("Error: Oeps, something went wrong (ang beroepsverbod postgres insert): %s, args=%s: ",e, e.args)
        helper.logger.info("Error: Oeps, something went wrong (ang beroepsverbod postgres insert): %s, args=%s: ",e, traceback_str)

def import_ang_beroepsverbod_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port, beroepsverbodpath, searchstring):
    try:
        helper.logger.info("Started ANG Beroepsverbod")
        helper.logger.info("-------------------------")
        # db_path = os.path.join(path, database)
        # list_files = fm.walk(os.path.join(path, dir_in), '.xlsx')
        list_files = fm.walk(beroepsverbodpath, '.xlsx')
        helper.logger.info(list_files)
        conn = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        create_table_ang_beroepsverbod_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        for full_path in list_files:
            helper.logger.info("Behandel file: %s", full_path)
            file_without_ext = fm.get_filename_without_extension(full_path)
            table = determine_import_table(file_without_ext, searchstring)
            if table == "Tbl_RawData_DRI_Ang_Beroepsverbod":
                # df = pd.read_excel(full_path, sheet_name='Verbod')
                df = pd.read_excel(full_path, sheet_name='Verbod', usecols='A:H') #importeer enkel deze kolommen
                df['FileBase'] = full_path
                df.rename(columns={
                df.columns[0]: "NAAM",
                df.columns[1]: "VOORNAAM",
                df.columns[2]: "GEBDATUM",
                df.columns[3]: "WOONPLAATS",
                df.columns[4]: "LOKPOL",
                df.columns[5]: "EINDDATUM",
                df.columns[6]: "ARTIKEL",
                df.columns[7]: "RRN",
                df.columns[8]: "FileBase"}
                     , inplace=True)
                # replace invalid dates with NULL
                df['GEBDATUM'] = pd.to_datetime(df['GEBDATUM'], errors='coerce')
                df['EINDDATUM'] = pd.to_datetime(df['EINDDATUM'], errors='coerce')

                # with pd.option_context('display.max_columns', None):
                #     print("Dataframe Beroepsverbod:")
                #     print(df)
                #
                # print("Column Names:")
                # print(df.columns.tolist())
                # print("Data Types:")
                # print(df.dtypes)

                helper.logger.info("Import %s records in df from file %s", len(df), full_path)
                # print("Print df Ang: ", df)
                df_ang_beroepsverbod_to_db_postgres(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
                helper.logger.info("Ang beroepsverbod file is imported into db")
                helper.logger.info("==============================================================================================")
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (ang beroepsverbod postgres import): %s, args=%s: ",e, e.args)