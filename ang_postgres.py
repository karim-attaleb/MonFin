import os
import filemanager as fm
import helper
import table as tb
import pandas as pd
import re
from sqlalchemy import create_engine


def determine_import_table15(file_without_ext, search_ang):
    try:
        match = re.search(search_ang, file_without_ext)
        # if fm.right(file_without_ext, 8) == "SCRCANDN":
        if match:
            table = "Tbl_RawData_DRI_Ang15"
            return table
        else:
            pass
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (ang getting tablename15 based on filename): %s, args=%s: ", e, e.args)


def determine_import_table01(file_without_ext, search_ang):
    try:
        match = re.search(search_ang, file_without_ext)
        # if fm.right(file_without_ext, 8) == "SCRCANDN":
        if match:
            table = "Tbl_RawData_DRI_Ang01"
            return table
        else:
            pass
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (ang getting tablename01 based on filename): %s, args=%s: ", e, e.args)


def create_table_ang_postgres15(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        # 01/02/2023: toevoeging van onderstaande velden:
        # [FEI SLEUT] text,
        # AFE1 text,
        # [Unnamed49] text,
        # [Unnamed50] float,
        cur.execute("SET search_path TO manual")
        cur.execute("""CREATE TABLE if not exists manual."Tbl_RawData_DRI_Ang15" (
                            "PL2-embargo" varchar,
                            "PL2-typ-info" varchar,
                            "Pers sleutel" varchar,
                            "Fei-typ-info" varchar,
                            "fei-embargo" varchar,
                            "Aard feit 1" varchar,
                            "Aard feit 2" varchar,
                            "Aard feit 3" varchar,
                            "Referte" varchar,
                            "PV Nr" varchar,
                            "Prefix" varchar,
                            "Rapport Nr" varchar,
                            "Register Eenheid" varchar,
                            "Lage datum" varchar,
                            "Hooge datum" varchar,
                            "Land" varchar,
                            "Gemeente" varchar,
                            "Straat" varchar,
                            "Nr" varchar,
                            "Appart Nr" varchar,
                            "Bestem Plaats 1" varchar,
                            "Bestem Plaats Lib 1" varchar,
                            "Bestem Plaats klas 1" varchar,
                            "Bestem Plaats Lib 1-" varchar,
                            "Bestem Plaats 2" varchar,
                            "Bestem Plaats Lib 2" varchar,
                            "Bestem Plaats klas 2" varchar,
                            "Bestem Plaats Lib 2-" varchar,
                            "Bestem Plaats 3" varchar,
                            "Bestem Plaats Lib 3" varchar,
                            "Bestem Plaats klas 3" varchar,
                            "Bestem Plaats Lib 3-" varchar,
                            "Reden reg" varchar,
                            "TNM" varchar,
                            "Ver Eenheid TNM" varchar,
                            "VervalDatum" varchar,
                            "Reden reg1" varchar,
                            "TNM1" varchar,
                            "Ver Eenheid TNM1" varchar,
                            "VervalDatum1" varchar,
                            "Unnamed40" varchar,
                            "Unnamed41" varchar,
                            "Unnamed42" float,
                            "Unnamed43" varchar,
                            "Unnamed44" varchar,
                            "Unnamed45" varchar,
                            "Unnamed46" double precision,
                            "FEI SLEUT" varchar,
                            "AFE1" varchar,
                            "AFE2" varchar,
                            "AFE3" varchar,
                            "Unnamed49" varchar,
                            "Unnamed50" double precision,
                            "FileBase" varchar);""")

        # c.execute(
        #     """
        #     CREATE INDEX alias_pin_IDX ON alias(pin);
        #     """)

        cur.execute("""DELETE FROM manual."Tbl_RawData_DRI_Ang15";""")  # truncate
        helper.logger.info("Table Tbl_RawData_DRI_Ang15 has been created")
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (ang15): %s, args=%s: ", e, e.args)


def create_table_ang_postgres01(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        # 01/02/2023: toevoeging van onderstaande velden:
        # [FEI SLEUT] text,
        # AFE1 text,
        # [Unnamed49] text,
        # [Unnamed50] float,
        cur.execute("SET search_path TO manual")
        cur.execute("""CREATE TABLE if not exists manual."Tbl_RawData_DRI_Ang01" (
                    "Embargo" varchar,
                    "Typ-info" varchar,
                    "Nationaliteit" varchar,
                    "Sleutel" varchar,
                    "Naam" varchar,
                    "Voornaam" varchar,
                    "Naam en voornaam" varchar,
                    "Geboorte datum" varchar,
                    "Geslacht" varchar,
                    "RRN" varchar,
                    "Foto ind" varchar,
                    "Foto laatste datum" varchar,
                    "Foto num key" varchar,
                    "Foto datum" varchar,
                    "FileBase" varchar);""")
        # c.execute(
        #     """
        #     CREATE INDEX alias_pin_IDX ON alias(pin);
        #     """)

        cur.execute("""DELETE FROM manual."Tbl_RawData_DRI_Ang01";""")  # truncate
        helper.logger.info("Table Tbl_RawData_DRI_Ang01 has been created")
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (ang01): %s, args=%s: ", e, e.args)


def df_ang_to_db_postgres15(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')
        cur.execute("SET search_path TO manual")
        df.to_sql(name='Tbl_RawData_DRI_Ang15', schema='manual', con=engine, if_exists='append', index=False)
        cur.execute('SELECT count(*) as records FROM "Tbl_RawData_DRI_Ang15"')
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_DRI_Ang15 has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (ang15): %s, args=%s: ",e, e.args)


def df_ang_to_db_postgres01(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')
        cur.execute("SET search_path TO manual")
        df.to_sql(name='Tbl_RawData_DRI_Ang01', schema='manual', con=engine, if_exists='append', index=False)
        cur.execute('SELECT count(*) as records FROM "Tbl_RawData_DRI_Ang01"')
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_DRI_Ang01 has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (ang01): %s, args=%s: ",e, e.args)


def import_ang_postgres15(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port, angpath, search_ang):
    try:
        helper.logger.info("Started ANG 15")
        helper.logger.info("-------------------------")
        # db_path = os.path.join(path, database)
        # list_files = fm.walk(os.path.join(path, dir_in), '.xlsx')
        list_files = fm.walk(angpath, '.xlsx')
        helper.logger.info(list_files)
        conn = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        create_table_ang_postgres15(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        for full_path in list_files:
            helper.logger.info("Behandel file: %s", full_path)
            file_without_ext = fm.get_filename_without_extension(full_path)
            table = determine_import_table15(file_without_ext,search_ang)
            if table == "Tbl_RawData_DRI_Ang15":
                df = pd.read_excel(full_path, sheet_name='15', usecols='A:AY')
                df['FileBase'] = full_path
                df.rename(columns={
                    df.columns[0]: "PL2-embargo",
                    df.columns[1]: "PL2-typ-info",
                    df.columns[2]: "Pers sleutel",
                    df.columns[3]: "Fei-typ-info",
                    df.columns[4]: "fei-embargo",
                    df.columns[5]: "Aard feit 1",
                    df.columns[6]: "Aard feit 2",
                    df.columns[7]: "Aard feit 3",
                    df.columns[8]: "Referte",
                    df.columns[9]: "PV Nr",
                    df.columns[10]: "Prefix",
                    df.columns[11]: "Rapport Nr",
                    df.columns[12]: "Register Eenheid",
                    df.columns[13]: "Lage datum",
                    df.columns[14]: "Hooge datum",
                    df.columns[15]: "Land",
                    df.columns[16]: "Gemeente",
                    df.columns[17]: "Straat",
                    df.columns[18]: "Nr",
                    df.columns[19]: "Appart Nr",
                    df.columns[20]: "Bestem Plaats 1",
                    df.columns[21]: "Bestem Plaats Lib 1",
                    df.columns[22]: "Bestem Plaats klas 1",
                    df.columns[23]: "Bestem Plaats Lib 1-",
                    df.columns[24]: "Bestem Plaats 2",
                    df.columns[25]: "Bestem Plaats Lib 2",
                    df.columns[26]: "Bestem Plaats klas 2",
                    df.columns[27]: "Bestem Plaats Lib 2-",
                    df.columns[28]: "Bestem Plaats 3",
                    df.columns[29]: "Bestem Plaats Lib 3",
                    df.columns[30]: "Bestem Plaats klas 3",
                    df.columns[31]: "Bestem Plaats Lib 3-",
                    df.columns[32]: "Reden reg",
                    df.columns[33]: "TNM",
                    df.columns[34]: "Ver Eenheid TNM",
                    df.columns[35]: "VervalDatum",
                    df.columns[36]: "Reden reg1",
                    df.columns[37]: "TNM1",
                    df.columns[38]: "Ver Eenheid TNM1",
                    df.columns[39]: "VervalDatum1",
                    df.columns[40]: "Unnamed40",
                    df.columns[41]: "Unnamed41",
                    df.columns[42]: "Unnamed42",
                    df.columns[43]: "Unnamed43",
                    df.columns[44]: "Unnamed44",
                    df.columns[45]: "Unnamed45",
                    df.columns[46]: "Unnamed46",
                    df.columns[47]: "FEI SLEUT",
                    df.columns[48]: "AFE1",
                    df.columns[49]: "Unnamed49",
                    df.columns[50]: "Unnamed50",
                    df.columns[51]: "FileBase"}
                    , inplace=True)
                df = df.iloc[1:]
                helper.logger.info("Import %s records in df from file %s", len(df), full_path)
                # print("Print df Ang: ", df)
                df_ang_to_db_postgres15(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
                helper.logger.info("Ang file is imported into db")
                helper.logger.info("==============================================================================================")
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (ang15): %s, args=%s: ",e, e.args)

def import_ang_postgres01(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port, angpath, search_ang):
    try:
        helper.logger.info("Started ANG 01")
        helper.logger.info("-------------------------")
        # db_path = os.path.join(path, database)
        # list_files = fm.walk(os.path.join(path, dir_in), '.xlsx')
        list_files = fm.walk(angpath, '.xlsx')
        helper.logger.info(list_files)
        conn = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        create_table_ang_postgres01(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        for full_path in list_files:
            helper.logger.info("Behandel file: %s", full_path)
            file_without_ext = fm.get_filename_without_extension(full_path)
            table = determine_import_table01(file_without_ext,search_ang)
            if table == "Tbl_RawData_DRI_Ang01":
                df = pd.read_excel(full_path, sheet_name='01', usecols='A:N')
                df['FileBase'] = full_path
                #todo: wis franstalige kolomhoofding
                helper.logger.info("Import %s records in df from file %s", len(df), full_path)
                # print("Print df Ang: ", df)
                df_ang_to_db_postgres01(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
                helper.logger.info("Ang file is imported into db")
                helper.logger.info("==============================================================================================")
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (ang01): %s, args=%s: ",e, e.args)