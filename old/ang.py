import os
import traceback

import filemanager as fm
import helper
import table as tb
import pandas as pd
import re


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


def create_table_ang15(db_path):
    try:
        conn, cur = tb.create_connection(db_path)
        # 01/02/2023: toevoeging van onderstaande velden:
        # [FEI SLEUT] text,
        # AFE1 text,
        # [Unnamed49] text,
        # [Unnamed50] float,
        # 05/06/2023: toevoeging AFE2 en AFE2
        cur.execute("""CREATE TABLE if not exists Tbl_RawData_DRI_Ang15 (
                    [PL2-embargo] text,
                    [PL2-typ-info] text,
                    [Pers sleutel] text,
                    [Fei-typ-info] text,
                    [fei-embargo] text,
                    [Aard feit 1] text,
                    [Aard feit 2] text,
                    [Aard feit 3] text,
                    Referte text,
                    [PV Nr] text,
                    Prefix text,
                    [Rapport Nr] text,
                    [Register Eenheid] text,
                    [Lage datum] text,
                    [Hooge datum] text,
                    Land text,
                    Gemeente text,
                    Straat text,
                    Nr text,
                    [Appart Nr] text,
                    [Bestem Plaats 1] text,
                    [Bestem Plaats Lib 1] text,
                    [Bestem Plaats klas 1] text,
                    [Bestem Plaats Lib 1-] text,
                    [Bestem Plaats 2] text,
                    [Bestem Plaats Lib 2] text,
                    [Bestem Plaats klas 2] text,
                    [Bestem Plaats Lib 2-] text,
                    [Bestem Plaats 3] text,
                    [Bestem Plaats Lib 3] text,
                    [Bestem Plaats klas 3] text,
                    [Bestem Plaats Lib 3-] text,
                    [Reden reg] text,
                    TNM text,
                    [Ver Eenheid TNM] text,
                    VervalDatum text,
                    [Reden reg1] text,
                    [TNM1] text,
                    [Ver Eenheid TNM1] text,
                    [VervalDatum1] text,
                    [Unnamed40] text,
                    [Unnamed41] text,
                    [Unnamed42] float,
                    [Unnamed43] text,
                    [Unnamed44] text,
                    [Unnamed45] text,
                    [Unnamed46] float,
                    [FEI SLEUT] text,
                    AFE1 text,
                    AFE2 text,
                    AFE3 text, 
                    [Unnamed49] text,
                    [Unnamed50] float,
                    FileBase text);""")

        # c.execute(
        #     """
        #     CREATE INDEX alias_pin_IDX ON alias(pin);
        #     """)

        cur.execute("""DELETE FROM Tbl_RawData_DRI_Ang15;""")  # truncate
        helper.logger.info("Table Tbl_RawData_DRI_Ang15 has been created")
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (ang15): %s, args=%s: ", e, e.args)


def create_table_ang01(db_path):
    try:
        conn, cur = tb.create_connection(db_path)
        cur.execute("""CREATE TABLE if not exists Tbl_RawData_DRI_Ang01 (
                    [Embargo] text,
                    [Typ-info] text,
                    [Nationaliteit] text,
                    [Sleutel] text,
                    [Naam] text,
                    [Voornaam] text,
                    [Naam en voornaam] text,
                    [Geboorte datum] text,
                    [Geslacht] text,
                    [RRN] text,
                    [Foto ind] text,
                    [Foto laatste datum] text,
                    [Foto num key] text,
                    [Foto datum] text,
                    FileBase text);""")

        # c.execute(
        #     """
        #     CREATE INDEX alias_pin_IDX ON alias(pin);
        #     """)

        cur.execute("""DELETE FROM Tbl_RawData_DRI_Ang01;""")  # truncate
        helper.logger.info("Table Tbl_RawData_DRI_Ang01 has been created")
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (ang01): %s, args=%s: ", e, e.args)

def df_ang_to_db15(df, db_path):
    try:
        conn, cur = tb.create_connection(db_path)

        # df = df.iloc[1:] # Remove the second row from the DataFrame
        #
        # #Controleer voor niet-integer rows in veld AFE1.
        # non_integer_rows = df[~df['AFE1'].astype(str).str.isdigit()] #isdigit werkt enkel op string dus cast, ~ (reverses boolean values)
        # non_integer_rows = non_integer_rows.append(df[df['AFE1'] == 'AFE1']) #als kolom AFE1, AFE1 bevat --> wissen
        #
        # if not non_integer_rows.empty:
        #     helper.logger.info("Rows with non-integer AFE1 values: %s", non_integer_rows)
        #     # df = df[df['AFE1'].astype(str).str.isdigit()] # Drop the rows with non-integer values from the DataFrame
        #     df = df[~df['AFE1'].isin(non_integer_rows['AFE1'])] #non-integer velden worden eruit gefilterd (isin)
        #     print("Non-integer-rows", df)
        # # # Replace null or empty values in AFE1 column with '000'
        # # df['AFE1'] = df['AFE1'].fillna('000') #vervangt nullwaardes met 000
        # # df.loc[df['AFE1'] == '', 'AFE1'] = '000' # vervangt lege waardes met 000
        #
        # #column wordt omgezet naar integer, non-convertible value wordt omgezet naar NaN, en dan omgezet naar 0,
        # #dan wordt kolom gecast naar integer (astype(int)
        # df['AFE1'] = pd.to_numeric(df['AFE1'], errors='coerce').fillna(0).astype(int)

        df.to_sql(name='Tbl_RawData_DRI_Ang15', con=conn, if_exists='append', index=False)
        cur.execute("SELECT count(*) as records FROM Tbl_RawData_DRI_Ang15")
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_DRI_Ang_15 has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        error_message = traceback.format_exc()
        # full_error_message = f"Error: Oeps, something went wrong (ang15): {e}, args={e.args}\n{error_message}"
        # helper.logger.error(full_error_message)
        helper.logger.info("Error: Oeps, something went wrong (ang15): %s, args=%s: ",e, e.args)

def df_ang_to_db01(df, db_path):
    try:
        conn, cur = tb.create_connection(db_path)
        df.to_sql(name='Tbl_RawData_DRI_Ang01', con=conn, if_exists='append', index=False)
        cur.execute("SELECT count(*) as records FROM Tbl_RawData_DRI_Ang01")
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_DRI_Ang_01 has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (ang01): %s, args=%s: ",e, e.args)


def import_ang15(db_path, angpath, search_ang):
    try:
        helper.logger.info("Started ANG")
        helper.logger.info("-------------------------")
        # db_path = os.path.join(path, database)
        # list_files = fm.walk(os.path.join(path, dir_in), '.xlsx')
        list_files = fm.walk(angpath, '.xlsx')
        helper.logger.info(list_files)
        conn = tb.create_connection(db_path)
        create_table_ang15(db_path)
        for full_path in list_files:
            helper.logger.info("Behandel file: %s", full_path)
            file_without_ext = fm.get_filename_without_extension(full_path)
            table = determine_import_table15(file_without_ext,search_ang)
            if table == "Tbl_RawData_DRI_Ang15":
                df = pd.read_excel(full_path, sheet_name='15')
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
                #todo: wis franstalige kolomhoofding
                helper.logger.info("Import %s records in df from file %s", len(df), full_path)
                # print("Print df Ang: ", df)
                df_ang_to_db15(df, db_path)
                helper.logger.info("Ang15 file is imported into db")
                helper.logger.info("==============================================================================================")
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (ang15): %s, args=%s: ",e, e.args)


def import_ang01(db_path, angpath, search_ang):
    try:
        helper.logger.info("Started ANG")
        helper.logger.info("-------------------------")
        # db_path = os.path.join(path, database)
        # list_files = fm.walk(os.path.join(path, dir_in), '.xlsx')
        list_files = fm.walk(angpath, '.xlsx')
        helper.logger.info(list_files)
        conn = tb.create_connection(db_path)
        create_table_ang01(db_path)
        for full_path in list_files:
            helper.logger.info("Behandel file: %s", full_path)
            file_without_ext = fm.get_filename_without_extension(full_path)
            table = determine_import_table01(file_without_ext,search_ang)
            if table == "Tbl_RawData_DRI_Ang01":
                df = pd.read_excel(full_path, sheet_name='01')
                df['FileBase'] = full_path
                #todo: wis franstalige kolomhoofding
                helper.logger.info("Import %s records in df from file %s", len(df), full_path)
                # print("Print df Ang: ", df)
                df_ang_to_db01(df, db_path)
                helper.logger.info("Ang01 file is imported into db")
                helper.logger.info("==============================================================================================")
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (ang01): %s, args=%s: ",e, e.args)