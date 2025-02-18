import os
import filemanager as fm
import helper
import table as tb
import pandas as pd
import re


def determine_import_table(file_without_ext, searchstring):
    try:
        match = re.search(searchstring, file_without_ext)
        # if fm.left(file_without_ext, 6) == "Dienst":
        if match:
            table = "Tbl_RawData_Fod_Dienstencentra"
            return table
        else:
            pass
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (dienstencentra getting tablename based on filename): %s, args=%s: ", e, e.args)


def create_table_fod_dienst(db_path):
    try:
        conn, cur = tb.create_connection(db_path)
        cur.execute("""CREATE TABLE if not exists Tbl_RawData_Fod_Dienstencentra (
                            [Ondernemingsnummer] text,
                            [Aanvrager] object,
                            [Type onderneming] object,
                            [Type van diensten] object,
                            [Adres] object,
                            [Postcode] int64,
                            [Gemeente] object,
                            [Adressen dienstverlening] object,
                            Straatcode int64,
                            [Telgsm] object,
                            [E-mail] object,
                            [Website] object,
                            [Begin datum registratie] datetime64[ns],
                            Huisnummer object,
                            Adrescode object,
                            FileBase object);""")

        # c.execute(
        #     """
        #     CREATE INDEX alias_pin_IDX ON alias(pin);
        #     """)

        cur.execute("""DELETE FROM Tbl_RawData_Fod_Dienstencentra;""")  # truncate
        helper.logger.info("Table Tbl_RawData_Fod_Dienstencentra has been created")
        conn.commit()
        conn.close()

    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (ang): %s, args=%s: ", e, e.args)


# issue: kolomhoofdingen bevatten linebreaks
def df_fod_dienst_to_db(df, db_path):
    try:
        conn, cur = tb.create_connection(db_path)
        df.to_sql(name='Tbl_RawData_Fod_Dienstencentra', con=conn, if_exists='append', index=False)
        cur.execute("SELECT count(*) as records FROM Tbl_RawData_Fod_Dienstencentra")
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_Fod_Dienstencentra has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (dienstencentra): %s, args=%s: ",e, e.args)


def import_fod_dienst(db_path, dienstencentrapath, searchstring):
    try:
        helper.logger.info("Started DIENSTENCENTRA")
        helper.logger.info("-------------------------")
        # db_path = os.path.join(path, database)
        # list_files = fm.walk(os.path.join(path, dir_in), '.xlsx')
        list_files = fm.walk(dienstencentrapath, '.xlsx')
        helper.logger.info(list_files)
        conn = tb.create_connection(db_path)
        create_table_fod_dienst(db_path)
        for full_path in list_files:
            helper.logger.info("Behandel file: %s", full_path)
            file_without_ext = fm.get_filename_without_extension(full_path)
            table = determine_import_table(file_without_ext, searchstring)
            if table == "Tbl_RawData_Fod_Dienstencentra":
                df = pd.read_excel(full_path, sheet_name=0)
                df['FileBase'] = full_path
                df.rename(columns={
                    df.columns[0]: "Ondernemingsnummer",
                    df.columns[1]: "Aanvrager",
                    df.columns[2]: "Type onderneming",
                    df.columns[3]: "Type van diensten",
                    df.columns[4]: "Adres",
                    df.columns[5]: "Postcode",
                    df.columns[6]: "Gemeente",
                    df.columns[7]: "Adressen dienstverlening",
                    df.columns[8]: "Straatcode",
                    df.columns[9]: "Telgsm",
                    df.columns[10]: "E-mail",
                    df.columns[11]: "Website",
                    df.columns[12]: "Begin datum registratie",
                    df.columns[13]: "Huisnummer",
                    df.columns[14]: "Adrescode"}
                    , inplace=True)
                df['Ondernemingsnummer'] = '0' + df['Ondernemingsnummer'].astype(str)
                helper.logger.info("Import %s records in df from file %s", len(df), full_path)
                # print("Print df Dienstencentra: ", df)
                df_fod_dienst_to_db(df, db_path)
                helper.logger.info("Dienstencentra file is imported into db")
                helper.logger.info("==============================================================================================")
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (dienstencentra): %s, args=%s: ",e, e.args)