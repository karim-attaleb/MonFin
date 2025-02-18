import os
import filemanager as fm
import helper
import table as tb
import pandas as pd
import re

def determine_import_table(file_without_ext, search_adres):
    # if fm.left(file_without_ext, 7) == "Geocode":
    # match = re.search(r"Geocode", file_without_ext)
    match = re.search(search_adres, file_without_ext)
    if match:
        table = "Tbl_RawData_Adres"
        return table
    else:
        pass


def create_table_adres(db_path):
    try:
        conn, cur = tb.create_connection(db_path)
        cur.execute("""CREATE TABLE if not exists Tbl_RawData_Adres (
                    X float64,
                    Y float64,
                    Onderne text,
                    Nummer text,
                    Straat text,
                    Postcode int64, 
                    Gemeente text,
                    ACQ text,
                    PRE int64,
                    FileBase text);""")

        cur.execute("""DELETE FROM Tbl_RawData_Adres;""")  # truncate
        helper.logger.info("Table Tbl_RawData_Adres has been created")
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (adres): %s, args=%s: ", e, e.args)


def df_adres_to_db(df, db_path):
    try:
        conn, cur = tb.create_connection(db_path)
        df.to_sql(name='Tbl_RawData_Adres', con=conn, if_exists='append', index=False)
        cur.execute("SELECT count(*) as records FROM Tbl_RawData_Adres")
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_Adres has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (adres): %s, args=%s: ",e, e.args)


def import_adres(db_path, adrespath, search_adres):
    try:
        helper.logger.info("Started Adres")
        helper.logger.info("-------------------------")
        # db_path = os.path.join(path, database)
        # list_files = fm.walk(os.path.join(path, dir_in), '.xlsx')
        list_files = fm.walk(os.path.join(adrespath), '.xlsx')
        helper.logger.info(list_files)
        conn = tb.create_connection(db_path)
        create_table_adres(db_path)
        for full_path in list_files:
            helper.logger.info("Behandel file: %s", full_path)
            file_without_ext = fm.get_filename_without_extension(full_path)
            table = determine_import_table(file_without_ext, search_adres)
            if table == "Tbl_RawData_Adres":
                df = pd.read_excel(full_path, sheet_name='Outputgeocode')
                df['FileBase'] = full_path
                df['Onderne'] = '0' + df['Onderne'].astype(str)
                helper.logger.info("Import %s records in df from file %s", len(df), full_path)
                # print("Print df Ang: ", df)
                df_adres_to_db(df, db_path)
                helper.logger.info("Adres file is imported into db")
                helper.logger.info("==============================================================================================")
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (adres): %s, args=%s: ",e, e.args)