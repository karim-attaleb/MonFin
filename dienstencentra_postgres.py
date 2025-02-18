import filemanager as fm
import table as tb
import re
from sqlalchemy import create_engine
import requests
from bs4 import BeautifulSoup
import os
import helper
import pandas as pd
from configparser import ConfigParser
import chardet
import openpyxl
import csv
from datetime import datetime


url = "https://economie.fgov.be/nl/themas/ondernemingen/een-onderneming-beheren-en/registratie-van-de-aanbieders"
# folder = r"A:\Process\IN\KBOSelect\Dienstencentra"


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


def create_table_fod_dienst_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        cur.execute("SET search_path TO manual")
        cur.execute("""CREATE TABLE if not exists manual."Tbl_RawData_Fod_Dienstencentra" (
                            "Ondernemingsnummer" varchar,
                            "Aanvrager" varchar,
                            "Type onderneming" varchar,
                            "Type van diensten" varchar,
                            "Adres" varchar,
                            "Postcode" int,
                            "Gemeente" varchar,
                            "Adressen dienstverlening" varchar,
                            "Telgsm" varchar,
                            "E-mail" varchar,
                            "Website" varchar,
                            "Begin datum registratie" timestamp,
                            "FileBase" varchar);""")

        # c.execute(
        #     """
        #     CREATE INDEX alias_pin_IDX ON alias(pin);
        #     """)

        cur.execute("""DELETE FROM manual."Tbl_RawData_Fod_Dienstencentra";""")  # truncate
        helper.logger.info("Table manual.Tbl_RawData_Fod_Dienstencentra has been created")
        conn.commit()
        conn.close()

    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (dienstencentra): %s, args=%s: ", e, e.args)


# issue: kolomhoofdingen bevatten linebreaks
def df_fod_dienst_to_db_postgres(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')
        cur.execute("SET search_path TO manual")
        df.to_sql(name='Tbl_RawData_Fod_Dienstencentra', schema='manual', con=engine, if_exists='append', index=False)
        cur.execute('SELECT count(*) as records FROM "Tbl_RawData_Fod_Dienstencentra"')
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_Fod_Dienstencentra has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (dienstencentra sql): %s, args=%s: ",e, e.args)


def import_fod_dienst_postgres_auto(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port, dienstencentrapath, searchstring):
    try:
        helper.logger.info("Started DIENSTENCENTRA")
        helper.logger.info("-------------------------")
        # db_path = os.path.join(path, database)
        # list_files = fm.walk(os.path.join(path, dir_in), '.xlsx')
        list_files = fm.walk(dienstencentrapath, '.xlsx')
        helper.logger.info(list_files)
        conn = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        create_table_fod_dienst_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        for full_path in list_files:
            helper.logger.info("Behandel file: %s", full_path)
            file_without_ext = fm.get_filename_without_extension(full_path)
            table = determine_import_table(file_without_ext, searchstring)
            if table == "Tbl_RawData_Fod_Dienstencentra":
                df = pd.read_excel(full_path, sheet_name=0, usecols='A:L')
                # df = pd.read_excel(full_path, sheet_name=0)
                df['FileBase'] = full_path

                #debug
                # with pd.option_context('display.max_columns', None):
                #     print("Before rename")
                #     print(df)
                #
                # print("Column Names:")
                # print(df.columns.tolist())
                # print("Data Types:")
                # print(df.dtypes)
                # print("------------------------------")

                df.rename(columns={
                    df.columns[0]: "Ondernemingsnummer",
                    df.columns[1]: "Aanvrager",
                    df.columns[2]: "Type onderneming",
                    df.columns[3]: "Type van diensten",
                    df.columns[4]: "Adres",
                    df.columns[5]: "Postcode",
                    df.columns[6]: "Gemeente",
                    df.columns[7]: "Adressen dienstverlening",
                    df.columns[8]: "Telgsm",
                    df.columns[9]: "E-mail",
                    df.columns[10]: "Website",
                    df.columns[11]: "Begin datum registratie"}
                    , inplace=True)
                # df['Ondernemingsnummer'] = '0' + df['Ondernemingsnummer'].astype(str)

                # df['Postcode'] = df['Postcode'].astype(str)  # Convert to string to handle non-numeric values
                # df['Postcode'] = df['Postcode'].str.replace(r'\D', '', regex=True)  # Remove non-numeric characters
                # df['Postcode'] = pd.to_numeric(df['Postcode'], errors='coerce',
                #                                downcast='integer')  # Convert to integer

                #converteer datumformat (issue met file die wordt gedownload)
                df['Begin datum registratie'] = df['Begin datum registratie'].apply(
                    lambda x: datetime.strptime(x, '%d/%m/%Y').strftime('%Y-%m-%d'))

                # with pd.option_context('display.max_columns', None):
                #     print("After rename")
                #     print(df)
                #
                # print("Column Names:")
                # print(df.columns.tolist())
                # print("Data Types:")
                # print(df.dtypes)
                # print("------------------------------")

                helper.logger.info("Import %s records in df from file %s", len(df), full_path)
                # print("Print df Dienstencentra: ", df)
                df_fod_dienst_to_db_postgres(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
                helper.logger.info("Dienstencentra file is imported into db")
                helper.logger.info("==============================================================================================")
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (dienstencentra): %s, args=%s: ",e, e.args)


def import_fod_dienst_postgres_manual(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port, dienstencentrapath, searchstring):
    try:
        helper.logger.info("Started DIENSTENCENTRA")
        helper.logger.info("-------------------------")
        # db_path = os.path.join(path, database)
        # list_files = fm.walk(os.path.join(path, dir_in), '.xlsx')
        list_files = fm.walk(dienstencentrapath, '.xlsx')
        helper.logger.info(list_files)
        conn = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        create_table_fod_dienst_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        for full_path in list_files:
            helper.logger.info("Behandel file: %s", full_path)
            file_without_ext = fm.get_filename_without_extension(full_path)
            table = determine_import_table(file_without_ext, searchstring)
            if table == "Tbl_RawData_Fod_Dienstencentra":
                df = pd.read_excel(full_path, sheet_name=0, usecols='A:L')
                # df = pd.read_excel(full_path, sheet_name=0)
                df['FileBase'] = full_path

                #debug
                # with pd.option_context('display.max_columns', None):
                #     print("Before rename")
                #     print(df)
                #
                print("Column Names:")
                print(df.columns.tolist())
                print("Data Types:")
                print(df.dtypes)
                # print("------------------------------")

                df.rename(columns={
                    df.columns[0]: "Ondernemingsnummer",
                    df.columns[1]: "Aanvrager",
                    df.columns[2]: "Type onderneming",
                    df.columns[3]: "Type van diensten",
                    df.columns[4]: "Adres",
                    df.columns[5]: "Postcode",
                    df.columns[6]: "Gemeente",
                    df.columns[7]: "Adressen dienstverlening",
                    df.columns[8]: "Telgsm",
                    df.columns[9]: "E-mail",
                    df.columns[10]: "Website",
                    df.columns[11]: "Begin datum registratie"}
                    , inplace=True)
                # df.columns[11] = pd.to_datetime(df.columns[11])
                df.columns[11] = pd.to_datetime(df.columns[11], format='%d/%m/%Y') #df.style.format({"Begin datum registratie": lambda t: t.strftime("%d/%m/%Y")})
                # df['Ondernemingsnummer'] = '0' + df['Ondernemingsnummer'].astype(str)

                # df['Postcode'] = df['Postcode'].astype(str)  # Convert to string to handle non-numeric values
                # df['Postcode'] = df['Postcode'].str.replace(r'\D', '', regex=True)  # Remove non-numeric characters
                # df['Postcode'] = pd.to_numeric(df['Postcode'], errors='coerce',
                #                                downcast='integer')  # Convert to integer

                # with pd.option_context('display.max_columns', None):
                #     print("After rename")
                #     print(df)
                #
                # print("Column Names:")
                # print(df.columns.tolist())
                # print("Data Types:")
                # print(df.dtypes)
                # print("------------------------------")

                helper.logger.info("Import %s records in df from file %s", len(df), full_path)
                # print("Print df Dienstencentra: ", df)
                df_fod_dienst_to_db_postgres(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
                helper.logger.info("Dienstencentra file is imported into db")
                helper.logger.info("==============================================================================================")
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (dienstencentra): %s, args=%s: ",e, e.args)




def delete_csv_xlsx_files_in_folder(folder):
    for filename in os.listdir(folder):
        if filename.endswith('.csv') or filename.endswith('.xlsx'):
            file_path = os.path.join(folder, filename)
            os.remove(file_path)
            helper.logger.info(f"Deleted: {file_path}")


def delete_csv_files_in_folder(folder):
    for filename in os.listdir(folder):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder, filename)
            os.remove(file_path)
            helper.logger.info(f"Deleted: {file_path}")


def get_csv_from_website(folder):
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")

        # Find all anchor tags <a> with ".csv" in their href attribute
        csv_links = soup.find_all("a", href=lambda href: href and href.endswith(".csv"))

        if csv_links:
            # Assume the first link is the one you want to download
            file_url = "https://economie.fgov.be" + csv_links[0]["href"]
            file_name = os.path.basename(file_url)
            file_name = "downloaded_" + file_name

            file_response = requests.get(file_url)

            if file_response.status_code == 200:
                file_path = os.path.join(folder, file_name)
                with open(file_path, "wb") as file:
                    file.write(file_response.content)

                helper.logger.info(f"File downloaded successfully: {file_name}")
            else:
                helper.logger.info(f"Failed to download the file. Status code: {file_response.status_code}")
        else:
            helper.logger.info("CSV file link not found on the page.")
    else:
        helper.logger.info(f"Failed to fetch the page. Status code: {response.status_code}")

#using pandas
# def convert_csv_to_excel(csv_path, excel_path):
#     encoding = detect_csv_encoding(csv_path)
#     df = pd.read_csv(csv_path, encoding=encoding)
#     df.to_excel(excel_path, index=False)


#using openpyxl
def convert_csv_to_excel(csv_path, excel_path):
    helper.logger.info("Convert csv file to excel")
    encoding = detect_csv_encoding(csv_path)
    wb = openpyxl.Workbook()
    ws = wb.active
    with open(csv_path, encoding=encoding) as f:
        reader = csv.reader(f, delimiter=';')
        for row in reader:
            ws.append(row)
    wb.save(excel_path)

    helper.logger.info("Finished convert csv file to excel")


def detect_csv_encoding(csv_path):
    with open(csv_path, 'rb') as rawdata:
        result = chardet.detect(rawdata.read(100000))
    helper.logger.info("Encoding csvfile:", result)
    return result['encoding']