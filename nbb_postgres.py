import os
import filemanager as fm
import helper
import pandas as pd
import table as tb
import io
import re
from sqlalchemy import create_engine
import time



def determine_import_table(file_without_ext, searchstring_annual, seachstring_ratio, searchstring_rubric):
    try:
        match_annual = re.search(searchstring_annual, file_without_ext)
        match_ratio = re.search(seachstring_ratio, file_without_ext)
        match_rubric = re.search(searchstring_rubric, file_without_ext)
        print(file_without_ext)
        # if fm.left(file_without_ext, 6) == "annual":
        if match_annual:
            table = "Tbl_RawData_NBB_AnnualAccountReport"
            return table
        # elif fm.left(file_without_ext, 11) == "ratioReport":
        elif match_ratio:
            table = "Tbl_RawData_NBB_ratioReport"
            return table
        # elif fm.left(file_without_ext, 12) == "rubricReport":
        elif match_rubric:
            table = "Tbl_RawData_NBB_rubricReport"
            return table
        # elif fm.left(file_without_ext, 11) == "inputReport":
        #     table = "Tbl_RawData_NBB_inputReport"
        #     return table
        else:
            pass
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb getting tablename based on filename): %s, args=%s: ", e, e.args)

#region Annual
def create_table_nbb_annual_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        cur.execute("SET search_path TO manual")
        cur.execute("""CREATE TABLE if not exists manual."Tbl_RawData_NBB_AnnualAccountReport" (
                        "CompanyNumber" varchar,
                        "CompanyNameFR" varchar,
                        "StreetFR" varchar,
                        "HouseNumberFR" varchar,
                        "BusNumberFR" varchar,
                        "PostalCodeDE" int,
                        "CityFR" varchar,
                        "CountryCodeFR" varchar,
                        "CompanyNameDE" varchar,
                        "StreetDE" varchar,
                        "HouseNumberDE" varchar,
                        "BusNumberDE" varchar,
                        "PostalCodeDE1" varchar,
                        "CityDE" varchar,
                        "CountryCodeDE" varchar,
                        "CompanyNameNL" varchar,
                        "StreetNL" varchar,
                        "HouseNumberNL" varchar,
                        "BusNumberNL" varchar,
                        "PostalCodeNL" varchar,
                        "CityNL" varchar,
                        "CountryCodeNL" int,
                        "CompanyNameEN" varchar,
                        "CompanyNameOther" varchar,
                        "JuridicalFormCode" int,
                        "JuridicalSituationCode" varchar,
                        "JuridicalSituationDate" timestamp,
                        "NaceCode" varchar,
                        "NisCode" varchar,
                        "AccountingYear" int,
                        "StartDateAccounting" timestamp,
                        "EndDateAccounting" timestamp,
                        "NbMonthAccountingYear" int,
                        "DateGeneralAssembly" timestamp,
                        "DateDeposit" timestamp,
                        "ExternalFlag" varchar,
                        "FileBase" varchar);""")

    # c.execute(
    #     """
    #     CREATE INDEX alias_pin_IDX ON alias(pin);
    #     """)

        cur.execute("""DELETE FROM manual."Tbl_RawData_NBB_AnnualAccountReport";""")  # truncate
        helper.logger.info("Table Tbl_RawData_NBB_AnnualAccountReport has been created")
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb_annual): %s, args=%s: ",e, e.args)


def df_nbb_annual_to_db_postgres(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')
        cur.execute("SET search_path TO manual")
        df.to_sql(name='Tbl_RawData_NBB_AnnualAccountReport', schema='manual', con=engine, if_exists='append', index=False)
        cur.execute('SELECT count(*) as records FROM "Tbl_RawData_NBB_AnnualAccountReport"')
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_NBB_AnnualAccountReport has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb anual): %s, args=%s: ", e, e.args)
#endregion Annual

#region Ratio
def create_table_nbb_ratio_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        cur.execute("SET search_path TO manual")
        cur.execute("""CREATE TABLE if not exists manual."Tbl_RawData_NBB_RatioReport" (
                    "CompanyNumber" varchar,
                    "AccountingYear" int,
                    "Schema" varchar,
                    "RAT001" double precision,
                    "RAT002" double precision,
                    "RAT003" double precision,
                    "RAT004" double precision,
                    "RAT005" double precision,
                    "RAT006" double precision,
                    "RAT007" double precision,
                    "RAT008" double precision,
                    "RAT009" double precision,
                    "RAT010" double precision,
                    "RAT011" double precision,
                    "RAT012" double precision,
                    "RAT013" double precision,
                    "RAT014" double precision,
                    "RAT015" double precision,
                    "RAT016" double precision,
                    "RAT017" double precision,
                    "RAT018" double precision,
                    "RAT019" double precision,
                    "RAT020" double precision,
                    "RAT021" double precision,
                    "RAT101" double precision,
                    "RAT102" double precision,
                    "RAT103" double precision,
                    "RAT104" double precision,
                    "RAT105" double precision,
                    "RAT106" double precision,
                    "RAT107" double precision,
                    "RAT108" double precision,
                    "RAT109" double precision,
                    "RAT110" double precision,
                    "RAT111" double precision,
                    "RAT112" double precision,
                    "RAT113" double precision,
                    "RAT114" double precision,
                    "RAT115" double precision,
                    "RAT116" double precision,
                    "RAT117" double precision,
                    "RAT118" double precision,
                    "RAT119" double precision,
                    "RAT120" double precision,
                    "RAT121" double precision,
                    "RAT125" double precision,
                    "RAT127" double precision,
                    "RAT201" double precision,
                    "RAT202" double precision,
                    "RAT203" double precision,
                    "RAT204" double precision,
                    "RAT205" double precision,
                    "RAT206" double precision,
                    "RAT207" double precision,
                    "RAT208" double precision,
                    "RAT209" double precision,
                    "RAT210" double precision,
                    "RAT211" double precision,
                    "RAT212" double precision,
                    "RAT213" double precision,
                    "RAT214" double precision,
                    "RAT217" double precision,
                    "RAT218" double precision,
                    "RAT219" double precision,
                    "RAT220" double precision,
                    "RAT221" double precision,
                    "FileBase" varchar);""")
        cur.execute("""DELETE FROM manual."Tbl_RawData_NBB_RatioReport";""") # truncate
        helper.logger.info("Table Tbl_RawData_NBB_RatioReport has been created")
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb_ratio): %s, args=%s: ",e, e.args)


def df_nbb_ratio_to_db_postgres(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')
        cur.execute("SET search_path TO manual")
        df.to_sql(name='Tbl_RawData_NBB_RatioReport', schema='manual',con=engine, if_exists='append', index=False)
        cur.execute('SELECT count(*) as records FROM "Tbl_RawData_NBB_RatioReport"')
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_NBB_RatioReport has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb ratio): %s, args=%s: ", e, e.args)
#endregion Ratio

#region Rubric
#ARS1
def create_table_nbb_rubric1_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        cur.execute("SET search_path TO manual")
        cur.execute("""CREATE TABLE if not exists manual."Tbl_RawData_NBB_rubricReport1" (
                        "id" SERIAL PRIMARY KEY,
                        "CompanyNumber" varchar,
                        "AccountingYear" int,
                        "Schema" varchar,
                        "RUB 10" double precision,
                        "RUB 10/11" double precision,
                        "RUB 10/15" double precision,
                        "RUB 10/49" double precision,
                        "RUB 100" double precision,
                        "RUB 1001" double precision,
                        "RUB 10011" double precision,
                        "RUB 10012" double precision,
                        "RUB 1002" double precision,
                        "RUB 10021" double precision,
                        "RUB 10022" double precision,
                        "RUB 1003" double precision,
                        "RUB 10031" double precision,
                        "RUB 10031P" double precision,
                        "RUB 10032" double precision,
                        "RUB 10032P" double precision,
                        "RUB 1003P" double precision,
                        "RUB 100P" double precision,
                        "RUB 101" double precision,
                        "RUB 1011" double precision,
                        "RUB 10111" double precision,
                        "RUB 10112" double precision,
                        "RUB 1012" double precision,
                        "RUB 10121" double precision,
                        "RUB 10122" double precision,
                        "RUB 1013" double precision,
                        "RUB 10131" double precision,
                        "RUB 10131P" double precision,
                        "RUB 10132" double precision,
                        "RUB 10132P" double precision,
                        "RUB 1013P" double precision,
                        "RUB 1021" double precision,
                        "RUB 10211" double precision,
                        "RUB 10212" double precision,
                        "RUB 1022" double precision,
                        "RUB 10221" double precision,
                        "RUB 10222" double precision,
                        "RUB 1023" double precision,
                        "RUB 10231" double precision,
                        "RUB 10231P" double precision,
                        "RUB 10232" double precision,
                        "RUB 10232P" double precision,
                        "RUB 1023P" double precision,
                        "RUB 1033" double precision,
                        "RUB 10331" double precision,
                        "RUB 10331P" double precision,
                        "RUB 10332" double precision,
                        "RUB 10332P" double precision,
                        "RUB 1033P" double precision,
                        "RUB 1051" double precision,
                        "RUB 1052" double precision,
                        "RUB 1053" double precision,
                        "RUB 11" double precision,
                        "RUB 110" double precision,
                        "RUB 1100/10" double precision,
                        "RUB 1101" double precision,
                        "RUB 1102" double precision,
                        "RUB 1103" double precision,
                        "RUB 1109/19" double precision,
                        "RUB 110P" double precision,
                        "RUB 111" double precision,
                        "RUB 1111" double precision,
                        "RUB 1112" double precision,
                        "RUB 1113" double precision,
                        "RUB 111P" double precision,
                        "RUB 1121" double precision,
                        "RUB 1122" double precision,
                        "RUB 1123" double precision,
                        "RUB 1131" double precision,
                        "RUB 1132" double precision,
                        "RUB 1133" double precision,
                        "RUB 12" double precision,
                        "RUB 12001" double precision,
                        "RUB 12002" double precision,
                        "RUB 12003" double precision,
                        "RUB 1201" double precision,
                        "RUB 12011" double precision,
                        "RUB 12012" double precision,
                        "RUB 12013" double precision,
                        "RUB 1202" double precision,
                        "RUB 12021" double precision,
                        "RUB 12022" double precision,
                        "RUB 12023" double precision,
                        "RUB 1203" double precision,
                        "RUB 12031" double precision,
                        "RUB 12032" double precision,
                        "RUB 12033" double precision,
                        "RUB 12101" double precision,
                        "RUB 12102" double precision,
                        "RUB 12103" double precision,
                        "RUB 1211" double precision,
                        "RUB 12111" double precision,
                        "RUB 12112" double precision,
                        "RUB 12113" double precision,
                        "RUB 1212" double precision,
                        "RUB 12121" double precision,
                        "RUB 12122" double precision,
                        "RUB 12123" double precision,
                        "RUB 1213" double precision,
                        "RUB 12131" double precision,
                        "RUB 12132" double precision,
                        "RUB 12133" double precision,
                        "RUB 13" double precision,
                        "RUB 130" double precision,
                        "RUB 130/1" double precision,
                        "RUB 1301" double precision,
                        "RUB 1302" double precision,
                        "RUB 1303" double precision,
                        "RUB 131" double precision,
                        "RUB 1310" double precision,
                        "RUB 1311" double precision,
                        "RUB 1312" double precision,
                        "RUB 1313" double precision,
                        "RUB 1319" double precision,
                        "RUB 132" double precision,
                        "RUB 1321" double precision,
                        "RUB 1322" double precision,
                        "RUB 1323" double precision,
                        "RUB 133" double precision,
                        "RUB 1331" double precision,
                        "RUB 1332" double precision,
                        "RUB 1333" double precision,
                        "RUB 1341" double precision,
                        "RUB 1342" double precision,
                        "RUB 1343" double precision,
                        "RUB 14" double precision,
                        "RUB 14P" double precision,
                        "RUB 15" double precision,
                        "RUB 1501" double precision,
                        "RUB 1502" double precision,
                        "RUB 1511" double precision,
                        "RUB 1512" double precision,
                        "RUB 1521" double precision,
                        "RUB 1522" double precision,
                        "RUB 16" double precision,
                        "RUB 160" double precision,
                        "RUB 160/5" double precision,
                        "RUB 161" double precision,
                        "RUB 162" double precision,
                        "RUB 163" double precision,
                        "RUB 164/5" double precision,
                        "RUB 168" double precision,
                        "RUB 17" double precision,
                        "RUB 17/49" double precision,
                        "RUB 170" double precision,
                        "RUB 170/4" double precision,
                        "RUB 171" double precision,
                        "RUB 172" double precision,
                        "RUB 172/3" double precision,
                        "RUB 173" double precision,
                        "RUB 174" double precision,
                        "RUB 174/0" double precision,
                        "RUB 175" double precision,
                        "RUB 1750" double precision,
                        "RUB 176" double precision,
                        "RUB 178/9" double precision,
                        "RUB 19" double precision,
                        "FileBase" varchar);""")
        cur.execute("""DELETE FROM manual."Tbl_RawData_NBB_rubricReport1";""")  # truncate
        cur.execute("""ALTER SEQUENCE manual."Tbl_RawData_NBB_rubricReport1_id_seq" RESTART WITH 1;""")  # reset id count
        helper.logger.info("Table Tbl_RawData_NBB_rubricReport1 has been created")
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb_rubric1): %s, args=%s: ", e, e.args)

def limit_rubric1(df):
    return df.reindex(columns=[
        "CompanyNumber",
        "AccountingYear",
        "Schema",
        "RUB 10",
        "RUB 10/11",
        "RUB 10/15",
        "RUB 10/49",
        "RUB 100",
        "RUB 1001",
        "RUB 10011",
        "RUB 10012",
        "RUB 1002",
        "RUB 10021",
        "RUB 10022",
        "RUB 1003",
        "RUB 10031",
        "RUB 10031P",
        "RUB 10032",
        "RUB 10032P",
        "RUB 1003P",
        "RUB 100P",
        "RUB 101", #*
        "RUB 1011",
        "RUB 10111",
        "RUB 10112",
        "RUB 1012",
        "RUB 10121",
        "RUB 10122",
        "RUB 1013",
        "RUB 10131",
        "RUB 10131P",
        "RUB 10132",
        "RUB 10132P",
        "RUB 1013P",
        "RUB 1021",
        "RUB 10211",
        "RUB 10212",
        "RUB 1022",
        "RUB 10221",
        "RUB 10222",
        "RUB 1023",
        "RUB 10231",
        "RUB 10231P",
        "RUB 10232",
        "RUB 10232P",
        "RUB 1023P",
        "RUB 1033",
        "RUB 10331",
        "RUB 10331P",
        "RUB 10332",
        "RUB 10332P",
        "RUB 1033P",
        "RUB 1051",
        "RUB 1052",
        "RUB 1053",
        "RUB 11",
        "RUB 110",
        "RUB 1100/10",
        "RUB 1101",
        "RUB 1102",
        "RUB 1103",
        "RUB 1109/19",
        "RUB 110P",
        "RUB 111",
        "RUB 1111",
        "RUB 1112",
        "RUB 1113",
        "RUB 111P",
        "RUB 1121", #*
        "RUB 1122", #*
        "RUB 1123", #*
        "RUB 1131",
        "RUB 1132", #*
        "RUB 1133",
        "RUB 12",
        "RUB 12001",
        "RUB 12002",
        "RUB 12003",
        "RUB 1201",
        "RUB 12011",
        "RUB 12012",
        "RUB 12013",
        "RUB 1202",
        "RUB 12021",
        "RUB 12022",
        "RUB 12023",
        "RUB 1203",
        "RUB 12031",
        "RUB 12032",
        "RUB 12033",
        "RUB 12101",
        "RUB 12102",
        "RUB 12103",
        "RUB 1211",
        "RUB 12111",
        "RUB 12112",
        "RUB 12113",
        "RUB 1212",
        "RUB 12121",
        "RUB 12122",
        "RUB 12123",
        "RUB 1213",
        "RUB 12131",
        "RUB 12132",
        "RUB 12133",
        "RUB 13",
        "RUB 130",
        "RUB 130/1",
        "RUB 1301",
        "RUB 1302",
        "RUB 1303",
        "RUB 131", #*
        "RUB 1310", #*
        "RUB 1311",
        "RUB 1312", #*
        "RUB 1313", #*
        "RUB 1319",
        "RUB 132",
        "RUB 1321",
        "RUB 1322",
        "RUB 1323",
        "RUB 133",
        "RUB 1331",
        "RUB 1332",
        "RUB 1333",
        "RUB 1341",
        "RUB 1342",
        "RUB 1343",
        "RUB 14",
        "RUB 14P",
        "RUB 15",
        "RUB 1501",
        "RUB 1502", #*
        "RUB 1511",
        "RUB 1512", #*
        "RUB 1521",
        "RUB 1522", #*
        "RUB 16",
        "RUB 160",
        "RUB 160/5",
        "RUB 161",
        "RUB 162",
        "RUB 163",
        "RUB 164/5",
        "RUB 168",
        "RUB 17",
        "RUB 17/49",
        "RUB 170",
        "RUB 170/4",
        "RUB 171", #*
        "RUB 172",
        "RUB 172/3",
        "RUB 173",
        "RUB 174",
        "RUB 174/0",
        "RUB 175",
        "RUB 1750", #*
        "RUB 176", #*
        "RUB 178/9",
        "RUB 19", #*
        "FileBase"], fill_value=0)

def df_nbb_rubric_to_db1_postgres(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')
        cur.execute("SET search_path TO manual")
        df.to_sql(name='Tbl_RawData_NBB_rubricReport1', schema='manual', con=engine, if_exists='append', index=False)
        cur.execute('SELECT count(*) as records FROM "Tbl_RawData_NBB_rubricReport1"')
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_NBB_rubricReport1 has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb rubric1): %s, args=%s: ", e, e.args)

#ARS2
def create_table_nbb_rubric2_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        cur.execute("SET search_path TO manual")
        cur.execute("""CREATE TABLE if not exists manual."Tbl_RawData_NBB_rubricReport2" (
                        "id" SERIAL PRIMARY KEY,
                        "CompanyNumber" varchar,
                        "AccountingYear" int,
                        "Schema" varchar,
                        "RUB 20" double precision,
                        "RUB 20/58" double precision,
                        "RUB 200/2" double precision,
                        "RUB 204" double precision,
                        "RUB 2051" double precision,
                        "RUB 2052" double precision,
                        "RUB 2053" double precision,
                        "RUB 20P" double precision,
                        "RUB 21" double precision,
                        "RUB 21/28" double precision,
                        "RUB 210" double precision,
                        "RUB 2101" double precision,
                        "RUB 2102" double precision,
                        "RUB 2103" double precision,
                        "RUB 211" double precision,
                        "RUB 2111" double precision,
                        "RUB 2112" double precision,
                        "RUB 2113" double precision,
                        "RUB 212" double precision,
                        "RUB 2121" double precision,
                        "RUB 2122" double precision,
                        "RUB 2123" double precision,
                        "RUB 213" double precision,
                        "RUB 2131" double precision,
                        "RUB 2132" double precision,
                        "RUB 2133" double precision,
                        "RUB 22" double precision,
                        "RUB 22/27" double precision,
                        "RUB 23" double precision,
                        "RUB 24" double precision,
                        "RUB 25" double precision,
                        "RUB 250" double precision,
                        "RUB 251" double precision,
                        "RUB 252" double precision,
                        "RUB 26" double precision,
                        "RUB 27" double precision,
                        "RUB 28" double precision,
                        "RUB 280" double precision,
                        "RUB 280/1" double precision,
                        "RUB 281" double precision,
                        "RUB 281P" double precision,
                        "RUB 282" double precision,
                        "RUB 282/3" double precision,
                        "RUB 283" double precision,
                        "RUB 283P" double precision,
                        "RUB 284" double precision,
                        "RUB 284/8" double precision,
                        "RUB 285/8" double precision,
                        "RUB 285/8P" double precision,
                        "RUB 29" double precision,
                        "RUB 29/58" double precision,
                        "RUB 290" double precision,
                        "RUB 291" double precision,
                        "FileBase" varchar);""")
        cur.execute("""DELETE FROM manual."Tbl_RawData_NBB_rubricReport2";""")  # truncate
        cur.execute("""ALTER SEQUENCE manual."Tbl_RawData_NBB_rubricReport2_id_seq" RESTART WITH 1;""")  # reset id count
        helper.logger.info("Table Tbl_RawData_NBB_rubricReport2 has been created")
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb_rubric2): %s, args=%s: ", e, e.args)

def limit_rubric2(df):
    return df.reindex(columns=[
        "CompanyNumber",
        "AccountingYear",
        "Schema",
        "RUB 20",
        "RUB 20/58",
        "RUB 200/2",
        "RUB 204",
        "RUB 2051",
        "RUB 2052",
        "RUB 2053",
        "RUB 20P",
        "RUB 21",
        "RUB 21/28",
        "RUB 210",
        "RUB 2101",
        "RUB 2102",
        "RUB 2103",
        "RUB 211",
        "RUB 2111",
        "RUB 2112",
        "RUB 2113",
        "RUB 212",
        "RUB 2121",
        "RUB 2122",
        "RUB 2123",
        "RUB 213",
        "RUB 2131",
        "RUB 2132",
        "RUB 2133",
        "RUB 22",
        "RUB 22/27",
        "RUB 23",
        "RUB 24",
        "RUB 25",
        "RUB 250",
        "RUB 251",
        "RUB 252",
        "RUB 26",
        "RUB 27",
        "RUB 28",
        "RUB 280",
        "RUB 280/1",
        "RUB 281",
        "RUB 281P",
        "RUB 282",
        "RUB 282/3",
        "RUB 283",
        "RUB 283P",
        "RUB 284",
        "RUB 284/8",
        "RUB 285/8",
        "RUB 285/8P",
        "RUB 29",
        "RUB 29/58",
        "RUB 290",
        "RUB 291",
        "FileBase"], fill_value=0)

def df_nbb_rubric_to_db2_postgres(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')
        cur.execute("SET search_path TO manual")
        df.to_sql(name='Tbl_RawData_NBB_rubricReport2', schema='manual', con=engine, if_exists='append', index=False)
        cur.execute('SELECT count(*) as records FROM "Tbl_RawData_NBB_rubricReport2"')
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_NBB_rubricReport2 has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb rubric2): %s, args=%s: ", e, e.args)

#ARS3
def create_table_nbb_rubric3_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        cur.execute("SET search_path TO manual")
        cur.execute("""CREATE TABLE if not exists manual."Tbl_RawData_NBB_rubricReport3" (
                        "id" SERIAL PRIMARY KEY,
                        "CompanyNumber" varchar,
                        "AccountingYear" int,
                        "Schema" varchar,
                        "RUB 3" double precision,
                        "RUB 30/31" double precision,
                        "RUB 30/36" double precision,
                        "RUB 3051" double precision,
                        "RUB 3052" double precision,
                        "RUB 3053" double precision,
                        "RUB 3101" double precision,
                        "RUB 3102" double precision,
                        "RUB 3103" double precision,
                        "RUB 3111" double precision,
                        "RUB 3112" double precision,
                        "RUB 3113" double precision,
                        "RUB 3121" double precision,
                        "RUB 3122" double precision,
                        "RUB 3123" double precision,
                        "RUB 3131" double precision,
                        "RUB 3132" double precision,
                        "RUB 3133" double precision,
                        "RUB 32" double precision,
                        "RUB 33" double precision,
                        "RUB 34" double precision,
                        "RUB 3401" double precision,
                        "RUB 3402" double precision,
                        "RUB 3403" double precision,
                        "RUB 3411" double precision,
                        "RUB 3412" double precision,
                        "RUB 3413" double precision,
                        "RUB 3421" double precision,
                        "RUB 3422" double precision,
                        "RUB 3423" double precision,
                        "RUB 3431" double precision,
                        "RUB 3432" double precision,
                        "RUB 3433" double precision,
                        "RUB 35" double precision,
                        "RUB 36" double precision,
                        "RUB 37" double precision,
                        "FileBase" varchar);""")
        cur.execute("""DELETE FROM manual."Tbl_RawData_NBB_rubricReport3";""")  # truncate
        cur.execute("""ALTER SEQUENCE manual."Tbl_RawData_NBB_rubricReport3_id_seq" RESTART WITH 1;""")  # reset id count
        helper.logger.info("Table Tbl_RawData_NBB_rubricReport3 has been created")
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb_rubric3): %s, args=%s: ", e, e.args)

def limit_rubric3(df):
    return df.reindex(columns=[
        "CompanyNumber",
        "AccountingYear",
        "Schema",
        "RUB 3",
        "RUB 30/31",
        "RUB 30/36",
        "RUB 3051",
        "RUB 3052",
        "RUB 3053",
        "RUB 3101",
        "RUB 3102",
        "RUB 3103",
        "RUB 3111",
        "RUB 3112",
        "RUB 3113",
        "RUB 3121",
        "RUB 3122",
        "RUB 3123",
        "RUB 3131",
        "RUB 3132",
        "RUB 3133",
        "RUB 32",
        "RUB 33",
        "RUB 34",
        "RUB 3401",
        "RUB 3402",
        "RUB 3403",
        "RUB 3411",
        "RUB 3412",
        "RUB 3413",
        "RUB 3421",
        "RUB 3422",
        "RUB 3423",
        "RUB 3431",
        "RUB 3432",
        "RUB 3433",
        "RUB 35",
        "RUB 36",
        "RUB 37",
        "FileBase"], fill_value=0)

def df_nbb_rubric_to_db3_postgres(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')
        cur.execute("SET search_path TO manual")
        df.to_sql(name='Tbl_RawData_NBB_rubricReport3', schema='manual', con=engine, if_exists='append', index=False)
        cur.execute('SELECT count(*) as records FROM "Tbl_RawData_NBB_rubricReport3"')
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_NBB_rubricReport3 has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb rubric3): %s, args=%s: ", e, e.args)

#ARS4
def create_table_nbb_rubric4_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        cur.execute("SET search_path TO manual")
        cur.execute("""CREATE TABLE if not exists manual."Tbl_RawData_NBB_rubricReport4" (
                        "id" SERIAL PRIMARY KEY,
                        "CompanyNumber" varchar,
                        "AccountingYear" int,
                        "Schema" varchar,
                        "RUB 40" double precision,
                        "RUB 40/41" double precision,
                        "RUB 41" double precision,
                        "RUB 42" double precision,
                        "RUB 42/48" double precision,
                        "RUB 43" double precision,
                        "RUB 430/8" double precision,
                        "RUB 439" double precision,
                        "RUB 44" double precision,
                        "RUB 440/4" double precision,
                        "RUB 441" double precision,
                        "RUB 45" double precision,
                        "RUB 450" double precision,
                        "RUB 450/3" double precision,
                        "RUB 454/9" double precision,
                        "RUB 46" double precision,
                        "RUB 47/48" double precision,
                        "RUB 490/1" double precision,
                        "RUB 492/3" double precision,
                        "FileBase" varchar);""")
        cur.execute("""DELETE FROM manual."Tbl_RawData_NBB_rubricReport4";""")  # truncate
        cur.execute("""ALTER SEQUENCE manual."Tbl_RawData_NBB_rubricReport4_id_seq" RESTART WITH 1;""")  # reset id count
        helper.logger.info("Table Tbl_RawData_NBB_rubricReport4 has been created")
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb_rubric4): %s, args=%s: ", e, e.args)

def limit_rubric4(df):
    return df.reindex(columns=[
        "CompanyNumber",
        "AccountingYear",
        "Schema",
        "RUB 40",
        "RUB 40/41",
        "RUB 41",
        "RUB 42",
        "RUB 42/48",
        "RUB 43",
        "RUB 430/8",
        "RUB 439",
        "RUB 44",
        "RUB 440/4",
        "RUB 441",
        "RUB 45",
        "RUB 450",
        "RUB 450/3",
        "RUB 454/9",
        "RUB 46",
        "RUB 47/48",
        "RUB 490/1",
        "RUB 492/3",
        "FileBase"], fill_value=0)

def df_nbb_rubric_to_db4_postgres(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')
        cur.execute("SET search_path TO manual")
        df.to_sql(name='Tbl_RawData_NBB_rubricReport4', schema='manual', con=engine, if_exists='append', index=False)
        cur.execute('SELECT count(*) as records FROM "Tbl_RawData_NBB_rubricReport4"')
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_NBB_rubricReport4 has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb rubric4): %s, args=%s: ", e, e.args)

#ARS5
def create_table_nbb_rubric5_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        cur.execute("SET search_path TO manual")
        cur.execute("""CREATE TABLE if not exists manual."Tbl_RawData_NBB_rubricReport5" (
                        "id" SERIAL PRIMARY KEY,
                        "CompanyNumber" varchar,
                        "AccountingYear" int,
                        "Schema" varchar,
                        "RUB 50" double precision,
                        "RUB 50/53" double precision,
                        "RUB 51" double precision,
                        "RUB 51/53" double precision,
                        "RUB 52" double precision,
                        "RUB 53" double precision,
                        "RUB 54/58" double precision,
                        "RUB 5801" double precision,
                        "RUB 5802" double precision,
                        "RUB 5803" double precision,
                        "RUB 58031" double precision,
                        "RUB 58032" double precision,
                        "RUB 58033" double precision,
                        "RUB 5811" double precision,
                        "RUB 5812" double precision,
                        "RUB 5813" double precision,
                        "RUB 58131" double precision,
                        "RUB 58132" double precision,
                        "RUB 58133" double precision,
                        "RUB 5821" double precision,
                        "RUB 5822" double precision,
                        "RUB 5823" double precision,
                        "RUB 5831" double precision,
                        "RUB 5832" double precision,
                        "RUB 5833" double precision,
                        "RUB 5841" double precision,
                        "RUB 5842" double precision,
                        "RUB 5843" double precision,
                        "RUB 5851" double precision,
                        "RUB 5852" double precision,
                        "RUB 5853" double precision,
                        "FileBase" varchar);""")
        cur.execute("""DELETE FROM manual."Tbl_RawData_NBB_rubricReport5";""")  # truncate
        cur.execute("""ALTER SEQUENCE manual."Tbl_RawData_NBB_rubricReport5_id_seq" RESTART WITH 1;""")  # reset id count
        helper.logger.info("Table Tbl_RawData_NBB_rubricReport5 has been created")
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb_rubric5): %s, args=%s: ", e, e.args)

def limit_rubric5(df):
    return df.reindex(columns=[
        "CompanyNumber",
        "AccountingYear",
        "Schema",
        "RUB 50",
        "RUB 50/53",
        "RUB 51",
        "RUB 51/53",
        "RUB 52",
        "RUB 53",
        "RUB 54/58",
        "RUB 5801",
        "RUB 5802",
        "RUB 5803",
        "RUB 58031",
        "RUB 58032",
        "RUB 58033",
        "RUB 5811",
        "RUB 5812",
        "RUB 5813",
        "RUB 58131",
        "RUB 58132",
        "RUB 58133",
        "RUB 5821",
        "RUB 5822",
        "RUB 5823",
        "RUB 5831",
        "RUB 5832",
        "RUB 5833",
        "RUB 5841",
        "RUB 5842",
        "RUB 5843",
        "RUB 5851",
        "RUB 5852",
        "RUB 5853",
        "FileBase"], fill_value=0)

def df_nbb_rubric_to_db5_postgres(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')
        cur.execute("SET search_path TO manual")
        df.to_sql(name='Tbl_RawData_NBB_rubricReport5', schema='manual', con=engine, if_exists='append', index=False)
        cur.execute('SELECT count(*) as records FROM "Tbl_RawData_NBB_rubricReport5"')
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_NBB_rubricReport5 has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb rubric5): %s, args=%s: ", e, e.args)

#ARS6
def create_table_nbb_rubric6_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        cur.execute("""CREATE TABLE if not exists manual."Tbl_RawData_NBB_rubricReport6" (
                        "id" SERIAL PRIMARY KEY,
                        "CompanyNumber" varchar,
                        "AccountingYear" int,
                        "Schema" varchar,
                        "RUB 60" double precision,
                        "RUB 60/61" double precision,
                        "RUB 60/66A" double precision,
                        "RUB 600/8" double precision,
                        "RUB 609" double precision,
                        "RUB 61" double precision,
                        "RUB 617" double precision,
                        "RUB 62" double precision,
                        "RUB 620" double precision,
                        "RUB 621" double precision,
                        "RUB 622" double precision,
                        "RUB 623" double precision,
                        "RUB 624" double precision,
                        "RUB 630" double precision,
                        "RUB 631/4" double precision,
                        "RUB 635" double precision,
                        "RUB 635/8" double precision,
                        "RUB 640" double precision,
                        "RUB 640/8" double precision,
                        "RUB 641/8" double precision,
                        "RUB 649" double precision,
                        "RUB 65" double precision,
                        "RUB 65/66B" double precision,
                        "RUB 650" double precision,
                        "RUB 6501" double precision,
                        "RUB 6502" double precision,
                        "RUB 6503" double precision,
                        "RUB 651" double precision,
                        "RUB 6510" double precision,
                        "RUB 6511" double precision,
                        "RUB 652/9" double precision,
                        "RUB 653" double precision,
                        "RUB 654" double precision,
                        "RUB 655" double precision,
                        "RUB 6560" double precision,
                        "RUB 6561" double precision,
                        "RUB 66" double precision,
                        "RUB 660" double precision,
                        "RUB 661" double precision,
                        "RUB 662" double precision,
                        "RUB 6620" double precision,
                        "RUB 6621" double precision,
                        "RUB 662AB" double precision,
                        "RUB 663" double precision,
                        "RUB 6630" double precision,
                        "RUB 6631" double precision,
                        "RUB 663AB" double precision,
                        "RUB 664/7" double precision,
                        "RUB 664/8" double precision,
                        "RUB 664/8AB" double precision,
                        "RUB 668" double precision,
                        "RUB 669" double precision,
                        "RUB 6690" double precision,
                        "RUB 669AB" double precision,
                        "RUB 66A" double precision,
                        "RUB 66AB" double precision,
                        "RUB 66B" double precision,
                        "RUB 67/77" double precision,
                        "RUB 670/3" double precision,
                        "RUB 680" double precision,
                        "RUB 689" double precision,
                        "RUB 691" double precision,
                        "RUB 691/2" double precision,
                        "RUB 6920" double precision,
                        "RUB 6921" double precision,
                        "RUB 694" double precision,
                        "RUB 694/7" double precision,
                        "RUB 695" double precision,
                        "RUB 696" double precision,
                        "RUB 697" double precision,
                        "FileBase" varchar);""")
        cur.execute("""DELETE FROM manual."Tbl_RawData_NBB_rubricReport6";""")  # truncate
        # time.sleep(3)
        cur.execute("""ALTER SEQUENCE manual."Tbl_RawData_NBB_rubricReport6_id_seq" RESTART WITH 1;""")  # reset id count
        helper.logger.info("Table Tbl_RawData_NBB_rubricReport6 has been created")
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb_rubric6): %s, args=%s: ", e, e.args)

def limit_rubric6(df):
    return df.reindex(columns=[
        "CompanyNumber",
        "AccountingYear",
        "Schema",
        "RUB 60",
        "RUB 60/61",
        "RUB 60/66A",
        "RUB 600/8",
        "RUB 609",
        "RUB 61",
        "RUB 617",
        "RUB 62",
        "RUB 620",
        "RUB 621",
        "RUB 622",
        "RUB 623",
        "RUB 624",
        "RUB 630",
        "RUB 631/4",
        "RUB 635",
        "RUB 635/8",
        "RUB 640",
        "RUB 640/8",
        "RUB 641/8",
        "RUB 649",
        "RUB 65",
        "RUB 65/66B",
        "RUB 650",
        "RUB 6501",
        "RUB 6502",
        "RUB 6503",
        "RUB 651",
        "RUB 6510",
        "RUB 6511",
        "RUB 652/9",
        "RUB 653",
        "RUB 654",
        "RUB 655",
        "RUB 6560",
        "RUB 6561",
        "RUB 66",
        "RUB 660",
        "RUB 661",
        "RUB 662",
        "RUB 6620",
        "RUB 6621",
        "RUB 662AB",
        "RUB 663",
        "RUB 6630",
        "RUB 6631",
        "RUB 663AB",
        "RUB 664/7",
        "RUB 664/8",
        "RUB 664/8AB",
        "RUB 668",
        "RUB 669",
        "RUB 6690",
        "RUB 669AB",
        "RUB 66A",
        "RUB 66AB",
        "RUB 66B",
        "RUB 67/77",
        "RUB 670/3",
        "RUB 680",
        "RUB 689",
        "RUB 691",
        "RUB 691/2",
        "RUB 6920",
        "RUB 6921",
        "RUB 694",
        "RUB 694/7",
        "RUB 695",
        "RUB 696",
        "RUB 697",
        "FileBase"], fill_value=0)

def df_nbb_rubric_to_db6_postgres(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')
        cur.execute("SET search_path TO manual")
        df.to_sql(name='Tbl_RawData_NBB_rubricReport6', con=engine, schema='manual', if_exists='append', index=False)
        cur.execute('SELECT count(*) as records FROM "Tbl_RawData_NBB_rubricReport6"')
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_NBB_rubricReport6 has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb rubric6): %s, args=%s: ", e, e.args)

#ARS7
def create_table_nbb_rubric7_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        cur.execute("SET search_path TO manual")
        cur.execute("""CREATE TABLE if not exists manual."Tbl_RawData_NBB_rubricReport7" (
                        "id" SERIAL PRIMARY KEY,
                        "CompanyNumber" varchar,
                        "AccountingYear" int,
                        "Schema" varchar,
                        "RUB 70" double precision,
                        "RUB 70/76A" double precision,
                        "RUB 71" double precision,
                        "RUB 72" double precision,
                        "RUB 74" double precision,
                        "RUB 740" double precision,
                        "RUB 75" double precision,
                        "RUB 75/76B" double precision,
                        "RUB 750" double precision,
                        "RUB 751" double precision,
                        "RUB 752/9" double precision,
                        "RUB 753" double precision,
                        "RUB 754" double precision,
                        "RUB 755" double precision,
                        "RUB 76" double precision,
                        "RUB 760" double precision,
                        "RUB 761" double precision,
                        "RUB 762" double precision,
                        "RUB 7620" double precision,
                        "RUB 762AB" double precision,
                        "RUB 763" double precision,
                        "RUB 7630" double precision,
                        "RUB 7631" double precision,
                        "RUB 763AB" double precision,
                        "RUB 764/8" double precision,
                        "RUB 764/9" double precision,
                        "RUB 764/9AB" double precision,
                        "RUB 769" double precision,
                        "RUB 76A" double precision,
                        "RUB 76AB" double precision,
                        "RUB 76B" double precision,
                        "RUB 77" double precision,
                        "RUB 780" double precision,
                        "RUB 789" double precision,
                        "RUB 791" double precision,
                        "RUB 791/2" double precision,
                        "RUB 792" double precision,
                        "RUB 794" double precision,
                        "FileBase" varchar);""")
        cur.execute("""DELETE FROM manual."Tbl_RawData_NBB_rubricReport7";""")  # truncate
        cur.execute("""ALTER SEQUENCE manual."Tbl_RawData_NBB_rubricReport7_id_seq" RESTART WITH 1;""")  # reset id count
        helper.logger.info("Table Tbl_RawData_NBB_rubricReport7 has been created")
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb_rubric7): %s, args=%s: ", e, e.args)

def limit_rubric7(df):
    return df.reindex(columns=[
        "CompanyNumber",
        "AccountingYear",
        "Schema",
        "RUB 70",
        "RUB 70/76A",
        "RUB 71",
        "RUB 72",
        "RUB 74",
        "RUB 740",
        "RUB 75",
        "RUB 75/76B",
        "RUB 750",
        "RUB 751",
        "RUB 752/9",
        "RUB 753",
        "RUB 754",
        "RUB 755",
        "RUB 76",
        "RUB 760",
        "RUB 761",
        "RUB 762",
        "RUB 7620",
        "RUB 762AB",
        "RUB 763",
        "RUB 7630",
        "RUB 7631",
        "RUB 763AB",
        "RUB 764/8",
        "RUB 764/9",
        "RUB 764/9AB",
        "RUB 769",
        "RUB 76A",
        "RUB 76AB",
        "RUB 76B",
        "RUB 77",
        "RUB 780",
        "RUB 789",
        "RUB 791",
        "RUB 791/2",
        "RUB 792",
        "RUB 794",
        "FileBase"], fill_value=0)

def df_nbb_rubric_to_db7_postgres(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')
        cur.execute("SET search_path TO manual")
        df.to_sql(name='Tbl_RawData_NBB_rubricReport7', schema='manual', con=engine, if_exists='append', index=False)
        cur.execute('SELECT count(*) as records FROM "Tbl_RawData_NBB_rubricReport7"')
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_NBB_rubricReport7 has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb rubric7): %s, args=%s: ", e, e.args)

#ARS8
def create_table_nbb_rubric8_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        cur.execute("SET search_path TO manual")
        cur.execute("""CREATE TABLE if not exists manual."Tbl_RawData_NBB_rubricReport8" (
                        "id" SERIAL PRIMARY KEY,
                        "CompanyNumber" varchar,
                        "AccountingYear" int,
                        "Schema" varchar,
                        "RUB 8002" double precision,
                        "RUB 8003" double precision,
                        "RUB 8021" double precision,
                        "RUB 8022" double precision,
                        "RUB 8023" double precision,
                        "RUB 8024" double precision,
                        "RUB 8025" double precision,
                        "RUB 8029" double precision,
                        "RUB 8031" double precision,
                        "RUB 8032" double precision,
                        "RUB 8033" double precision,
                        "RUB 8039" double precision,
                        "RUB 8041" double precision,
                        "RUB 8042" double precision,
                        "RUB 8043" double precision,
                        "RUB 8049" double precision,
                        "RUB 8051" double precision,
                        "RUB 8051P" double precision,
                        "RUB 8052" double precision,
                        "RUB 8052P" double precision,
                        "RUB 8053" double precision,
                        "RUB 8053P" double precision,
                        "RUB 8054" double precision,
                        "RUB 8054P" double precision,
                        "RUB 8055" double precision,
                        "RUB 8055P" double precision,
                        "RUB 8056" double precision,
                        "RUB 8059" double precision,
                        "RUB 8059P" double precision,
                        "RUB 8071" double precision,
                        "RUB 8072" double precision,
                        "RUB 8073" double precision,
                        "RUB 8074" double precision,
                        "RUB 8075" double precision,
                        "RUB 8079" double precision,
                        "RUB 8082" double precision,
                        "RUB 8089" double precision,
                        "RUB 8092" double precision,
                        "RUB 8099" double precision,
                        "RUB 8101" double precision,
                        "RUB 8102" double precision,
                        "RUB 8103" double precision,
                        "RUB 8109" double precision,
                        "RUB 8111" double precision,
                        "RUB 8112" double precision,
                        "RUB 8113" double precision,
                        "RUB 8119" double precision,
                        "RUB 8121" double precision,
                        "RUB 8121P" double precision,
                        "RUB 8122" double precision,
                        "RUB 8122P" double precision,
                        "RUB 8123" double precision,
                        "RUB 8123P" double precision,
                        "RUB 8124" double precision,
                        "RUB 8124P" double precision,
                        "RUB 8125" double precision,
                        "RUB 8125P" double precision,
                        "RUB 8126" double precision,
                        "RUB 8129" double precision,
                        "RUB 8129P" double precision,
                        "RUB 81311" double precision,
                        "RUB 81312" double precision,
                        "RUB 81313" double precision,
                        "RUB 8161" double precision,
                        "RUB 8162" double precision,
                        "RUB 8163" double precision,
                        "RUB 8164" double precision,
                        "RUB 8165" double precision,
                        "RUB 8166" double precision,
                        "RUB 8169" double precision,
                        "RUB 8171" double precision,
                        "RUB 8172" double precision,
                        "RUB 8173" double precision,
                        "RUB 8174" double precision,
                        "RUB 8175" double precision,
                        "RUB 8176" double precision,
                        "RUB 8179" double precision,
                        "RUB 8181" double precision,
                        "RUB 8182" double precision,
                        "RUB 8183" double precision,
                        "RUB 8184" double precision,
                        "RUB 8185" double precision,
                        "RUB 8186" double precision,
                        "RUB 8189" double precision,
                        "RUB 8191" double precision,
                        "RUB 8191P" double precision,
                        "RUB 8192" double precision,
                        "RUB 8192P" double precision,
                        "RUB 8193" double precision,
                        "RUB 8193P" double precision,
                        "RUB 8194" double precision,
                        "RUB 8194P" double precision,
                        "RUB 8195" double precision,
                        "RUB 8195P" double precision,
                        "RUB 8196" double precision,
                        "RUB 8196P" double precision,
                        "RUB 8199" double precision,
                        "RUB 8199P" double precision,
                        "RUB 8211" double precision,
                        "RUB 8215" double precision,
                        "RUB 8219" double precision,
                        "RUB 8221" double precision,
                        "RUB 8229" double precision,
                        "RUB 8232" double precision,
                        "RUB 8233" double precision,
                        "RUB 8235" double precision,
                        "RUB 8239" double precision,
                        "RUB 8242" double precision,
                        "RUB 8243" double precision,
                        "RUB 8249" double precision,
                        "RUB 8251" double precision,
                        "RUB 8251P" double precision,
                        "RUB 8252" double precision,
                        "RUB 8252P" double precision,
                        "RUB 8253" double precision,
                        "RUB 8253P" double precision,
                        "RUB 8255" double precision,
                        "RUB 8255P" double precision,
                        "RUB 8259" double precision,
                        "RUB 8259P" double precision,
                        "RUB 8271" double precision,
                        "RUB 8272" double precision,
                        "RUB 8273" double precision,
                        "RUB 8274" double precision,
                        "RUB 8275" double precision,
                        "RUB 8276" double precision,
                        "RUB 8279" double precision,
                        "RUB 8281" double precision,
                        "RUB 8282" double precision,
                        "RUB 8283" double precision,
                        "RUB 8285" double precision,
                        "RUB 8289" double precision,
                        "RUB 8291" double precision,
                        "RUB 8292" double precision,
                        "RUB 8293" double precision,
                        "RUB 8294" double precision,
                        "RUB 8299" double precision,
                        "RUB 8301" double precision,
                        "RUB 8302" double precision,
                        "RUB 8303" double precision,
                        "RUB 8304" double precision,
                        "RUB 8305" double precision,
                        "RUB 8306" double precision,
                        "RUB 8309" double precision,
                        "RUB 8311" double precision,
                        "RUB 8312" double precision,
                        "RUB 8313" double precision,
                        "RUB 8314" double precision,
                        "RUB 8315" double precision,
                        "RUB 8316" double precision,
                        "RUB 8319" double precision,
                        "RUB 8321" double precision,
                        "RUB 8321P" double precision,
                        "RUB 8322" double precision,
                        "RUB 8322P" double precision,
                        "RUB 8323" double precision,
                        "RUB 8323P" double precision,
                        "RUB 8324" double precision,
                        "RUB 8324P" double precision,
                        "RUB 8325" double precision,
                        "RUB 8325P" double precision,
                        "RUB 8326" double precision,
                        "RUB 8326P" double precision,
                        "RUB 8329" double precision,
                        "RUB 8329P" double precision,
                        "RUB 8361" double precision,
                        "RUB 8362" double precision,
                        "RUB 8363" double precision,
                        "RUB 8364" double precision,
                        "RUB 8365" double precision,
                        "RUB 8371" double precision,
                        "RUB 8372" double precision,
                        "RUB 8373" double precision,
                        "RUB 8374" double precision,
                        "RUB 8375" double precision,
                        "RUB 8381" double precision,
                        "RUB 8382" double precision,
                        "RUB 8383" double precision,
                        "RUB 8384" double precision,
                        "RUB 8385" double precision,
                        "RUB 8386" double precision,
                        "RUB 8391" double precision,
                        "RUB 8391P" double precision,
                        "RUB 8392" double precision,
                        "RUB 8392P" double precision,
                        "RUB 8393" double precision,
                        "RUB 8393P" double precision,
                        "RUB 8394" double precision,
                        "RUB 8394P" double precision,
                        "RUB 8395" double precision,
                        "RUB 8395P" double precision,
                        "RUB 8411" double precision,
                        "RUB 8412" double precision,
                        "RUB 8414" double precision,
                        "RUB 8415" double precision,
                        "RUB 8425" double precision,
                        "RUB 8431" double precision,
                        "RUB 8432" double precision,
                        "RUB 8434" double precision,
                        "RUB 8435" double precision,
                        "RUB 8445" double precision,
                        "RUB 8451" double precision,
                        "RUB 8451P" double precision,
                        "RUB 8452" double precision,
                        "RUB 8452P" double precision,
                        "RUB 8453" double precision,
                        "RUB 8453P" double precision,
                        "RUB 8454" double precision,
                        "RUB 8454P" double precision,
                        "RUB 8455" double precision,
                        "RUB 8455P" double precision,
                        "RUB 8471" double precision,
                        "RUB 8472" double precision,
                        "RUB 8473" double precision,
                        "RUB 8474" double precision,
                        "RUB 8475" double precision,
                        "RUB 8479" double precision,
                        "RUB 8481" double precision,
                        "RUB 8482" double precision,
                        "RUB 8483" double precision,
                        "RUB 8484" double precision,
                        "RUB 8485" double precision,
                        "RUB 8489" double precision,
                        "RUB 8495" double precision,
                        "RUB 8501" double precision,
                        "RUB 8502" double precision,
                        "RUB 8503" double precision,
                        "RUB 8504" double precision,
                        "RUB 8505" double precision,
                        "RUB 8511" double precision,
                        "RUB 8512" double precision,
                        "RUB 8514" double precision,
                        "RUB 8515" double precision,
                        "RUB 8521" double precision,
                        "RUB 8521P" double precision,
                        "RUB 8522" double precision,
                        "RUB 8522P" double precision,
                        "RUB 8523" double precision,
                        "RUB 8523P" double precision,
                        "RUB 8524" double precision,
                        "RUB 8524P" double precision,
                        "RUB 8525" double precision,
                        "RUB 8525P" double precision,
                        "RUB 8541" double precision,
                        "RUB 8542" double precision,
                        "RUB 8543" double precision,
                        "RUB 8544" double precision,
                        "RUB 8545" double precision,
                        "RUB 8551" double precision,
                        "RUB 8551P" double precision,
                        "RUB 8552" double precision,
                        "RUB 8552P" double precision,
                        "RUB 8553" double precision,
                        "RUB 8553P" double precision,
                        "RUB 8554" double precision,
                        "RUB 8554P" double precision,
                        "RUB 8555" double precision,
                        "RUB 8555P" double precision,
                        "RUB 8564" double precision,
                        "RUB 8581" double precision,
                        "RUB 8582" double precision,
                        "RUB 8583" double precision,
                        "RUB 8584" double precision,
                        "RUB 8591" double precision,
                        "RUB 8592" double precision,
                        "RUB 8593" double precision,
                        "RUB 8594" double precision,
                        "RUB 8601" double precision,
                        "RUB 8602" double precision,
                        "RUB 8603" double precision,
                        "RUB 8604" double precision,
                        "RUB 8611" double precision,
                        "RUB 8612" double precision,
                        "RUB 8613" double precision,
                        "RUB 8614" double precision,
                        "RUB 8621" double precision,
                        "RUB 8623" double precision,
                        "RUB 8624" double precision,
                        "RUB 8631" double precision,
                        "RUB 8632" double precision,
                        "RUB 8633" double precision,
                        "RUB 8634" double precision,
                        "RUB 8644" double precision,
                        "RUB 8644P" double precision,
                        "RUB 8651" double precision,
                        "RUB 8652" double precision,
                        "RUB 8653" double precision,
                        "RUB 8654" double precision,
                        "RUB 8681" double precision,
                        "RUB 8682" double precision,
                        "RUB 8684" double precision,
                        "RUB 8686" double precision,
                        "RUB 8687" double precision,
                        "RUB 8688" double precision,
                        "RUB 8689" double precision,
                        "RUB 8702" double precision,
                        "RUB 8703" double precision,
                        "RUB 8712" double precision,
                        "RUB 8721" double precision,
                        "RUB 8722" double precision,
                        "RUB 8731" double precision,
                        "RUB 8732" double precision,
                        "RUB 8751" double precision,
                        "RUB 8761" double precision,
                        "RUB 8762" double precision,
                        "RUB 8771" double precision,
                        "RUB 8790" double precision,
                        "RUB 87901" double precision,
                        "RUB 8791" double precision,
                        "RUB 8801" double precision,
                        "RUB 8802" double precision,
                        "RUB 8803" double precision,
                        "RUB 8811" double precision,
                        "RUB 8812" double precision,
                        "RUB 8813" double precision,
                        "RUB 8822" double precision,
                        "RUB 8823" double precision,
                        "RUB 8831" double precision,
                        "RUB 8832" double precision,
                        "RUB 8833" double precision,
                        "RUB 8841" double precision,
                        "RUB 8842" double precision,
                        "RUB 8843" double precision,
                        "RUB 8851" double precision,
                        "RUB 8852" double precision,
                        "RUB 8853" double precision,
                        "RUB 8861" double precision,
                        "RUB 8862" double precision,
                        "RUB 8863" double precision,
                        "RUB 8871" double precision,
                        "RUB 8872" double precision,
                        "RUB 8873" double precision,
                        "RUB 8892" double precision,
                        "RUB 8893" double precision,
                        "RUB 8901" double precision,
                        "RUB 8902" double precision,
                        "RUB 8903" double precision,
                        "RUB 891" double precision,
                        "RUB 8912" double precision,
                        "RUB 8913" double precision,
                        "RUB 892" double precision,
                        "RUB 8921" double precision,
                        "RUB 8922" double precision,
                        "RUB 8932" double precision,
                        "RUB 8951" double precision,
                        "RUB 8952" double precision,
                        "RUB 8961" double precision,
                        "RUB 8962" double precision,
                        "RUB 8972" double precision,
                        "RUB 8981" double precision,
                        "RUB 8982" double precision,
                        "RUB 8991" double precision,
                        "RUB 8992" double precision,
                        "FileBase" varchar);""")
        cur.execute("""DELETE FROM manual."Tbl_RawData_NBB_rubricReport8";""")  # truncate
        cur.execute("""ALTER SEQUENCE manual."Tbl_RawData_NBB_rubricReport8_id_seq" RESTART WITH 1;""")  # reset id count
        helper.logger.info("Table Tbl_RawData_NBB_rubricReport8 has been created")
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb_rubric8): %s, args=%s: ", e, e.args)

def limit_rubric8(df):
    return df.reindex(columns=[
        "CompanyNumber",
        "AccountingYear",
        "Schema",
        "RUB 8002",
        "RUB 8003",
        "RUB 8021",
        "RUB 8022",
        "RUB 8023",
        "RUB 8024",
        "RUB 8025",
        "RUB 8029",
        "RUB 8031",
        "RUB 8032",
        "RUB 8033",
        "RUB 8039",
        "RUB 8041",
        "RUB 8042",
        "RUB 8043",
        "RUB 8049",
        "RUB 8051",
        "RUB 8051P",
        "RUB 8052",
        "RUB 8052P",
        "RUB 8053",
        "RUB 8053P",
        "RUB 8054",
        "RUB 8054P",
        "RUB 8055",
        "RUB 8055P",
        "RUB 8056",
        "RUB 8059",
        "RUB 8059P",
        "RUB 8071",
        "RUB 8072",
        "RUB 8073",
        "RUB 8074",
        "RUB 8075",
        "RUB 8079",
        "RUB 8082",
        "RUB 8089",
        "RUB 8092",
        "RUB 8099",
        "RUB 8101",
        "RUB 8102",
        "RUB 8103",
        "RUB 8109",
        "RUB 8111",
        "RUB 8112",
        "RUB 8113",
        "RUB 8119",
        "RUB 8121",
        "RUB 8121P",
        "RUB 8122",
        "RUB 8122P",
        "RUB 8123",
        "RUB 8123P",
        "RUB 8124",
        "RUB 8124P",
        "RUB 8125",
        "RUB 8125P",
        "RUB 8126",
        "RUB 8129",
        "RUB 8129P",
        "RUB 81311",
        "RUB 81312",
        "RUB 81313",
        "RUB 8161",
        "RUB 8162",
        "RUB 8163",
        "RUB 8164",
        "RUB 8165",
        "RUB 8166",
        "RUB 8169",
        "RUB 8171",
        "RUB 8172",
        "RUB 8173",
        "RUB 8174",
        "RUB 8175",
        "RUB 8176",
        "RUB 8179",
        "RUB 8181",
        "RUB 8182",
        "RUB 8183",
        "RUB 8184",
        "RUB 8185",
        "RUB 8186",
        "RUB 8189",
        "RUB 8191",
        "RUB 8191P",
        "RUB 8192",
        "RUB 8192P",
        "RUB 8193",
        "RUB 8193P",
        "RUB 8194",
        "RUB 8194P",
        "RUB 8195",
        "RUB 8195P",
        "RUB 8196",
        "RUB 8196P",
        "RUB 8199",
        "RUB 8199P",
        "RUB 8211",
        "RUB 8215",
        "RUB 8219",
        "RUB 8221",
        "RUB 8229",
        "RUB 8232",
        "RUB 8233",
        "RUB 8235",
        "RUB 8239",
        "RUB 8242",
        "RUB 8243",
        "RUB 8249",
        "RUB 8251",
        "RUB 8251P",
        "RUB 8252",
        "RUB 8252P",
        "RUB 8253",
        "RUB 8253P",
        "RUB 8255",
        "RUB 8255P",
        "RUB 8259",
        "RUB 8259P",
        "RUB 8271",
        "RUB 8272",
        "RUB 8273",
        "RUB 8274",
        "RUB 8275",
        "RUB 8276",
        "RUB 8279",
        "RUB 8281",
        "RUB 8282",
        "RUB 8283",
        "RUB 8285",
        "RUB 8289",
        "RUB 8291",
        "RUB 8292",
        "RUB 8293",
        "RUB 8294",
        "RUB 8299",
        "RUB 8301",
        "RUB 8302",
        "RUB 8303",
        "RUB 8304",
        "RUB 8305",
        "RUB 8306",
        "RUB 8309",
        "RUB 8311",
        "RUB 8312",
        "RUB 8313",
        "RUB 8314",
        "RUB 8315",
        "RUB 8316",
        "RUB 8319",
        "RUB 8321",
        "RUB 8321P",
        "RUB 8322",
        "RUB 8322P",
        "RUB 8323",
        "RUB 8323P",
        "RUB 8324",
        "RUB 8324P",
        "RUB 8325",
        "RUB 8325P",
        "RUB 8326",
        "RUB 8326P",
        "RUB 8329",
        "RUB 8329P",
        "RUB 8361",
        "RUB 8362",
        "RUB 8363",
        "RUB 8364",
        "RUB 8365",
        "RUB 8371",
        "RUB 8372",
        "RUB 8373",
        "RUB 8374",
        "RUB 8375",
        "RUB 8381",
        "RUB 8382",
        "RUB 8383",
        "RUB 8384",
        "RUB 8385",
        "RUB 8386",
        "RUB 8391",
        "RUB 8391P",
        "RUB 8392",
        "RUB 8392P",
        "RUB 8393",
        "RUB 8393P",
        "RUB 8394",
        "RUB 8394P",
        "RUB 8395",
        "RUB 8395P",
        "RUB 8411",
        "RUB 8412",
        "RUB 8414",
        "RUB 8415",
        "RUB 8425",
        "RUB 8431",
        "RUB 8432",
        "RUB 8434",
        "RUB 8435",
        "RUB 8445",
        "RUB 8451",
        "RUB 8451P",
        "RUB 8452",
        "RUB 8452P",
        "RUB 8453",
        "RUB 8453P",
        "RUB 8454",
        "RUB 8454P",
        "RUB 8455",
        "RUB 8455P",
        "RUB 8471",
        "RUB 8472",
        "RUB 8473",
        "RUB 8474",
        "RUB 8475",
        "RUB 8479",
        "RUB 8481",
        "RUB 8482",
        "RUB 8483",
        "RUB 8484",
        "RUB 8485",
        "RUB 8489",
        "RUB 8495",
        "RUB 8501",
        "RUB 8502",
        "RUB 8503",
        "RUB 8504",
        "RUB 8505",
        "RUB 8511",
        "RUB 8512",
        "RUB 8514",
        "RUB 8515",
        "RUB 8521",
        "RUB 8521P",
        "RUB 8522",
        "RUB 8522P",
        "RUB 8523",
        "RUB 8523P",
        "RUB 8524",
        "RUB 8524P",
        "RUB 8525",
        "RUB 8525P",
        "RUB 8541",
        "RUB 8542",
        "RUB 8543",
        "RUB 8544",
        "RUB 8545",
        "RUB 8551",
        "RUB 8551P",
        "RUB 8552",
        "RUB 8552P",
        "RUB 8553",
        "RUB 8553P",
        "RUB 8554",
        "RUB 8554P",
        "RUB 8555",
        "RUB 8555P",
        "RUB 8564",
        "RUB 8581",
        "RUB 8582",
        "RUB 8583",
        "RUB 8584",
        "RUB 8591",
        "RUB 8592",
        "RUB 8593",
        "RUB 8594",
        "RUB 8601",
        "RUB 8602",
        "RUB 8603",
        "RUB 8604",
        "RUB 8611",
        "RUB 8612",
        "RUB 8613",
        "RUB 8614",
        "RUB 8621",
        "RUB 8623",
        "RUB 8624",
        "RUB 8631",
        "RUB 8632",
        "RUB 8633",
        "RUB 8634",
        "RUB 8644",
        "RUB 8644P",
        "RUB 8651",
        "RUB 8652",
        "RUB 8653",
        "RUB 8654",
        "RUB 8681",
        "RUB 8682",
        "RUB 8684",
        "RUB 8686",
        "RUB 8687",
        "RUB 8688",
        "RUB 8689",
        "RUB 8702",
        "RUB 8703",
        "RUB 8712",
        "RUB 8721",
        "RUB 8722",
        "RUB 8731",
        "RUB 8732",
        "RUB 8751",
        "RUB 8761",
        "RUB 8762",
        "RUB 8771",
        "RUB 8790",
        "RUB 87901",
        "RUB 8791",
        "RUB 8801",
        "RUB 8802",
        "RUB 8803",
        "RUB 8811",
        "RUB 8812",
        "RUB 8813",
        "RUB 8822",
        "RUB 8823",
        "RUB 8831",
        "RUB 8832",
        "RUB 8833",
        "RUB 8841",
        "RUB 8842",
        "RUB 8843",
        "RUB 8851",
        "RUB 8852",
        "RUB 8853",
        "RUB 8861",
        "RUB 8862",
        "RUB 8863",
        "RUB 8871",
        "RUB 8872",
        "RUB 8873",
        "RUB 8892",
        "RUB 8893",
        "RUB 8901",
        "RUB 8902",
        "RUB 8903",
        "RUB 891",
        "RUB 8912",
        "RUB 8913",
        "RUB 892",
        "RUB 8921",
        "RUB 8922",
        "RUB 8932",
        "RUB 8951",
        "RUB 8952",
        "RUB 8961",
        "RUB 8962",
        "RUB 8972",
        "RUB 8981",
        "RUB 8982",
        "RUB 8991",
        "RUB 8992",
        "FileBase"], fill_value=0)

def df_nbb_rubric_to_db8_postgres(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')
        cur.execute("SET search_path TO manual")
        df.to_sql(name='Tbl_RawData_NBB_rubricReport8', schema='manual', con=engine, if_exists='append', index=False)
        cur.execute('SELECT count(*) as records FROM "Tbl_RawData_NBB_rubricReport8"')
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_NBB_rubricReport8 has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb rubric8): %s, args=%s: ", e, e.args)

#ARS9
def create_table_nbb_rubric9_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        cur.execute("""CREATE TABLE if not exists manual."Tbl_RawData_NBB_rubricReport9" (
                        "id" SERIAL PRIMARY KEY,
                        "CompanyNumber" varchar,
                        "AccountingYear" int,
                        "Schema" varchar,
                        "RUB 901" double precision,
                        "RUB 9012" double precision,
                        "RUB 902" double precision,
                        "RUB 9021" double precision,
                        "RUB 9022" double precision,
                        "RUB 9032" double precision,
                        "RUB 9042" double precision,
                        "RUB 9051" double precision,
                        "RUB 9052" double precision,
                        "RUB 9061" double precision,
                        "RUB 9062" double precision,
                        "RUB 9072" double precision,
                        "RUB 9073" double precision,
                        "RUB 9076" double precision,
                        "RUB 9077" double precision,
                        "RUB 9078" double precision,
                        "RUB 9086" double precision,
                        "RUB 9087" double precision,
                        "RUB 9088" double precision,
                        "RUB 9096" double precision,
                        "RUB 9097" double precision,
                        "RUB 9098" double precision,
                        "RUB 9110" double precision,
                        "RUB 9111" double precision,
                        "RUB 9112" double precision,
                        "RUB 9113" double precision,
                        "RUB 9115" double precision,
                        "RUB 9116" double precision,
                        "RUB 9125" double precision,
                        "RUB 9126" double precision,
                        "RUB 9130" double precision,
                        "RUB 9131" double precision,
                        "RUB 9134" double precision,
                        "RUB 9135" double precision,
                        "RUB 9136" double precision,
                        "RUB 9137" double precision,
                        "RUB 9138" double precision,
                        "RUB 9139" double precision,
                        "RUB 9140" double precision,
                        "RUB 9141" double precision,
                        "RUB 9142" double precision,
                        "RUB 9144" double precision,
                        "RUB 9145" double precision,
                        "RUB 9146" double precision,
                        "RUB 9147" double precision,
                        "RUB 9148" double precision,
                        "RUB 9149" double precision,
                        "RUB 9153" double precision,
                        "RUB 9161" double precision,
                        "RUB 91611" double precision,
                        "RUB 91612" double precision,
                        "RUB 9162" double precision,
                        "RUB 91621" double precision,
                        "RUB 91622" double precision,
                        "RUB 91631" double precision,
                        "RUB 91632" double precision,
                        "RUB 9171" double precision,
                        "RUB 91711" double precision,
                        "RUB 91712" double precision,
                        "RUB 9172" double precision,
                        "RUB 91721" double precision,
                        "RUB 91722" double precision,
                        "RUB 9181" double precision,
                        "RUB 91811" double precision,
                        "RUB 91812" double precision,
                        "RUB 9182" double precision,
                        "RUB 91821" double precision,
                        "RUB 91822" double precision,
                        "RUB 9191" double precision,
                        "RUB 91911" double precision,
                        "RUB 91912" double precision,
                        "RUB 9192" double precision,
                        "RUB 91921" double precision,
                        "RUB 91922" double precision,
                        "RUB 9201" double precision,
                        "RUB 92011" double precision,
                        "RUB 92021" double precision,
                        "RUB 9213" double precision,
                        "RUB 9214" double precision,
                        "RUB 9215" double precision,
                        "RUB 9216" double precision,
                        "RUB 9220" double precision,
                        "RUB 9252" double precision,
                        "RUB 9253" double precision,
                        "RUB 9262" double precision,
                        "RUB 9263" double precision,
                        "RUB 9271" double precision,
                        "RUB 9272" double precision,
                        "RUB 9281" double precision,
                        "RUB 9282" double precision,
                        "RUB 9283" double precision,
                        "RUB 9291" double precision,
                        "RUB 9292" double precision,
                        "RUB 9293" double precision,
                        "RUB 9294" double precision,
                        "RUB 9295" double precision,
                        "RUB 9301" double precision,
                        "RUB 9302" double precision,
                        "RUB 9303" double precision,
                        "RUB 9311" double precision,
                        "RUB 9312" double precision,
                        "RUB 9313" double precision,
                        "RUB 9321" double precision,
                        "RUB 9331" double precision,
                        "RUB 9341" double precision,
                        "RUB 9351" double precision,
                        "RUB 9352" double precision,
                        "RUB 9353" double precision,
                        "RUB 9361" double precision,
                        "RUB 9362" double precision,
                        "RUB 9363" double precision,
                        "RUB 9371" double precision,
                        "RUB 9372" double precision,
                        "RUB 9373" double precision,
                        "RUB 9381" double precision,
                        "RUB 9383" double precision,
                        "RUB 9391" double precision,
                        "RUB 9393" double precision,
                        "RUB 9401" double precision,
                        "RUB 9421" double precision,
                        "RUB 9431" double precision,
                        "RUB 9441" double precision,
                        "RUB 9461" double precision,
                        "RUB 9471" double precision,
                        "RUB 9481" double precision,
                        "RUB 9491" double precision,
                        "RUB 9500" double precision,
                        "RUB 9501" double precision,
                        "RUB 9502" double precision,
                        "RUB 9503" double precision,
                        "RUB 9504" double precision,
                        "RUB 9505" double precision,
                        "RUB 95061" double precision,
                        "RUB 95062" double precision,
                        "RUB 95063" double precision,
                        "RUB 9507" double precision,
                        "RUB 95071" double precision,
                        "RUB 95072" double precision,
                        "RUB 95073" double precision,
                        "RUB 95081" double precision,
                        "RUB 95082" double precision,
                        "RUB 95083" double precision,
                        "RUB 9509" double precision,
                        "RUB 95091" double precision,
                        "RUB 95092" double precision,
                        "RUB 95093" double precision,
                        "RUB 9800" double precision,
                        "RUB 9900" double precision,
                        "RUB 9901" double precision,
                        "RUB 9903" double precision,
                        "RUB 9904" double precision,
                        "RUB 9905" double precision,
                        "RUB 9906" double precision,
                        "FileBase" varchar);""")
        cur.execute("""DELETE FROM manual."Tbl_RawData_NBB_rubricReport9";""")  # truncate
        # time.sleep(3)
        cur.execute("""ALTER SEQUENCE manual."Tbl_RawData_NBB_rubricReport9_id_seq" RESTART WITH 1;""")  # reset id count
        helper.logger.info("Table Tbl_RawData_NBB_rubricReport9 has been created")
        conn.commit()
        conn.close()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb_rubric9): %s, args=%s: ", e, e.args)

def limit_rubric9(df):
    return df.reindex(columns=[
        "CompanyNumber",
        "AccountingYear",
        "Schema",
        "RUB 901",
        "RUB 9012",
        "RUB 902",
        "RUB 9021",
        "RUB 9022",
        "RUB 9032",
        "RUB 9042",
        "RUB 9051",
        "RUB 9052",
        "RUB 9061",
        "RUB 9062",
        "RUB 9072",
        "RUB 9073",
        "RUB 9076",
        "RUB 9077",
        "RUB 9078",
        "RUB 9086",
        "RUB 9087",
        "RUB 9088",
        "RUB 9096",
        "RUB 9097",
        "RUB 9098",
        "RUB 9110",
        "RUB 9111",
        "RUB 9112",
        "RUB 9113",
        "RUB 9115",
        "RUB 9116",
        "RUB 9125",
        "RUB 9126",
        "RUB 9130",
        "RUB 9131",
        "RUB 9134",
        "RUB 9135",
        "RUB 9136",
        "RUB 9137",
        "RUB 9138",
        "RUB 9139",
        "RUB 9140",
        "RUB 9141",
        "RUB 9142",
        "RUB 9144",
        "RUB 9145",
        "RUB 9146",
        "RUB 9147",
        "RUB 9148",
        "RUB 9149",
        "RUB 9153",
        "RUB 9161",
        "RUB 91611",
        "RUB 91612",
        "RUB 9162",
        "RUB 91621",
        "RUB 91622",
        "RUB 91631",
        "RUB 91632",
        "RUB 9171",
        "RUB 91711",
        "RUB 91712",
        "RUB 9172",
        "RUB 91721",
        "RUB 91722",
        "RUB 9181",
        "RUB 91811",
        "RUB 91812",
        "RUB 9182",
        "RUB 91821",
        "RUB 91822",
        "RUB 9191",
        "RUB 91911",
        "RUB 91912",
        "RUB 9192",
        "RUB 91921",
        "RUB 91922",
        "RUB 9201",
        "RUB 92011",
        "RUB 92021",
        "RUB 9213",
        "RUB 9214",
        "RUB 9215",
        "RUB 9216",
        "RUB 9220",
        "RUB 9252",
        "RUB 9253",
        "RUB 9262",
        "RUB 9263",
        "RUB 9271",
        "RUB 9272",
        "RUB 9281",
        "RUB 9282",
        "RUB 9283",
        "RUB 9291",
        "RUB 9292",
        "RUB 9293",
        "RUB 9294",
        "RUB 9295",
        "RUB 9301",
        "RUB 9302",
        "RUB 9303",
        "RUB 9311",
        "RUB 9312",
        "RUB 9313",
        "RUB 9321",
        "RUB 9331",
        "RUB 9341",
        "RUB 9351",
        "RUB 9352",
        "RUB 9353",
        "RUB 9361",
        "RUB 9362",
        "RUB 9363",
        "RUB 9371",
        "RUB 9372",
        "RUB 9373",
        "RUB 9381",
        "RUB 9383",
        "RUB 9391",
        "RUB 9393",
        "RUB 9401",
        "RUB 9421",
        "RUB 9431",
        "RUB 9441",
        "RUB 9461",
        "RUB 9471",
        "RUB 9481",
        "RUB 9491",
        "RUB 9500",
        "RUB 9501",
        "RUB 9502",
        "RUB 9503",
        "RUB 9504",
        "RUB 9505",
        "RUB 95061",
        "RUB 95062",
        "RUB 95063",
        "RUB 9507",
        "RUB 95071",
        "RUB 95072",
        "RUB 95073",
        "RUB 95081",
        "RUB 95082",
        "RUB 95083",
        "RUB 9509",
        "RUB 95091",
        "RUB 95092",
        "RUB 95093",
        "RUB 9800",
        "RUB 9900",
        "RUB 9901",
        "RUB 9903",
        "RUB 9904",
        "RUB 9905",
        "RUB 9906",
        "FileBase"], fill_value=0)

def df_nbb_rubric_to_db9_postgres(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port):
    try:
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')
        cur.execute("SET search_path TO manual")
        df.to_sql(name='Tbl_RawData_NBB_rubricReport9', schema='manual', con=engine, if_exists='append', index=False)
        cur.execute('SELECT count(*) as records FROM "Tbl_RawData_NBB_rubricReport9"')
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_NBB_rubricReport9 has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (nbb rubric9): %s, args=%s: ", e, e.args)

#endregion Rubric


# todo: check eerste lijn op old_text en dan pas vervangen
def import_nbb_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port, nbbpath, searchstringannual, searchstringratio, searchstringrubric):
    helper.logger.info("Started NBB ANNUAL")
    helper.logger.info("-------------------------")
    # db_path = os.path.join(path, database)
    # list_files = fm.walk(os.path.join(path, dir_in), '.csv')
    list_files = fm.walk(nbbpath, '.csv')
    helper.logger.info(list_files)
    conn = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
    create_table_nbb_annual_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
    create_table_nbb_ratio_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
    create_table_nbb_rubric1_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
    # add_id_rubric1(postgres_database, postgres_user, postgres_password,
    #                 postgres_host, postgres_port)
    create_table_nbb_rubric2_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
    create_table_nbb_rubric3_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
    create_table_nbb_rubric4_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
    create_table_nbb_rubric5_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
    create_table_nbb_rubric6_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
    create_table_nbb_rubric7_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
    create_table_nbb_rubric8_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
    create_table_nbb_rubric9_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)

    count = 0
    for full_path in list_files:
        count += 1
        helper.logger.info("Behandel file: %s", full_path)
        file_without_ext = fm.get_filename_without_extension(full_path)
        table = determine_import_table(file_without_ext, searchstringannual, searchstringratio, searchstringrubric)
        if table == "Tbl_RawData_NBB_AnnualAccountReport":
            modify_csv_file(full_path) #overschrijf issue met "FRANK VOORTMANS CONSULTING
            df = pd.read_csv(full_path, sep=";", encoding="utf-8")  # checked with chardet
            df['FileBase'] = full_path
            # df['CompanyNumber'] = '0' + df['CompanyNumber'].astype(str)
            df.rename(columns={
                df.columns[12]: "PostalCodeDE1",}
                , inplace=True)
            # print("Df", count, len(df), full_path)
            frames = [df]
            df = pd.concat(frames)
            helper.logger.info("Import %s records in df from file %s", len(df), full_path)
            df_nbb_annual_to_db_postgres(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
            helper.logger.info("==============================================================================================")
        elif table == "Tbl_RawData_NBB_ratioReport":
            fm.convert_csv_to_excel(full_path)
            path = fm.get_path(full_path)
            old_file = fm.get_filename(full_path)
            new_file = fm.get_filename_without_extension(old_file) + ".xlsx"
            full_path = os.path.join(path, new_file)
            df = pd.read_excel(full_path)
            df['FileBase'] = full_path
            # df['CompanyNumber'] = '0' + df['CompanyNumber'].astype(str)
            frames = [df]
            df = pd.concat(frames)
            helper.logger.info("Import %s records in df from file %s", len(df), full_path)
            df_nbb_ratio_to_db_postgres(df, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
            os.remove(full_path)
        elif table == "Tbl_RawData_NBB_rubricReport":
            #importeer file(s) in df
            df = pd.read_csv(full_path, sep=";", encoding="ascii")  # checked with chardet
            df['FileBase'] = full_path
            # df['CompanyNumber'] = '0' + df['CompanyNumber'].astype(str)
            # print("Df", count, len(df), full_path)
            frames = [df]
            df = pd.concat(frames)
            helper.logger.info("Import %s records in df from file %s", len(df), full_path)
            # Enkel ARS1
            df1 = limit_rubric1(df)
            df_nbb_rubric_to_db1_postgres(df1, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
            helper.logger.info("==============================================================================================")
            # Enkel ARS2
            df2 = limit_rubric2(df)
            df_nbb_rubric_to_db2_postgres(df2, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
            helper.logger.info("==============================================================================================")
            # Enkel ARS3
            df3 = limit_rubric3(df)
            df_nbb_rubric_to_db3_postgres(df3, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
            helper.logger.info("==============================================================================================")
            # Enkel ARS4
            df4 = limit_rubric4(df)
            df_nbb_rubric_to_db4_postgres(df4, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
            helper.logger.info("==============================================================================================")
            # Enkel ARS5
            df5 = limit_rubric5(df)
            df_nbb_rubric_to_db5_postgres(df5, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
            helper.logger.info("==============================================================================================")
            # Enkel ARS6
            df6 = limit_rubric6(df)
            df_nbb_rubric_to_db6_postgres(df6, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
            helper.logger.info("==============================================================================================")
            # # Enkel ARS7
            df7 = limit_rubric7(df)
            df_nbb_rubric_to_db7_postgres(df7, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
            helper.logger.info("==============================================================================================")
            # # Enkel ARS8
            df8 = limit_rubric8(df)
            df_nbb_rubric_to_db8_postgres(df8, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
            helper.logger.info("==============================================================================================")
            # Enkel ARS9
            df9 = limit_rubric9(df)
            df_nbb_rubric_to_db9_postgres(df9, postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
            helper.logger.info("==============================================================================================")


def modify_csv_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    content = content.replace(';"FRANK VOORTMANS CONSULTING;', ';FRANK VOORTMANS CONSULTING;')

    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(content)





