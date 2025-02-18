import os
import filemanager as fm
import helper
import pandas as pd
import table as tb
import numpy as np
import re


def determine_import_table(file_without_ext, searchstring):
    try:
        match = re.search(searchstring, file_without_ext)
        # if fm.left(file_without_ext, 5) == "Basis": #gedesactiveerde 31/03/2023
        # if fm.left(file_without_ext, 7) == "LIM-ACT":
        if match:
            table = "Tbl_RawData_KBO_Actief"
            return table
        else:
            pass
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (kbo_actief getting tablename based on filename): %s, args=%s: ", e, e.args)


def df_kbo_actief_to_db(df, db_path):
    # print("To db", df)
    # print(df.dtypes)
    try:
        conn, cur = tb.create_connection(db_path)
        df.to_sql(name='Tbl_RawData_KBO_Actief', con=conn, if_exists='append', index=False)
        cur.execute("SELECT count(*) as records FROM Tbl_RawData_KBO_Actief")
        result = cur.fetchall()
        helper.logger.info("Tbl_RawData_KBO_Actief has %s records", result[0][0])
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (kbo_actief): %s, args=%s: ",e, e.args)


def create_table_kbo_actief(db_path):
    try:
        conn, cur = tb.create_connection(db_path)
        # 31/03/2023:  velden uit create gehaald:
        # 31/03/2023: datum velden datatype datetime64[ns] gegeven
        # 31/03/2023: alle objecten omgezet naar text
        # 31/03/2023: ondernemingsnummer functiehouder omgezet naar text
        # 13/02/2023: ALLE velden KBO Select voorzien
        cur.execute("""CREATE TABLE if not exists Tbl_RawData_KBO_Actief (
                    Ondernemingsnummer text,
                    Inschrijvingsdatum datetime64[ns],
                    Begindatum datetime64[ns],
                    [Type van ambtshalve doorhaling van de entiteit] float64,
                    [Begindatum van ambtshalve doorhaling van de entiteit] datetime64[ns],
                    Status text,
                    Type text,
                    [Naam (DE)] text,
                    [Naam (EN)] text,
                    [Naam (FR)] text,
                    [Naam (NL)] text,
                    [Naam (OTHER)] text,
                    [Commerciële naam (DE)] text,
                    [Commerciële naam (EN)] text,
                    [Commerciële naam (FR)] text,
                    [Commerciële naam (NL)] text,
                    [Commerciële naam (OTHER)] text,
                    [Afgekorte naam (DE)] text,
                    [Afgekorte naam (EN)] text,
                    [Afgekorte naam (FR)] text,
                    [Afgekorte naam (NL)] text,
                    [Afgekorte naam (OTHER)] text,
                    Kapitaal text, 
                    [Begindatum van de bankrekening] datetime64[ns],
                    IBAN text,
                    BIC text,
                    [Niet SEPA-rekeningnummer] text,
                    [Begindatum adres] datetime64[ns],
                    [Type adres] text,
                    Straatcode text,
                    [Straat (manueel ingegeven)] text,
                    [Straat (DE)] text,
                    [Straat (FR)] text,
                    [Straat (NL)] text,
                    Nummer text,
                    Bus text,
                    Postcode int64,
                    Gemeentecode int64,
                    [Gemeente (manueel ingegeven)] text,
                    [Gemeente (DE)] text,
                    [Gemeente (FR)] text,
                    [Gemeente (NL)] text,
                    Landencode int64,
                    [Land (manueel ingegeven)] text,
                    [Land (DE)] text,
                    [Land (FR)] text,
                    [Land (NL)] text,
                    [Staat code] float64,
                    [Extra adres] float64,
                    [Adres doorhaling] text,
                    Telefoonnummer text,
                    Fax text,
                    [E-mail] text,
                    Website text,
                    Rechtsvormcode int64,
                    [Rechtsvorm (DE)] text,
                    [Rechtsvorm (FR)] text,
                    [Rechtsvorm (NL)] text,
                    [Begindatum rechtsvorm] datetime64[ns],
                    Rechtstoestandcode int64,
                    [Rechtstoestand (DE)] text,
                    [Rechtstoestand (FR)] text,
                    [Rechtstoestand (NL)] text,
                    [Begindatum rechtstoestand] datetime64[ns],
                    Gebeurteniscode int64,
                    [Datum gebeurtenis] datetime64[ns],
                    Functiecode float64,
                    [Functie (DE)] text,
                    [Functie (FR)] text,
                    [Functie (NL)] text,
                    [Functie type] text,
                    [Begindatum functie] datetime64[ns],
                    [Vrijstelling] text, 
                    [Persoonsnummer functiehouder] float64,
                    [Naam functiehouder] text,
                    [Voornaam functiehouder] text,
                    [Ondernemingsnummer functiehouder] text,
                    [ADMIN/initiator activiteit] object,
                    [Versie Nacebel] int64,
                    Activiteitencode int64,
                    [Activiteit (DE)] text,
                    [Activiteit (FR)] text,
                    [Activiteit (NL)] text,
                    [Type activiteit] text,
                    [Begindatum activiteit] datetime64[ns],
                    [Toelatingscode of hoedanigheidscode] int64,
                    [Hoedanigheid of toelating] text,
                    [Hoedanigheid of toelating (DE)] text,
                    [Hoedanigheid of toelating (FR)] text,
                    [Hoedanigheid of toelating (NL)] text,
                    [Duur hoedanigheid of toelating (in jaren)] text,
                    [Begindatum hoedanigheid of toelating] datetime64[ns],
                    [Behandelaar toelatingsaanvraag] text,
                    [Begindatum toelatingsaanvraag] datetime64[ns],
                    [Code fase toelatingsaanvraag] text,
                    [Fase toelatingsaanvraag (DE)] text,
                    [Fase toelatingsaanvraag (FR)] text,
                    [Fase toelatingsaanvraag (NL)] text,
                    [Code reden stopzetting toelatingsaanvraag] text,
                    [Reden stopzetting toelatingsaanvraag (DE)] text,
                    [Reden stopzetting toelatingsaanvraag (FR)] text,
                    [Reden stopzetting toelatingsaanvraag (NL)] text,
                    [Begindatum fase toelatingsaanvraag] datetime64[ns],
                    FileBase object);""")
        cur.execute("""DELETE FROM Tbl_RawData_KBO_Actief;""")  # truncate
        helper.logger.info("Table Tbl_RawData_KBO_Actief has been created")
        conn.commit()
        conn.close()
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (kbo_actief): %s, args=%s: ",e, e.args)


def import_kbo_actief(db_path, kbo_actiefpath, searchstring):
    try:
        helper.logger.info("Started KBO ACTIEF")
        helper.logger.info("-------------------------")
        # db_path = os.path.join(path, database)
        # list_files = fm.walk(os.path.join(path, dir_in), '.csv')
        list_files = fm.walk(kbo_actiefpath, '.csv')
        helper.logger.info(list_files)
        conn = tb.create_connection(db_path)
        create_table_kbo_actief(db_path)
        count = 0
        for full_path in list_files:
            count +=1
            helper.logger.info("Behandel file: %s", full_path)
            file_without_ext = fm.get_filename_without_extension(full_path)
            table = determine_import_table(file_without_ext, searchstring)
            if table == "Tbl_RawData_KBO_Actief":
                df = pd.read_csv(full_path, encoding="ISO-8859-1") # checked with chardet
                df['FileBase'] = full_path
                # print("Before", df)
                #https://stackoverflow.com/questions/29051555/where-does-my-leading-zeroes-go
                df['Ondernemingsnummer'] = '0' + df['Ondernemingsnummer'].astype(str)
                #Error in veld ondernemingsnummer functiehouder wordt als *.0 doorgegeven.
                df['Ondernemingsnummer functiehouder'] = df['Ondernemingsnummer functiehouder'].astype(str)
                df['Ondernemingsnummer functiehouder'] = "0" + df['Ondernemingsnummer functiehouder'].str[:-2]
                df = df.replace(r'0n', np.nan, regex=True)
                # print("After", df)
                # print("Df", count, len(df), full_path)
                frames =[df]
                df = pd.concat(frames)
                helper.logger.info("Import %s records in df from file %s", len(df), full_path)
                df_kbo_actief_to_db(df, db_path)
                helper.logger.info("==============================================================================================")
    except UnicodeDecodeError as e:
        helper.logger.error("Error: Encoding-error while reading file (kbo-actief): %s, args=%s: ",e, e.args)
    except Exception as e:
        helper.logger.error("Error: Oeps, something went wrong (kbo-actief): %s, args=%s", e, e.args)
