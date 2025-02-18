import decorator
import helper
import adres_postgres as adp #postgres
import adres_export as ade
from old import ang as an, adres as ad
import ang_postgres as anp #postgres
import ang_beroepsverbod as anb #sqlite
import ang_beroepsverbod_postgres as anbp #postgres
import ang_export as ane
import dienstencentra as di #sqlite
import dienstencentra_postgres as dip #postgres
import kbo_actief as ka #sqlite
import kbo_stop as ks #sqlite
import nbb as nbb #sqlite
import nbb_postgres as nbbp #postgres
import reftab
import mail
import table as tb
import sqlscript as sql
import configparser
import sys

def display_menu_sqlite():
    print("*===================================================================*")
    print("*===-------------------------------------------------------------===*")
    print("*=============Import data for Mon(t)Fin SQLite======================*")
    print("*===-------------------------------------------------------------===*")
    print("*===================================================================*")
    print("1. Import adres")
    print("2. Import ang")
    print("3. Import ang-beroepsverbod")
    print("4. Import fod-dienstencentra")
    print("5. Import nbb")
    print("6. Import kbo-actief")
    print("7. Import kbo-stop")
    print("a. Give overview imported data SQLite")
    print("z. Exit")


def display_menu_postgres():
    print("*===================================================================*")
    print("*===-------------------------------------------------------------===*")
    print("*===========Import data for Mon(t)Fin PostgreSQL=====================*")
    print("*===-------------------------------------------------------------===*")
    print("*===================================================================*")
    print("1. Export adres (input for QGIS)")
    print("2. Import adres (output from QGIS)")
    print("3. Geocode automatically (Nominatim)")
    print("-----------------------------------------")
    print("4. Export data ang (for DRI)")
    print("5. Import ang (from DRI)")
    print("-----------------------------------------")
    print("6. Import ang-beroepsverbod (from AIK)")
    print("7. Import fod-dienstencentra")
    print("8. Import nbb")
    print("a. Get RefTab tables")
    print("b. Give overview imported data PostgreSQL")
    print("c. Execute sql script part1 (general)")
    print("d. Execute sql script part2 (specific FGP Limburg)")
    print("e. Send data to other FGP's")
    print("z. Exit")

def get_config_values():
    parser = configparser.ConfigParser()
    parser.read('MonFin_old.ini')
    db_path_ini = parser.get('Paths', 'dbpath')
    adrespath_ini = parser.get('Paths', 'adrespath')
    angpath_ini = parser.get('Paths', 'angpath')
    ang_beroepsverbodpath_ini = parser.get('Paths', 'ang_beroepsverbodpath')
    dienstencentrapath_ini = parser.get('Paths', 'dienstencentrapath')
    kbo_actiefpath_ini = parser.get('Paths', 'kbo_actiefpath')
    kbo_stoppath_ini = parser.get('Paths', 'kbo_stoppath')
    nbbpath_ini = parser.get('Paths', 'nbbpath')
    searchadres_ini = parser.get('Searchstring', 'adresstring')
    searchang_ini = parser.get('Searchstring', 'angstring')
    serachang_beroepsverbod_ini = parser.get('Searchstring', 'ang_beroepsverbodstring')
    searchdienstencentra_ini = parser.get('Searchstring', 'dienstencentrastring')
    searchkbo_actief_ini = parser.get('Searchstring', 'kbo_actiefstring')
    searchkbo_stop_ini = parser.get('Searchstring', 'kbo_stopstring')
    searchnbb_annual_ini = parser.get('Searchstring', 'nbb_annualstring')
    searchnbb_ratio_ini = parser.get('Searchstring', 'nbb_ratiostring')
    searchnbb_rubric_ini = parser.get('Searchstring', 'nbb_rubricstring')
    postgres_database = parser.get('postgresql', 'database')
    postgres_user = parser.get('postgresql', 'user')
    postgres_password = parser.get('postgresql', 'password')
    postgres_host = parser.get('postgresql', 'host')
    postgres_port = parser.get('postgresql', 'port')
    sqlfilepath1_ini = parser.get('sql', 'sqlscript1_filepath')
    sqlfilepath2_ini = parser.get('sql', 'sqlscript2_filepath')
    access_path = parser.get('reftab','accesspath')
    csv_path = parser.get('reftab', 'csvpath')
    tables = parser.get('reftab', 'tables').split(',')
    exportdata_path = parser.get('export', 'exportdatapath')
    exportsetup_path = parser.get('export', 'exportsetuppath')
    fgps = parser.get('fgps', 'list').split(',')
    ang_export_path = parser.get('Paths', 'ang_export_path')
    adres_export_path = parser.get('Paths', 'adres_export_path')
    recipients_execution_script1 = parser.get('execution', 'script1')
    recipients_execution_script2 = parser.get('execution', 'script2')

    return db_path_ini, adrespath_ini, angpath_ini, ang_beroepsverbodpath_ini, dienstencentrapath_ini, \
           kbo_actiefpath_ini, kbo_stoppath_ini, nbbpath_ini, searchadres_ini, searchang_ini, \
           serachang_beroepsverbod_ini, searchdienstencentra_ini, searchkbo_actief_ini, searchkbo_stop_ini, \
           searchnbb_annual_ini, searchnbb_ratio_ini, searchnbb_rubric_ini, postgres_database, postgres_user, \
           postgres_password, postgres_host, postgres_port, sqlfilepath1_ini, sqlfilepath2_ini, access_path, csv_path,\
           tables, exportsetup_path, exportdata_path, fgps, ang_export_path, adres_export_path, recipients_execution_script1, \
           recipients_execution_script2


@decorator.timer
def main():
    try:
        # helper.logger.info("Started processing files MontFin")
        # helper.logger.info("=====================================================================")
        db_path_ini, adrespath_ini, angpath_ini, ang_beroepsverbodpath_ini, dienstencentrapath_ini, kbo_actiefpath_ini,\
        kbo_stoppath_ini, nbbpath_ini, searchadres_ini, searchang_ini, serachang_beroepsverbod_ini, \
        searchdienstencentra_ini, searchkbo_actief_ini, searchkbo_stop_ini, searchnbb_annual_ini, \
        searchnbb_ratio_ini, searchnbb_rubric_ini, postgres_database, postgres_user, postgres_password, \
        postgres_host, postgres_port, sqlfilepath1_ini, sqlfilepath2_ini, access_path, csv_path, tables,\
        exportsetup_path, exportdata_path, fgps, ang_export_path, adres_export_path, recipients_execution_script1, \
        recipients_execution_script2 = get_config_values()

        print("Choose the database to load the data into:")
        print("1. SQLite")
        print("2. PostgreSQL")
        db_choice = input("Enter your choice (1/2): ")

        if db_choice == "1":
            db_type = "SQLite"
            display_menu_sqlite()
            selected_options = input("Enter options, seperated by commas: ").strip().split(",")
            for option in selected_options:
                if option == "1":
                    ad.import_adres(db_path_ini, adrespath_ini, searchadres_ini)
                elif option == "2":
                    an.import_ang15(db_path_ini, angpath_ini, searchang_ini)
                    an.import_ang01(db_path_ini, angpath_ini, searchang_ini)
                elif option == "3":
                    anb.import_ang_beroepsverbod(db_path_ini, ang_beroepsverbodpath_ini, serachang_beroepsverbod_ini)
                elif option == "4":
                    di.import_fod_dienst(db_path_ini, dienstencentrapath_ini, searchdienstencentra_ini)
                elif option == "5":
                    nbb.import_nbb(db_path_ini, nbbpath_ini, searchnbb_annual_ini, searchnbb_ratio_ini,
                                   searchnbb_rubric_ini)
                elif option == "6":
                    ka.import_kbo_actief(db_path_ini, kbo_actiefpath_ini, searchkbo_actief_ini)
                elif option == "7":
                    ks.import_kbo_stop(db_path_ini, kbo_stoppath_ini, searchkbo_stop_ini)
                elif option == "a":
                    tb.overview_tables(db_path_ini)
                elif option == "z":
                    sys.exit()

        elif db_choice == "2":
            db_type = "PostgreSQL"
            display_menu_postgres()
            selected_options = input("Enter options, seperated by commas: ").strip().split(",")
            for option in selected_options:
                if option == "1":
                    ade.export_ang(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port, adres_export_path)
                elif option == "2":
                    adp.import_adres_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port, adrespath_ini, searchadres_ini)
                elif option == "3":
                    print("Under construction ...")
                # --------------------------------------------------------------------------------------------------
                elif option == "4":
                    ane.export_ang(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port, ang_export_path)
                elif option == "5":
                    anp.import_ang_postgres15(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port, angpath_ini, searchang_ini)
                    anp.import_ang_postgres01(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port, angpath_ini, searchang_ini)
                # --------------------------------------------------------------------------------------------------
                elif option == "6":
                    anbp.import_ang_beroepsverbod_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port, ang_beroepsverbodpath_ini, serachang_beroepsverbod_ini)
                elif option == "7":
                    dip.import_fod_dienst_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port, dienstencentrapath_ini, searchdienstencentra_ini)
                elif option == "8":
                    nbbp.import_nbb_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port, nbbpath_ini, searchnbb_annual_ini, searchnbb_ratio_ini,
                                   searchnbb_rubric_ini)
                    display_menu_postgres()
                elif option == "a":
                    # reftab.drop_all_tables_in_schema(postgres_database, postgres_user, postgres_password,
                    # postgres_host, postgres_port, 'reftab')
                    reftab.drop_tables(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port, tables)
                    reftab.export_access_tables_to_csv(access_path, csv_path, tables)
                    # reftab.export_data_types_to_csv(access_path, csv_path, tables)
                    reftab.export_data_types_to_one_file(access_path, csv_path, tables)
                    # reftab.csv_to_postgres(postgres_database, postgres_user, postgres_password,
                    # postgres_host, postgres_port,csv_path)
                    reftab.create_tables_from_csv(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port,csv_path)
                    reftab.csvdata_in_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port,csv_path)
                elif option == "b":
                    tb.overview_tables_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
                    display_menu_postgres()
                elif option == "c":
                    helper.logger.info("Making connection sqlscript part1")
                    helper.logger.info("Start executing sqlscript part1")
                    sql.execute_sqlscript(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port, sqlfilepath1_ini, recipients_execution_script2)
                    # helper.logger.info("Finished executing sqlscript part1")
                elif option == "d":
                    helper.logger.info("Making connection sqlscript part2")
                    helper.logger.info("Start executing sqlscript part2")
                    sql.execute_sqlscript(postgres_database, postgres_user, postgres_password,
                                           postgres_host, postgres_port, sqlfilepath2_ini,recipients_execution_script2 )
                    tb.create_relationships(postgres_database, postgres_user, postgres_password,
                                           postgres_host, postgres_port)
                    # helper.logger.info("Finished executing sqlscript part2")
                elif option == "e":
                    # verzenden van mails naar andere fgp's op basis van ini file
                    # export setup
                    mail.export_setuptables(postgres_database, postgres_user, postgres_password,
                                           postgres_host, postgres_port, exportsetup_path)
                    # export data, kap in stukjes en verstuur samen met setup
                    mail.export_mail_datatables(postgres_database, postgres_user, postgres_password,
                                                postgres_host, postgres_port, exportdata_path, fgps,
                                                exportsetup_path)
                elif option == "z":
                    sys.exit()

        else:
            print("Invalid choice. Exiting program.")
            sys.exit()
    except Exception as e:
        helper.logger.error("Error while processing: " + str(e))


if __name__ == '__main__':
    print("*===================================================================*")
    print("*===-------------------------------------------------------------===*")
    print("*===Importeer de gegevens voor de monitoringstool vennootschappen===*")
    print("*===-------------------------------------------------------------===*")
    print("*===================================================================*")
    main()
    input("Press enter to exit.")

