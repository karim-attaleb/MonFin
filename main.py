import decorator
import helper
import ang_export as ane
from old import adres_export as ade
import ang_postgres as anp
import adres_postgres as adp
import ang_beroepsverbod_postgres as anbp
import dienstencentra_postgres as dip
import nbb_postgres as nbbp
import reftab
import mail
import table as tb
import sqlscript as sql
import configparser
import sys
import time
import os
import export_files as ex
from reftable_postgres import create_table_historiek_nis_postgres


def show_loading_message():
    print("Loading... Please wait.")


def show_progress_bar(total, progress):
    bar_length = 40
    filled_length = int(bar_length * progress / total)
    bar = '#' * filled_length + '-' * (bar_length - filled_length)
    percent = progress / total * 100
    print(f"Progress: [{bar}] {percent:.2f}%")


def initialize_program():
    # Simulating a long initialization process
    total_tasks = 10
    for i in range(total_tasks):
        # Perform initialization tasks
        time.sleep(2)  # Simulate task execution time
        show_progress_bar(total_tasks, i + 1)


def display_menu_postgres():
    # print("*===================================================================*")
    # print("*===-------------------------------------------------------------===*")
    # print("*===========Import data for Mon(t)Fin PostgreSQL=====================*")
    # print("*===-------------------------------------------------------------===*")
    print(" *===================================================================*")
    print(" a. Get RefTab tables")
    print(" b. Execute sql script part1 (basetables)")
    print(" -------------------------------------------------------")
    print(" c. Export adres manually (input for QGIS)")
    print(" d. Import adres manually (output from QGIS)")
    print(" e. Geocode automatically (Nominatim)")
    print(" -------------------------------------------------------")
    print(" f. Export data ang (for DRI)")
    print(" g. Import ang (from DRI)")
    print(" -------------------------------------------------------")
    print(" h. Import ang-beroepsverbod (from AIK)")
    print(" -------------------------------------------------------")
    print(" i. Import fod-dienstencentra manually [j preferred]")
    print(" j. Import fod-dienstencentra automatically")
    print(" -------------------------------------------------------")
    print(" k. Import nbb manually")
    print(" l. Get data nbb automatically")
    print(" -------------------------------------------------------")
    print(" m. Execute sql script part2 (scoring - general)")
    print(" n. Execute sql script part3 (scoring - specific FGP Limburg)")
    print(" -------------------------------------------------------")
    print(" o. Export tables schema Final FGP Limburg (input report)")
    print(" -------------------------------------------------------")
    print(" x. Create zipfiles for transfering data to other FGP's")
    print(" -------------------------------------------------------")
    print(" y. Give overview imported data PostgreSQL")
    print(" -------------------------------------------------------")
    print(" z. Exit")
    print(" *===================================================================*")


def get_config_values():
    parser = configparser.ConfigParser()
    parser.read('MonFin.ini')
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
    sqlfilepath3_ini = parser.get('sql', 'sqlscript3_filepath')
    access_path = parser.get('reftab','accesspath')
    csv_path_reftab = parser.get('reftab', 'csvpath_reftab')
    tables = parser.get('reftab', 'tables').split(',')
    exportdata_path = parser.get('export', 'exportdatapath')
    exportsetup_path = parser.get('export', 'exportsetuppath')
    fgps = parser.get('fgps', 'list').split(',')
    ang_export_path = parser.get('Paths', 'ang_export_path')
    adres_export_path = parser.get('Paths', 'adres_export_path')
    export_lim_path = parser.get('export_final_fgp_lim', 'exportlimpath')
    # recipients_execution_script1: str = parser.get('execution', 'script1')
    # recipients_execution_script2: str = parser.get('execution', 'script2')

    return db_path_ini, adrespath_ini, angpath_ini, ang_beroepsverbodpath_ini, dienstencentrapath_ini, \
           kbo_actiefpath_ini, kbo_stoppath_ini, nbbpath_ini, searchadres_ini, searchang_ini, \
           serachang_beroepsverbod_ini, searchdienstencentra_ini, searchkbo_actief_ini, searchkbo_stop_ini, \
           searchnbb_annual_ini, searchnbb_ratio_ini, searchnbb_rubric_ini, postgres_database, postgres_user, \
           postgres_password, postgres_host, postgres_port, sqlfilepath1_ini, sqlfilepath2_ini, sqlfilepath3_ini, access_path, csv_path_reftab,\
           tables, exportsetup_path, exportdata_path, fgps, ang_export_path, adres_export_path, export_lim_path
            # ,recipients_execution_script1, # recipients_execution_script2


# @decorator.timer TVDE
def main():
    try:
        # helper.logger.info("Started processing files MontFin")
        # helper.logger.info("=====================================================================")
        db_path_ini, adrespath_ini, angpath_ini, ang_beroepsverbodpath_ini, dienstencentrapath_ini, kbo_actiefpath_ini,\
        kbo_stoppath_ini, nbbpath_ini, searchadres_ini, searchang_ini, serachang_beroepsverbod_ini, \
        searchdienstencentra_ini, searchkbo_actief_ini, searchkbo_stop_ini, searchnbb_annual_ini, \
        searchnbb_ratio_ini, searchnbb_rubric_ini, postgres_database, postgres_user, postgres_password, \
        postgres_host, postgres_port, sqlfilepath1_ini, sqlfilepath2_ini, sqlfilepath3_ini, access_path, csv_path_reftab, tables,\
        exportsetup_path, exportdata_path, fgps, ang_export_path, adres_export_path, export_lim_path = get_config_values()
        # recipients_execution_script1, recipients_execution_script2

        while True:
            # @decorator.timer TVDE
            def execute_option():
                display_menu_postgres()
                selected_options = input("\n Enter options, seperated by commas: ").strip().split(",")
                for option in selected_options:
                    if option == "c":
                        ade.export_ang(postgres_database, postgres_user, postgres_password,
                        postgres_host, postgres_port, adres_export_path)
                    elif option == "d":
                        adp.import_adres_postgres(postgres_database, postgres_user, postgres_password,
                        postgres_host, postgres_port, adrespath_ini, searchadres_ini)
                    elif option == "e":
                        print("Under construction ...")
                    # --------------------------------------------------------------------------------------------------
                    elif option == "f":
                        ane.export_ang(postgres_database, postgres_user, postgres_password,
                        postgres_host, postgres_port, ang_export_path)
                    elif option == "g":
                        anp.import_ang_postgres15(postgres_database, postgres_user, postgres_password,
                        postgres_host, postgres_port, angpath_ini, searchang_ini)
                        anp.import_ang_postgres01(postgres_database, postgres_user, postgres_password,
                        postgres_host, postgres_port, angpath_ini, searchang_ini)
                    # --------------------------------------------------------------------------------------------------
                    elif option == "h":
                        anbp.import_ang_beroepsverbod_postgres(postgres_database, postgres_user, postgres_password,
                        postgres_host, postgres_port, ang_beroepsverbodpath_ini, serachang_beroepsverbod_ini)
                    elif option == "i":
                        dip.import_fod_dienst_postgres_manual(postgres_database, postgres_user, postgres_password,
                        postgres_host, postgres_port, dienstencentrapath_ini, searchdienstencentra_ini)
                    elif option == "j":
                        excel_filename_prefix = searchdienstencentra_ini
                        folder = dienstencentrapath_ini
                        dip.delete_csv_xlsx_files_in_folder(folder)
                        dip.get_csv_from_website(folder)

                        for filename in os.listdir(folder):
                            if filename.startswith('downloaded') and filename.endswith('.csv'):
                                csv_path = os.path.join(folder, filename)
                                output_excel_filename = f"{excel_filename_prefix}_{filename[:-4]}.xlsx"
                                excel_path = os.path.join(folder, output_excel_filename)

                                dip.convert_csv_to_excel(csv_path, excel_path)
                                helper.logger.info(f"Converted {filename} to Excel: {output_excel_filename}")

                        dip.delete_csv_files_in_folder(folder)
                        dip.import_fod_dienst_postgres_auto(postgres_database, postgres_user, postgres_password,
                                                       postgres_host, postgres_port, dienstencentrapath_ini,
                                                       searchdienstencentra_ini)
                    elif option == "k":
                        nbbp.import_nbb_postgres(postgres_database, postgres_user, postgres_password,
                        postgres_host, postgres_port, nbbpath_ini, searchnbb_annual_ini, searchnbb_ratio_ini,
                                       searchnbb_rubric_ini)
                    elif option == "l":
                        print("Under construction ...")
                    elif option == "a":
                        #reftab.drop_tables(postgres_database, postgres_user, postgres_password,
                        #postgres_host, postgres_port, tables)
                        #reftab.export_access_tables_to_csv(access_path, csv_path_reftab, tables)
                        #reftab.export_data_types_to_csv(access_path, csv_path_reftab, tables)
                        #reftab.export_data_types_to_one_file(access_path, csv_path_reftab, tables)
                        #reftab.create_tables_from_csv(postgres_database, postgres_user, postgres_password,
                        #postgres_host, postgres_port,csv_path_reftab) #new
                        #reftab.import_csv_to_postgres(postgres_database, postgres_user, postgres_password,
                        #postgres_host, postgres_port,csv_path_reftab)
                        create_table_historiek_nis_postgres(postgres_database, postgres_user, postgres_password, postgres_host, postgres_port)

                        print("TODO: directly get from KISS DB")
                        print("TODO: sh mdb2postgresql.sh /media/sf_private/montfin/Data/MontFin/RefTab/RefTabAllMontFin.accdb RefTabAllMontFin.sql 'copy_reftab_rMun copy_reftab_rGar copy_reftab_rAfe copy_reftab_rPol copy_reftab_rPo2' reftab")
                        print(
                            "TODO: psql -U postgres -d kbo -h 192.168.70.5 -p 5433 -f RefTabAllMontFin.sql")
                        print(
                            "TODO: sh mdb2postgresql.sh /media/sf_private/montfin/Data/MontFin/RefTab/ConversiePostNis/ConversiePostNis.accdb ConversiePostNis.sql 'ConversiePostNis' reftab")
                        print(
                            "TODO: psql -U postgres -d kbo -h 192.168.70.5 -p 5433 -f ConversiePostNis.sql")
                        print(
                            "TODO: psql -U postgres -d kbo -h 192.168.70.5 -p 5433 -c \"\copy reftab.historieknis FROM '/media/sf_private/montfin/Data/MontFin/RefTab/historiek_nis.csv' delimiter ';' csv\"")
                        # pass
                    elif option == "b":
                        helper.logger.info("Making connection sqlscript part1")
                        helper.logger.info("Start executing sqlscript part1")
                        # sql.execute_sqlscript(postgres_database, postgres_user, postgres_password,
                        # postgres_host, postgres_port, sqlfilepath1_ini, recipients_execution_script1, "0")
                        sql.execute_sqlscript(postgres_database, postgres_user, postgres_password,
                        postgres_host, postgres_port, sqlfilepath1_ini, "1")
                        helper.logger.info("Finished executing sqlscript part1")
                    elif option == "m":
                        helper.logger.info("Making connection sqlscript part2")
                        helper.logger.info("Start executing sqlscript part2")
                        # sql.execute_sqlscript(postgres_database, postgres_user, postgres_password,
                        # postgres_host, postgres_port, sqlfilepath1_ini, recipients_execution_script1, "1")
                        sql.execute_sqlscript(postgres_database, postgres_user, postgres_password,
                        postgres_host, postgres_port, sqlfilepath2_ini,  "2")
                        helper.logger.info("Finished executing sqlscript part2")
                    elif option == "n":
                        helper.logger.info("Making connection sqlscript part3")
                        helper.logger.info("Start executing sqlscript part3")
                        # sql.execute_sqlscript(postgres_database, postgres_user, postgres_password,
                        #                        postgres_host, postgres_port, sqlfilepath2_ini,recipients_execution_script2,
                        #                       "2" )
                        sql.execute_sqlscript(postgres_database, postgres_user, postgres_password,
                                               postgres_host, postgres_port, sqlfilepath3_ini,"3" )
                        tb.create_relationships(postgres_database, postgres_user, postgres_password,
                                               postgres_host, postgres_port)
                        helper.logger.info("Finished executing sqlscript part3")
                    elif option == "o":
                        helper.logger.info("Preparing final files FGP Limburg for export")
                        ex.export_fgplimtables(postgres_database, postgres_user, postgres_password,
                                               postgres_host, postgres_port, export_lim_path)
                        # ex.export_special_table(postgres_database, postgres_user, postgres_password,
                        #                        postgres_host, postgres_port, export_lim_path)
                        helper.logger.info("Finished final files FGP Limburg files for export")
                    elif option == "x":
                        # verzenden van mails naar andere fgp's op basis van ini file
                        # export setup
                        helper.logger.info("Preparing setup files for export")
                        mail.export_setuptables(postgres_database, postgres_user, postgres_password,
                                               postgres_host, postgres_port, exportsetup_path)
                        helper.logger.info("Finished preparing setup files for export")
                        # export data, kap in stukjes en verstuur samen met setup
                        helper.logger.info("Preparing data files for export")
                        mail.export_mail_datatables(postgres_database, postgres_user, postgres_password,
                                                    postgres_host, postgres_port, exportdata_path, fgps,
                                                    exportsetup_path)
                        helper.logger.info("Finished preparing data files for export")
                    elif option == "y":
                        tb.overview_tables_postgres(postgres_database, postgres_user, postgres_password,
                                                    postgres_host, postgres_port)
                    elif option == "z":
                        sys.exit()

            execute_option()
    except Exception as e:
        helper.logger.error("Error while processing: " + str(e))


if __name__ == '__main__':
    #show_loading_message() TVDE
    #initialize_program() # TVDE wat is hier de bedoeling van?
    #print("Initialization complete. Program ready.")
    print("")
    print(" *===================================================================*")
    print(" *===-------------------------------------------------------------===*")
    print(" *===-----Process data for monitoringtool enterprises (MonFin)----===*")
    print(" *===-------------------------------------------------------------===*")
    print(" *===================================================================*")
    main()
    input(" Press enter to exit.")

