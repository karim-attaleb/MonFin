import decorator
import helper
from old import ang as an, adres as ad
import ang_beroepsverbod as anb
import dienstencentra as di
import kbo_actief as ka
import kbo_stop as ks
import nbb as nbb
import table as tb
import configparser
# ??

@decorator.timer
def main():
    try:
        helper.logger.info("Started processing files MontFin")
        helper.logger.info("=====================================================================")
        parser = configparser.ConfigParser()
        parser.read('MonFin_old.ini')
        # DIR_IN_REF_ini = parser.get('Folders', 'DIR_IN_REF')
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

        ad.import_adres(db_path_ini, adrespath_ini, searchadres_ini)
        an.import_ang(db_path_ini, angpath_ini, searchang_ini) # create table if necessary, truncate, load
        anb.import_ang_beroepsverbod(db_path_ini, ang_beroepsverbodpath_ini, serachang_beroepsverbod_ini) # create table if necessary, truncate, load
        di.import_fod_dienst(db_path_ini, dienstencentrapath_ini,searchdienstencentra_ini) # create table if necessary (change columheading), truncate, load
        ka.import_kbo_actief(db_path_ini,kbo_actiefpath_ini,searchkbo_actief_ini) # create table if neceassary, truncate, load
        ks.import_kbo_stop(db_path_ini, kbo_stoppath_ini,searchkbo_stop_ini) # create table if neceassary, truncate, load
        nbb.import_nbb(db_path_ini, nbbpath_ini, searchnbb_annual_ini, searchnbb_ratio_ini, searchnbb_rubric_ini) #create table if necessary, truncate, load
        # ref.import_referentiegegevens(filepath_ini, database_ini, DIR_IN_REF_ini)
        helper.logger.info("Stopped processing files MontFin")
        tb.overview_tables(db_path_ini)
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (main): %s, args=%s: ", e, e.args)
    finally:
        helper.logger.info("Ending the program")


if __name__ == '__main__':
    print("*===================================================================*")
    print("*===-------------------------------------------------------------===*")
    print("*===Importeer de gegevens voor de monitoringstool vennootschappen===*")
    print("*===-------------------------------------------------------------===*")
    print("*===================================================================*")
    input("Press enter to start.")
    main()
    input("Press enter to exit.")

