import os
import filemanager as fm
import helper


# def determine_import_table(file_without_ext):
#     print(file_without_ext)
#     # Adres
#     if fm.left(file_without_ext, 4) == "xxx":
#         table = "Tbl_RawData_xxx_Referentiegegevens"
#     print(table)
#     return table
#
#
# def import_referentiegegevens(path, database, dir_in):
#     list_files = fm.walk(os.path.join(path, dir_in), '.xlsx')
#     helper.logger.info(list_files)
#     conn = fm.create_connection(database)
#     for full_path in list_files:
#         helper.logger.info("Behandel file: %s", full_path)
#         file_without_ext = fm.get_filename_without_extension(full_path)
#         table = fm.determine_import_table(file_without_ext)