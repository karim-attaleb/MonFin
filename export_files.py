import helper
import table as tb
import pandas as pd
import os
import csv


# def export_special_table(postgres_database, postgres_user, postgres_password, postgres_host, postgres_port, export_lim_path):
#     try:
#         helper.logger.info("Start exporting final files FGP Limburg")
#         helper.logger.info("------------------------------------")
#         conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password, postgres_host, postgres_port)
#         table_name = 'Tbl_LaatsteNeerleggingInfo_Limburg'
#         schema = 'final'
#         df = pd.read_sql(f'SELECT * FROM "{schema}"."{table_name}"', conn)
#
#         # Define data types for the columns
#         df = df.astype({
#             'CompanyNumber': str,
#             'CompanyNameNL': str,
#             'JuridicalFormCode': int,
#             'JuridicalSituationCode': str,
#             'JuridicalSituationDate': 'datetime64',
#             'AccountingYear': int,
#             'StartDateAccounting': 'datetime64',
#             'EndDateAccounting': 'datetime64',
#             'NbMonthAccountingYear': int,
#             'DateGeneralAssembly': 'datetime64',
#             'DateDeposit': 'datetime64',
#             'RUB 10/15': float,
#             'RUB 14': float,
#             'RUB 29/58': float,
#             'RUB 41': float,
#             'RUB 42/48': float,
#             'RUB 47/48': float,
#             'RUB 9900': float,
#             'EigenVermogen': str,
#             'Liquiditeit': float,
#             'CategorieLiquiditeit': str
#         })
#
#         num_rows = len(df)
#         print(f'Number of rows: {num_rows}')
#
#         csv_file_path = os.path.join(export_lim_path, f'{table_name}.csv')
#
#         if os.path.exists(csv_file_path):
#             os.remove(csv_file_path)
#
#         df.to_csv(csv_file_path, index=False, sep=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
#
#         helper.logger.info(f'Exported {table_name} to {csv_file_path}')
#
#     except Exception as e:
#         helper.logger.error(f"Error while processing (export {table_name}): {str(e)}")


def export_fgplimtables(postgres_database, postgres_user, postgres_password, postgres_host, postgres_port, export_lim_path):
    try:
        helper.logger.info("Start exporting final files FGP Limburg")
        helper.logger.info("------------------------------------")
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password, postgres_host, postgres_port)

        tables_to_export = [
            'Tbl_CheckIndicatoren_Limburg',
            'Tbl_ControleNeerlegging_Limburg',
            'Tbl_Final_Limburg',
            'Tbl_GemiddeldeScoreAdres_Limburg',
            'Tbl_GemiddeldeScoreBestuurder_Limburg',
            'Tbl_OutputGeocode_Limburg',
            'Tbl_RankingAdressen_Limburg',
            'Tbl_RankingBestuurders_Limburg',
            'Tbl_RankingVennootschappen_Limburg',
            'Tbl_LaatsteNeerleggingInfo_Limburg'
        ]

        for table_name in tables_to_export:
            schema = 'final'
            df = pd.read_sql(f'SELECT * FROM "{schema}"."{table_name}"', conn)
            num_rows = len(df)
            print(f'Number of rows: {num_rows}')

            csv_file_path = os.path.join(export_lim_path, f'{table_name}.csv')

            if os.path.exists(csv_file_path):
                os.remove(csv_file_path)

            df.to_csv(csv_file_path, index=False, sep=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)

            helper.logger.info(f'Exported {table_name} to {csv_file_path}')

        # # Export special table
        # export_special_table(conn, export_lim_path)

        helper.logger.info("Stop exporting final files FGP Limburg")

    except Exception as e:
        helper.logger.error("Error while processing (export fgp lim tables): " + str(e))
    finally:
        conn.close()



