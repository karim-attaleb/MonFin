import pyodbc
import table as tb
import helper
import csv
from psycopg2 import sql
import os


def export_access_tables_to_csv(access_path, csv_path, table_list):
    try:
        helper.logger.info("Started RefTab export to csv")
        helper.logger.info("-------------------------")
        # connect to Access database
        access_conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + access_path + ';')
        access_cursor = access_conn.cursor()

        # Loop through each table in the list
        for table_name in table_list:
            # Get column information for the current table in Access
            access_cursor.execute(f"SELECT * FROM {table_name} WHERE 1=0")  # execute a query that returns no rows
            access_columns = access_cursor.description
            # access_column_names = [column[0] for column in access_columns]
            access_column_names = [column[0].lower() for column in access_columns]

            filename = table_name.lower()+".csv"
            # Export data from Access to CSV file
            access_query = f"SELECT * FROM {table_name}"

            with open(os.path.join(csv_path, filename.strip()), 'w+', newline='') as csv_file:
                csv_writer = csv.writer(csv_file, delimiter=',')
                csv_writer.writerow(access_column_names)  # write header row
                access_cursor.execute(access_query)
                for row in access_cursor.fetchall():
                    csv_writer.writerow(row)

        # close connection
        access_conn.close()
        helper.logger.info("RefTab is exported to csv")
        helper.logger.info(
            "==============================================================================================")
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (reftab export to csv): %s, args=%s: ", e, e.args)

def export_data_types_to_csv(access_path, csv_path, table_list):
    helper.logger.info("Started RefTab export to csv")
    helper.logger.info("-------------------------")
    # connect to Access database
    access_conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + access_path + ';')
    # access_cursor = access_conn.cursor()

    for table_name in table_list:
        filename = table_name.lower() + "_datatype.csv"
        access_cursor = access_conn.cursor()
        access_cursor.execute(f"SELECT * FROM {table_name}")
        column_names = [column[0] for column in access_cursor.description]
        column_types = [column[1] for column in access_cursor.description]
        with open(os.path.join(csv_path, filename.strip()), "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Column Name", "Data Type"])
            for i in range(len(column_names)):
                writer.writerow([column_names[i], column_types[i]])
        access_cursor.close()
    access_conn.close()

def export_data_types_to_one_file(access_path, csv_path, table_list):
    helper.logger.info("Started RefTab export to one file csv")
    helper.logger.info("-------------------------")
    with open(os.path.join(csv_path, "reftab_datatypes.csv"), "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["table_name", "column_data_types"])

        for table_name in table_list:
            access_conn = pyodbc.connect(
                r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + access_path + ';')
            access_cursor = access_conn.cursor()
            access_cursor.execute(f"SELECT * FROM {table_name}")
            column_names = [column[0] for column in access_cursor.description]
            column_types = [f"{column[0]}:{column[1]}" for column in access_cursor.description]
            access_cursor.close()
            access_conn.close()

            writer.writerow([table_name] + column_types)

def create_tables_from_csv(postgres_database, postgres_user, postgres_password,
                                                    postgres_host, postgres_port, csv_path):
    try:
        helper.logger.info("Started create table in in postgres")
        helper.logger.info("-------------------------")

        # connect to PostgreSQL database
        pg_conn, pg_cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                                                        postgres_host, postgres_port)

        with open(os.path.join(csv_path, "reftab_datatypes.csv"), "r") as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # skip header row
            for row in reader:
                table_name, column_data = row[0], row[1:]
                column_data_types = [column.strip() for column in column_data]
                # Build the SQL CREATE TABLE statement
                sql = f'CREATE TABLE reftab.{table_name} ('

                for column in column_data_types:
                    column_name, data_type_str = column.split(":")
                    # Convert the data type from MsAccess to Postgres
                    data_type = ""
                    if data_type_str == "<class 'str'>":
                        data_type = "VARCHAR"
                    elif data_type_str == "<class 'int'>":
                        data_type = "INTEGER"
                    elif data_type_str == "<class 'float'>":
                        data_type = "NUMERIC"
                    elif data_type_str == "<class 'bool'>":
                        data_type = "BOOLEAN"
                    elif data_type_str == "<class 'datetime.datetime'>":
                        data_type = "TIMESTAMP"
                    # Add the column to the CREATE TABLE statement
                    sql += f'"{column_name.strip()}" {data_type}, '
                sql = sql[:-2] + ")"  # Add closing parenthesis to complete the statement
                pg_cur.execute(sql) # Execute the CREATE TABLE statement

            pg_conn.commit()
            pg_cur.close()
            pg_conn.close()
        helper.logger.info("Reftab tables are created")
        helper.logger.info(
            "==============================================================================================")
    except Exception as e:
        helper.logger.info("Error: Oeps, something went wrong (reftab create table in postgres): %s, args=%s: ", e, e.args)

def import_csv_to_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port, csv_path):
    try:
        pg_conn, pg_cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                                                        postgres_host, postgres_port)

        for file_name in os.listdir(csv_path):
            if (file_name.startswith("conversiepostnis" ) or file_name.startswith("copy_reftab")) and "datatype" not in file_name:
                table_name = file_name.split(".csv")[0]
                table_name = table_name.lower()

                with open(os.path.join(csv_path, file_name), 'r', encoding="cp1252") as csv_file:
                    # pg_cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", csv_file)
                    pg_cur.copy_expert(f"COPY reftab.{table_name} FROM STDIN WITH CSV HEADER", csv_file)

        pg_conn.commit()
        pg_cur.close()
        pg_conn.close()
        print("Data imported successfully.")
    except Exception as e:
        print(f"Error: {e}")




def drop_tables(postgres_database, postgres_user, postgres_password,
                postgres_host, postgres_port, tables_to_drop):
    helper.logger.info("Started dropping tables")
    helper.logger.info("-----------------------")

    pg_conn, pg_cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                                                    postgres_host, postgres_port)

    for table in tables_to_drop:
        table_name = table.lower()
        pg_cur.execute(f"DROP TABLE IF EXISTS reftab.{table_name} CASCADE")
    pg_conn.commit()
    pg_cur.close()
    pg_conn.close()

    # check if all tables were dropped
    pg_conn, pg_cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                                                    postgres_host, postgres_port)
    remaining_tables = []
    for table in tables_to_drop:
        pg_cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'reftab' AND table_name = '{table_name}');")
        result = pg_cur.fetchone()
        if result[0]:
            remaining_tables.append(table_name)
    if len(remaining_tables) > 0:
        helper.logger.info(f"ERROR: Failed to drop {len(remaining_tables)} tables:")
        for table_name in remaining_tables:
            print(table_name)
    else:
        helper.logger.info("RefTab tables were successfully dropped.")
    helper.logger.info("==============================================================================================")
    pg_cur.close()
    pg_conn.close()


def drop_all_tables_in_schema(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port, schema_name):
    helper.logger.info("Started dropping tables schema RefTab")
    helper.logger.info("-------------------------")

    pg_conn, pg_cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                                                    postgres_host, postgres_port)

    pg_cur.execute(f"SELECT tablename FROM pg_tables WHERE schemaname = '{schema_name}'")
    tables = pg_cur.fetchall()
    print(f"Found {len(tables)} tables in schema {schema_name}:")
    for table in tables:
        print(table[0])
        # pg_cur.execute(f"DROP TABLE IF EXISTS {schema_name}.{table[0]} CASCADE")
        pg_cur.execute(f"DROP TABLE IF EXISTS {schema_name}.{table[0]}")
    pg_conn.commit()
    pg_cur.close()
    pg_conn.close()

    # check if all tables were dropped
    pg_conn, pg_cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                                                    postgres_host, postgres_port)
    pg_cur.execute(f"SELECT tablename FROM pg_tables WHERE schemaname = '{schema_name}'")
    remaining_tables = pg_cur.fetchall()
    if len(remaining_tables) > 0:
        helper.logger.info(f"ERROR: Failed to drop {len(remaining_tables)} tables in schema {schema_name}:")
        for table in remaining_tables:
            print(table[0])
    else:
        helper.logger.info(f"All tables in schema {schema_name} were successfully dropped.")
        helper.logger.info("==============================================================================================")
    pg_cur.close()
    pg_conn.close()


# def csvdata_in_postgres(postgres_database, postgres_user, postgres_password,
#                     postgres_host, postgres_port, csv_path):
#     helper.logger.info("Started RefTab import in postgres")
#     helper.logger.info("-------------------------")
#
#     pg_conn, pg_cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
#                                                     postgres_host, postgres_port)
#     try:
#         # loop through files in csv directory
#         for file_name in os.listdir(csv_path):
#             # if file_name.endswith(".csv") and file_name != "reftab_datatypes.csv":
#             if file_name.endswith(".csv") and file_name != "_datatype.csv":
#                 # get table name from file name
#                 table_name = file_name[:-4]  # remove ".csv" extension
#                 table_name = table_name.lower()  # convert to lowercase
#
#                 # import data from csv file
#                 with open(os.path.join(csv_path, file_name), 'r') as csv_file:
#                     csv_reader = csv.reader(csv_file, delimiter=',')
#                     header = next(csv_reader)
#                     columns = [sql.Identifier(col.lower()) for col in header]
#                     for row in csv_reader:
#                         values = [sql.Literal(val) if val != "" else sql.SQL('NULL') for val in row]
#                         insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
#                             sql.Identifier("reftab"),
#                             sql.Identifier(table_name),
#                             sql.SQL(", ").join(columns),
#                             sql.SQL(", ").join(values)
#                         )
#                         try:
#                             pg_cur.execute(insert_query)
#                         except Exception as e:
#                             error_message = f"Error executing INSERT statement: {e}, args={e.args}"
#                             helper.logger.info(error_message)
#                             print(error_message)  # Print error for visibility
#
#                             # Try to access the original exception
#                             original_exception = e.__cause__
#                             if original_exception:
#                                 original_error_message = f"Original Error: {original_exception}"
#                                 helper.logger.info(original_error_message)
#                                 print(original_error_message)
#
#         # commit changes and close connection
#         try:
#             pg_conn.commit()  # Move the commit statement to its own try-except block
#         except Exception as e:
#             error_message = f"Error committing changes: {e}, args={e.args}"
#             helper.logger.info(error_message)
#             print(error_message)  # Print error for visibility
#
#         pg_cur.close()
#         pg_conn.close()
#         helper.logger.info("RefTab is imported into db")
#         helper.logger.info("==============================================================================================")
#     except Exception as e:
#         error_message = f"Error: Oeps, something went wrong (reftab import csv): {e}, args={e.args}"
#         helper.logger.info(error_message)
#         print(error_message)  # Add this line for additional visibility


#
# def csv_to_postgres_old(postgres_database, postgres_user, postgres_password,
#                     postgres_host, postgres_port, csv_path):
#     try:
#         helper.logger.info("Started RefTab import in postgres")
#         helper.logger.info("-------------------------")
#
#         # connect to PostgreSQL database
#         pg_conn, pg_cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
#                                               postgres_host, postgres_port)
#
#         # loop through files in csv directory
#         for file_name in os.listdir(csv_path):
#             if file_name.endswith(".csv"):
#                 # get table name from file name
#                 table_name = file_name[:-4]  # remove ".csv" extension
#                 table_name = table_name.lower()  # convert to lowercase
#
#                 # create table in postgresql
#                 with open(os.path.join(csv_path, file_name), 'r') as csv_file:
#                     csv_reader = csv.reader(csv_file, delimiter=',')
#                     header = next(csv_reader)
#                     columns = []
#                     for col_name in header:
#                         col_name = col_name.lower()  # convert column name to lowercase
#                         columns.append(sql.Identifier(col_name))
#                     create_table_query = sql.SQL("CREATE TABLE {}.{} ({})").format(
#                         sql.Identifier("reftab"),
#                         sql.Identifier(table_name),
#                         sql.SQL(", ").join([col + sql.SQL(" text") for col in columns])
#                     )
#                     pg_cur.execute(create_table_query)
#
#                 # import data from csv file
#                 with open(os.path.join(csv_path, file_name), 'r') as csv_file:
#                     csv_reader = csv.reader(csv_file, delimiter=',')
#                     header = next(csv_reader)
#                     columns = [sql.Identifier(col.lower()) for col in header]
#                     for row in csv_reader:
#                         values = [sql.Literal(val) for val in row]
#                         insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
#                             sql.Identifier("reftab"),
#                             sql.Identifier(table_name),
#                             sql.SQL(", ").join(columns),
#                             sql.SQL(", ").join(values)
#                         )
#                         pg_cur.execute(insert_query)
#
#         # commit changes and close connection
#         pg_conn.commit()
#         pg_cur.close()
#         pg_conn.close()
#         helper.logger.info("RefTab is imported into db")
#         helper.logger.info(
#             "==============================================================================================")
#     except Exception as e:
#         helper.logger.info("Error: Oeps, something went wrong (reftab import csv): %s, args=%s: ", e, e.args)

