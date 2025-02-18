import csv
# import win32com
import table as tb
import helper
import os
import zipfile
import shutil
# import win32com.client as win32
# import configparser
# import time
# import pythoncom
# import psutil


# def get_outlook_app():
#     try:
#         outlook = win32.GetActiveObject('Outlook.Application')
#     except pythoncom.com_error:
#         outlook = win32.Dispatch('Outlook.Application')
#     return outlook


# def get_outlook_process():
#     # Get a list of all running processes
#     for proc in psutil.process_iter(['pid', 'name']):
#         if proc.info['name'].lower() == 'outlook.exe':
#             return proc
#     return None

# def is_outlook_running():
#     try:
#         # Attempt to access the Outlook Application object
#         win32com.client.Dispatch("Outlook.Application")
#         return True
#     except pythoncom.com_error:
#         return False
#
# def get_outlook_process():
#     if is_outlook_running():
#         # If Outlook is running, return the Outlook Application object
#         return win32com.client.Dispatch("Outlook.Application")
#
#     return None
#
#
# def send_email(subject, body, recipients, attachments=None):
#     outlook_process = get_outlook_process()
#
#     try:
#         if outlook_process:
#             outlook = outlook_process
#         else:
#             outlook = win32.Dispatch('outlook.application')
#
#         mail = outlook.CreateItem(0)
#         mail.Subject = subject
#         mail.Body = body
#         for recipient in recipients:
#             mail.Recipients.Add(recipient)
#         if attachments:
#             for attachment in attachments:
#                 mail.Attachments.Add(attachment)
#         mail.Send()
#         helper.logger.info("MonFin: Mail %s is sent", subject)
#     except pythoncom.com_error as e:
#         # Handle the com_error exception, which occurs if Outlook is not running or accessible
#         helper.logger.error(f"Error occurred while sending email: {e}")
#     except Exception as e:
#         helper.logger.error(f"Error occurred while sending email: {e}")



def export_setuptables(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port, exportsetup_path):
    try:
        helper.logger.info("Started Send to other FGP's - setup")
        helper.logger.info("------------------------------------")
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)

        # Delete all files and subdirectories in the exportsetup_path directory
        for filename in os.listdir(exportsetup_path):
            file_path = os.path.join(exportsetup_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                helper.logger.info('Failed to delete %s. Reason: %s' % (file_path, e))

        schema_name = 'setup'

        # Get the table names in the setup schema
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s", (schema_name,))
        helper.logger.info("Selected data from schema setup")

        # Create a zipfile for the export
        helper.logger.info("Creating zipfile setup.zip")
        zipfilename = "setup.zip"
        with zipfile.ZipFile(os.path.join(exportsetup_path, zipfilename), mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
            # Loop through the table names and export each table to a CSV file
            for table_name in cur.fetchall():
                # Get the table data
                cur.execute('SELECT * FROM "{}"."{}"'.format(schema_name, table_name[0]))

                rows = cur.fetchall()

                # Write the table data to a temporary CSV file
                temp_csvfile = os.path.join(exportsetup_path, table_name[0] + ".csv")
                with open(temp_csvfile, "w", newline="") as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow([desc[0] for desc in cur.description])
                    writer.writerows(rows)

                # Add the CSV file to the zip file
                zf.write(temp_csvfile, arcname=os.path.join("setup", table_name[0] + ".csv"))

                # Remove the temporary CSV file
                os.remove(temp_csvfile)

            # Get the column datatypes for each table
            cur.execute("SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = %s", (schema_name,))
            helper.logger.info("Selected data from schema setup")

            # Write the column datatypes to a temporary CSV file
            temp_csvfile = os.path.join(exportsetup_path, "Setup_datatypes.csv")
            helper.logger.info("Writing data to csv")
            with open(temp_csvfile, "w", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["Table Name", "Column Name", "Data Type"])
                table_columns = {}
                for row in cur.fetchall():
                    table_name = row[0]
                    if table_name not in table_columns:
                        table_columns[table_name] = []
                    table_columns[table_name].append(row)
                for table_name, columns in table_columns.items():
                    for column in columns:
                        writer.writerow([table_name] + list(column[1:]))

            # Add the CSV file to the zip file
            zf.write(temp_csvfile, arcname="Setup_datatypes.csv")
            helper.logger.info("Created zipfile setup.zip")

            # Remove the temporary CSV file
            os.remove(temp_csvfile)

        cur.close()
        conn.close()

    except Exception as e:
        helper.logger.error("Error while processing (export setup): " + str(e))


def export_mail_datatables(postgres_database, postgres_user, postgres_password,
                           postgres_host, postgres_port, exportdata_path, fgp_list, exportsetup_path):

    try:
        helper.logger.info("Started Send to other FGP's - data")
        helper.logger.info("------------------------------------")
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                                                  postgres_host, postgres_port)
        # Delete all files and folders in the exportdata_path
        for filename in os.listdir(exportdata_path):
            file_path = os.path.join(exportdata_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                helper.logger.info('Failed to delete %s. Reason: %s' % (file_path, e))

        if not os.path.exists(exportdata_path):
            os.makedirs(exportdata_path)

        # config = configparser.ConfigParser()
        # config.read('MonFin.ini')

        for fgp in fgp_list:
            helper.logger.info(f"Started treating FGP {fgp}")
            helper.logger.info("------------------------------------")
            table_name = f"Tbl_Final_{fgp}"

            # Create the folder for this fgp if it doesn't exist
            fgp_folder_path = os.path.join(exportdata_path, fgp)
            if not os.path.exists(fgp_folder_path):
                os.makedirs(fgp_folder_path)

            subfolder = fgp.capitalize()  # creates 'Limburg' or 'Leuven' subfolder
            subfolder_path = os.path.join(exportdata_path, subfolder)
            cur.execute('SELECT * FROM final."{}"'.format(table_name))
            rows = cur.fetchall()

            #Split file
            file_num = 1
            batch_size = 50000
            while rows:
                helper.logger.info(f"Writing data to csv - {file_num}")
                batch = rows[:batch_size]
                rows = rows[batch_size:]
                #maak een nieuwe file voor deze batch
                csv_file_path_batch = os.path.join(subfolder_path, f"{fgp}_{file_num}.csv")
                with open(csv_file_path_batch, 'w', newline='', encoding='utf-8') as csv_file:
                    writer = csv.writer(csv_file)
                    writer.writerow([desc[0] for desc in cur.description])
                    writer.writerows(batch)

                # Create a zip file containing the CSV file
                helper.logger.info("Creating data.zip")
                zip_file_path = os.path.join(subfolder_path, f"{fgp}_{file_num}.zip")
                with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                    zip_file.write(csv_file_path_batch, os.path.join(fgp, f"{fgp}_{file_num}.csv"))
                helper.logger.info("Created data.zip")

                # send email to recipients for this fgp
                # helper.logger.info("Getting variables for sending mail")
                # if file_num == 1:
                #     helper.logger.info("Getting variables subject for sending mail")
                #     subject = f"MonFin: Data export for {fgp} - {file_num}"
                #     helper.logger.info("Getting variables body for sending mail")
                #     body = f"Attached are the data export files for {fgp}"
                #     helper.logger.info(f"Getting variables recipients for fgp {fgp} for sending mail")
                #     recipients_str = config['mail'][fgp]
                #     recipients_list = recipients_str.split(';')
                #     helper.logger.info(f"Getting variables exportsetup_path for sending mail")
                #     attachments = [zip_file_path, os.path.join(exportsetup_path, "setup.zip")]
                # else:
                #     subject = f"MonFin: Data export for {fgp} - {file_num}"
                #     attachments = [zip_file_path]
                # helper.logger.info("Wait for 2 seconds before sending mail")
                # time.sleep(2)
                # helper.logger.info("Send mail")
                # send_email(subject, body, recipients_list, attachments)
                helper.logger.info("Remove csv file")
                os.remove(csv_file_path_batch)
                file_num += 1
            helper.logger.info(f"Stopped treating FGP {fgp}")
            helper.logger.info(f"-----")
        cur.close()
        conn.close()

    except Exception as e:
        print("Error while processing (export data): " + str(e))

# def send_mail_after_execution_script1(recipients):
#     subject = f"SQL script1 is executed succesfully"
#     body = f"SQL script1 is executed succesfully"
#     # recipients_list = recipients.split(';')
#     recipients_list = recipients #is already a list
#     send_email(subject, body, recipients_list)


# def send_email_exe(subject, body, recipients, attachments=None):
#     config = configparser.ConfigParser()
#     config.read('MonFin_old.ini')
#     email_sender = config['script']['email_sender_path']
#
#     # Prepare the arguments for the executable
#     arguments = [email_sender, subject, body, ",".join(recipients)]
#     if attachments:
#         arguments.extend(attachments)
#
#     # Call the executable using subprocess
#     subprocess.run(arguments)


# def send_email(subject, body, recipients, attachments=None):
#     MAX_ATTEMPTS = 2
#     outlook = None
#     attempt = 1
#
#     while attempt <= MAX_ATTEMPTS:
#         try:
#             outlook = win32.Dispatch('outlook.application')
#             break  # If successful, break out of the loop
#         except Exception as e:
#             helper.logger.error("Failed to open Outlook (attempt %d of %d): %s", attempt, MAX_ATTEMPTS, str(e))
#             attempt += 1
#             time.sleep(5)  # Wait for 5 seconds before trying again
#
#     if not outlook:
#         helper.logger.error("Failed to open Outlook after %d attempts. Exiting.", MAX_ATTEMPTS)
#         return
#
#     try:
#         mail = outlook.CreateItem(0)
#         mail.Subject = subject
#         mail.Body = body
#         for recipient in recipients:
#             mail.Recipients.Add(recipient)
#         if attachments:
#             for attachment in attachments:
#                 mail.Attachments.Add(attachment)
#         mail.Send()
#         helper.logger.info("MonFin: Mail %s is sent", subject)
#     except Exception as e:
#         helper.logger.error("Failed to send email: %s", str(e))
#     finally:
#         outlook.Quit()  # Close Outlook after sending the email


# def choose_outlook_account(outlook):
#     accounts = outlook.Session.Accounts
#     if accounts.Count == 1:
#         return accounts[0]
#
#     print("")
#     print("Please choose the email account to send the email:")
#     for idx, account in enumerate(accounts, start=1):
#         print(f"{idx}. {account.DisplayName} - {account.SmtpAddress}")
#
#     while True:
#         try:
#             print("")
#             choice = int(input("Enter the number of the account: "))
#             if 1 <= choice <= accounts.Count:
#                 return accounts[choice - 1]
#             print("Invalid choice. Please enter a valid number.")
#         except ValueError:
#             print("Invalid input. Please enter a valid number.")


# def send_email2(subject, body, recipients, attachments=None):
#     MAX_ATTEMPTS = 2
#     outlook = None
#     attempt = 1
#
#     while attempt <= MAX_ATTEMPTS:
#         try:
#             helper.logger.info("Attempting to open Outlook (attempt %d of %d)", attempt, MAX_ATTEMPTS)
#             outlook = win32.Dispatch('outlook.application')
#             # chosen_account = choose_outlook_account(outlook)
#             break  # If successful, break out of the loop
#         except Exception as e:
#             helper.logger.error("Failed to open Outlook (attempt %d of %d): %s", attempt, MAX_ATTEMPTS, str(e))
#             attempt += 1
#             time.sleep(5)  # Wait for 5 seconds before trying again
#
#     if not outlook:
#         helper.logger.error("Failed to open Outlook after %d attempts. Exiting.", MAX_ATTEMPTS)
#         return
#
#     try:
#         chosen_account = choose_outlook_account(outlook)
#         account_info = f"{chosen_account.DisplayName} - {chosen_account.SmtpAddress}"
#         helper.logger.info("Using email account: %s", account_info)
#
#         mail = outlook.CreateItem(0)
#         mail._oleobj_.Invoke(*(64209, 0, 8, 0, chosen_account))  # Set the account to use
#
#         helper.logger.info("Preparing the email...")
#         mail.Subject = subject
#         mail.Body = body
#         for recipient in recipients:
#             mail.Recipients.Add(recipient)
#         if attachments:
#             for attachment in attachments:
#                 mail.Attachments.Add(attachment)
#
#         helper.logger.info("Sending the email...")
#         mail.Send()
#
#         helper.logger.info("Mon(t)Fin: Mail %s is sent", subject)
#     except Exception as e:
#         helper.logger.error("Failed to send email: %s", str(e))
#     finally:
#         outlook.Quit()  # Close Outlook after sending the email

