import table as tb
import helper
import time
import mail
import traceback


# def execute_sqlscript(postgres_database, postgres_user, postgres_password,
#                     postgres_host, postgres_port, sqlfilepath_ini, recipients, message):
def execute_sqlscript(postgres_database, postgres_user, postgres_password,
                      postgres_host, postgres_port, sqlfilepath_ini, message):
    try:
        helper.logger.info("Making connection sqlscript")
        conn, cur = tb.create_connection_postgres(postgres_database, postgres_user, postgres_password,
                    postgres_host, postgres_port)
        helper.logger.info("Start executing sqlscript")
        with open(sqlfilepath_ini, 'r') as f: # Open the SQL script and read its contents
            sql = f.read()
            sql_commands = sql.split(';')

        statement_count = 0
        for i, command in enumerate(sql_commands):
            if command.strip():  # ignore empty commands
                start_time = time.perf_counter()
                # telt vanaf 2 omdat eerste sql command leeg is
                # helper.logger.info(f"Executing SQL statement {i + 1}/{len(sql_commands)}: {command}")
                statement_count += 1  # increment statement count
                statement_index = statement_count  # calculate statement index
                # sql komt niet in de logging
                # helper.logger.info(f"Executing SQL statement {statement_index}/{len(sql_commands) - sql_commands.count('')}: {command}")
                # sql komt in de logging
                helper.logger.info(f"Executing SQL statement {statement_index}/{len(sql_commands) - sql_commands.count('')}: {sql_commands[i]}")
                cur.execute(command)
                conn.commit()
                end_time = time.perf_counter()  # end timer for this SQL statement
                statement_time = end_time - start_time  # calculate time taken for this SQL statement
                helper.logger.info(f"Time taken for SQL statement {i + 1}: {statement_time:.4f} seconds")

        conn.close() # Close the database connection
        # helper.logger.info("Finished executing sqlscript part")
        subject = f"MonFin: SQL script {message} execution succesfully"
        body = f"SQL script {message} execution succesfully"
        helper.logger.info("body: %s", body)
        helper.logger.info("subject: %s", subject)

        # recipients_str = recipients
        # recipients_list = recipients_str.split(';') #splits string into a list
        # mail.send_email(subject, body, recipients_list) #sql_script execution successfully
        # mail.send_mail_after_execution_script1(recipients_list)
    except Exception as e:
        # Get the index of the failed SQL statement
        statement_index = statement_count
        for j, command in enumerate(sql_commands[:i]):
            if not command.strip():
                statement_index -= 1
        traceback_str = traceback.format_exc()

        subject = "SQL script execution failed"
        # body = f"Error: {e}\nSQL statement {statement_index}/{len(sql_commands) - sql_commands.count('')}: {sql_commands[i]}"
        body = f"Error: {e}\n\nTraceback:\n{traceback_str}\n\nSQL statement {statement_index}/{len(sql_commands) - sql_commands.count('')}: {sql_commands[i]}"
        helper.logger.info("body: %s", body)
        helper.logger.info("subject: %s", subject)

        # if isinstance(recipients, list): #--
        #     recipients_str = ';'.join(recipients) #is een lijst, dus joined met ; en uiteindelijk een string #--
        # else: #--
        #     recipients_str = recipients #is al een string, rechtstreeks toegekend #--
        # recipients_str = recipients
        # recipients_list = recipients_str.split(';')
        # mail.send_email(subject, body, recipients_list)
        helper.logger.info("Error: Oeps, something went wrong (sqlscript part x): %s, args=%s: ", e, e.args)










