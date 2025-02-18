import win32com.client as win32


def send_email(subject, body, recipients, attachments=None):
    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.Subject = subject
    mail.Body = body
    for recipient in recipients:
        mail.Recipients.Add(recipient)
    if attachments:
        for attachment in attachments:
            mail.Attachments.Add(attachment)
    mail.Send()
    print("Mon(t)Fin: Mail %s is sent" % subject)
