import os, time
import signal
import smtplib
from email.Header import Header
from email.Utils import parseaddr, formataddr
from email.mime.text import MIMEText


MONITORING_DIR = '/var/pdffiller/pdffiller-stack/' # path of the directory which should be monitored

TIME_LIMIT = 60 # time limit. if It was esceeded all processes with target_string in description will be stopped

SUBJECT = "Too long time for pdf processing" # subject of notification mail

MSG_TEXT = ("Following files were processed "
            "longer then %d seconds: {{FILENAMES}}" % TIME_LIMIT) # body of notification mail
                   # {{FILENAMES}} is placeholder for list of file namess separated by semicolumn

SERVER = "localhost" # mail server

SENDER = u"Monitoring bot <support@pdffiller.com>" #sender address

RECIPIENTS = ["<koshevchenko@gmail.com>",
              "<6178773156@txt.att.net>",
              "<support@pdffiller.com>"] #recipient addresses

TARGET_STRING = 'swfdaemon_corrected' # processes with such string in description will be stopped
                                      # if time be exceeded

HARD_STOP_SERVICE = False # Wheteher found services should be stopped or just notify via mail about time exceeding

DAEMON_STOP_STRING = 'python /var/pdffiller/swfdaemon_corrected.py stop'

DAEMON_START_STRING = 'python /var/pdffiller/swfdaemon_corrected.py start'

NUMBER_OF_OUTDATED_FILES = 5 # min Number of outdated files when service should be restarted

def send_mail(server, sender, recipient, subject, body):
    msg = MIMEText(body)

    header_charset = 'ISO-8859-1'

    # Split real name (which is optional) and email address parts

    sender_name, sender_addr = parseaddr(sender)
    recipient_name, recipient_addr = parseaddr(recipient)

    # We must always pass Unicode strings to Header, otherwise it will
    # use RFC 2047 encoding even on plain ASCII strings.
    sender_name = str(Header(unicode(sender_name), header_charset))
    recipient_name = str(Header(unicode(recipient_name), header_charset))

    # Make sure email addresses do not contain non-ASCII characters
    sender_addr = sender_addr.encode('ascii')
    recipient_addr = recipient_addr.encode('ascii')

    # Create the message ('plain' stands for Content-Type: text/plain)
    #msg = MIMEText(msg.encode(body_charset), 'plain', body_charset)
    msg['From'] = formataddr((sender_name, sender_addr))
    msg['To'] = formataddr((recipient_name, recipient_addr))
    msg['Subject'] = Header(unicode(subject), header_charset)

    server = smtplib.SMTP(server)
    time.sleep(0.3)
    server.sendmail(sender, recipient, msg.as_string())
    server.quit()

def analize_dir(dir_name, time_limit):
    pathes = [os.path.join(dir_name, i) for i in os.listdir(dir_name)]
    change_times = [(i, time.time() - os.lstat(i).st_ctime) for i in pathes]
    return [f_n for f_n, c_t in change_times if c_t > time_limit]

known_files = set()
while True:
    outdated_files = analize_dir(MONITORING_DIR, TIME_LIMIT)
    if len(outdated_files) >= NUMBER_OF_OUTDATED_FILES:
        msg = MSG_TEXT.replace('{{FILENAMES}}', '; '.join(outdated_files))
        if set(outdated_files) - known_files:
            for RECIPIENT in RECIPIENTS:
                send_mail(SERVER, SENDER, RECIPIENT, SUBJECT, msg)
            known_files.update(outdated_files)
        if HARD_STOP_SERVICE:
            f = os.popen('ps ax')
            processes = [i for i in f.readlines() if TARGET_STRING in i]
            f.close()
            process_ids = [int(i.strip().split(' ', 1)[0]) for i in processes]
            for pid in process_ids:
                os.kill(pid, signal.SIGKILL)
        else:
            os.system(DAEMON_STOP_STRING)
        os.system(DAEMON_START_STRING)
    time.sleep(1)
