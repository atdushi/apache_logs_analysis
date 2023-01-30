import csv
import apachelogs
from apachelogs import LogParser
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from enum import Enum
from tqdm import tqdm


class DataLoadingMode(Enum):
    Incremental = 1
    Initializing = 2


class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'


data_loading_mode = DataLoadingMode.Initializing

# https://httpd.apache.org/docs/current/mod/mod_log_config.html
log_format = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" \"%{Some-IP}i\""
log_parser = LogParser(log_format)


def write_to_cassandra(source):
    debug_print = False

    profile = ExecutionProfile(request_timeout=30)
    cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    session = cluster.connect('my_keyspace')

    if debug_print:
        print(session.execute("SELECT * FROM system.local").one())
        print('****************Apache Logs************* ')

    in_file = open(source, 'r')

    while True:
        line = in_file.readline()

        if not line:
            break

        log = log_parser.parse(line)

        session.execute(
            """
            INSERT INTO apache_logs (remote_host, remote_logname, remote_user, request_time, request_line, final_status, bytes_sent, user_agent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (log.remote_host, log.remote_logname, log.remote_user, log.request_time, log.request_line, log.final_status,
             int(log.bytes_sent), log.headers_in["User-Agent"])
        )

        if debug_print:
            print('Log text: ')
            print(line.strip())
            print(
                f"{log.remote_host} {log.remote_logname} {log.remote_user} {log.request_time} {log.request_line} {log.final_status} {log.bytes_sent} {log.headers_in['User-Agent']}")
            print('Parsed log: ')
            # print(log.directives["%u"])
            print('------------------------------')

    in_file.close()

    if debug_print:
        rows = session.execute('SELECT * FROM apache_logs LIMIT 10')
        for row in rows:
            print(row)


def write_to_csv(source, dest):
    with open(source, 'r') as in_file, open(dest, 'w') as out_file:
        writer = csv.writer(out_file, escapechar="\\", quoting=csv.QUOTE_MINIMAL)
        header = ["remote_host", "remote_logname", "remote_user", "request_time",
                  "request_line", "final_status", "bytes_sent", "user_agent"]
        writer.writerow(header)

        lines = in_file.readlines()
        for row in tqdm(lines):
            try:
                log = log_parser.parse(row)

                line = [log.remote_host, log.remote_logname, log.remote_user, log.request_time,
                        log.request_line, log.final_status, log.bytes_sent, log.headers_in['User-Agent']]

                writer.writerow(line)
            except apachelogs.InvalidEntryError as ex:
                print(Colors.FAIL + 'The format specified does not match the log file. Aborting...' + Colors.ENDC)
                print('Line: ' + ex.log_line + 'RegEx: ' + ex.regex)
                exit()
            # except csv.Error as ex:
                # print(Colors.FAIL + log.request_line + Colors.ENDC)
                # exit()

    print(Colors.OKGREEN + 'Conversion finished.' + Colors.ENDC)


path = 'access.log path'
if data_loading_mode == DataLoadingMode.Initializing:
    write_to_csv(path, '../data/result.csv')
else:
    write_to_cassandra(path)
