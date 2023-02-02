import csv
import apachelogs
from apachelogs import LogParser
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from enum import Enum
from tqdm import tqdm
from user_agents import parse as ua_parse


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


# https://httpd.apache.org/docs/current/mod/mod_log_config.html
log_format = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" \"%{Some-IP}i\""
log_parser = LogParser(log_format)


def write_to_cassandra(source):
    profile = ExecutionProfile(request_timeout=30)
    cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    session = cluster.connect('my_keyspace')

    in_file = open(source, 'r')

    while True:
        line = in_file.readline()

        if not line:
            break

        log = log_parser.parse(line)
        ua_string = log.headers_in["User-Agent"]
        user_agent = ua_parse(ua_string)

        session.execute(
            """
            INSERT INTO apache_logs (
                remote_host, remote_logname, remote_user, request_time, request_line, final_status, bytes_sent, user_agent,
                device_family, device_brand, device_model,
                browser_family, browser_version,
                is_mobile, is_tablet, is_pc, is_bot
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s)
            """,
            (log.remote_host, log.remote_logname, log.remote_user, log.request_time, log.request_line, log.final_status, int(log.bytes_sent), ua_string,
             user_agent.device.family, user_agent.device.brand, user_agent.device.model,
             user_agent.browser.family, user_agent.browser.version_string,
             user_agent.is_mobile, user_agent.is_tablet, user_agent.is_pc, user_agent.is_bot)
        )

    in_file.close()


def write_to_csv(source, dest='../data/result.csv'):
    with open(source, 'r') as in_file, open(dest, 'w') as out_file:
        writer = csv.writer(out_file, escapechar="\\", quoting=csv.QUOTE_MINIMAL)
        header = ["remote_host", "remote_logname", "remote_user", "request_time",
                  "request_line", "final_status", "bytes_sent", "user_agent",
                  "device_family", "device_brand", "device_model",
                  "browser_family", "browser_version",
                  "is_mobile", "is_tablet", "is_pc", "is_bot"
                  ]
        writer.writerow(header)

        lines = in_file.readlines()
        for row in tqdm(lines):
            try:
                log = log_parser.parse(row)
                ua_string = log.headers_in["User-Agent"]
                user_agent = ua_parse(ua_string)

                line = [log.remote_host, log.remote_logname, log.remote_user, log.request_time,
                        log.request_line, log.final_status, log.bytes_sent, ua_string,
                        user_agent.device.family, user_agent.device.brand, user_agent.device.model,
                        user_agent.browser.family, user_agent.browser.version_string,
                        user_agent.is_mobile, user_agent.is_tablet, user_agent.is_pc, user_agent.is_bot
                        ]

                writer.writerow(line)
            except apachelogs.InvalidEntryError as ex:
                print(Colors.FAIL + 'The format specified does not match the log file. Aborting...' + Colors.ENDC)
                print('Line: ' + ex.log_line + 'RegEx: ' + ex.regex)
                exit()
            # except csv.Error as ex:
                # print(Colors.FAIL + log.request_line + Colors.ENDC)
                # exit()

    print(Colors.OKGREEN + 'Conversion finished.' + Colors.ENDC)


write_strategies = {
    DataLoadingMode.Incremental: write_to_cassandra,
    DataLoadingMode.Initializing: write_to_csv
}

data_loading_mode = DataLoadingMode.Incremental
path = '/mnt/nvme/Projects/DataEngineer/apache_logs_analysis/data/head'

write_strategies[data_loading_mode](path)
