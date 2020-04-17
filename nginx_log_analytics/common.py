from pygrok import Grok
from sqlalchemy import create_engine, Column, MetaData, literal, func
from decimal import Decimal
from datetime import datetime
import os
from decimal import Decimal
import gzip
from sqlalchemy.orm import class_mapper
from toolspy import subdict, filechunks, null_safe_type_cast
from databuddy import session_scope
import json


access_log_entry_pattern = '%{IPORHOST:remote_address} - %{USERNAME:remote_user} \[%{HTTPDATE:created_on}\] \"(?:%{WORD:method} %{DATA:request_uri} HTTP/%{NUMBER:http_version}|%{DATA:request_uri})\" %{INT:response_status} (?:%{NUMBER:response_bytes_sent}|-) \"(?:%{DATA:http_referrer}|-)\" \"(?:%{DATA:http_user_agent}|-)\" \"(?:%{DATA:forwarded_for_ips}|-)\" \"%{IPORHOST:hostname}\" sn=\"%{IPORHOST:server_name}\" rt=%{NUMBER:request_time} ua=\"(?:%{DATA:upstream_address}|-)\" us=\"(?:%{INT:upstream_status}|-)\" ut=\"(?:%{NUMBER:upstream_response_time}|-)\" ul=\"(?:%{INT:upstream_response_length}|-)\" cs=(?:%{INT:upstream_cache_status}|-) cfip=\"(?:%{IPORHOST:clientip}|-)\" userid=\"%{NOTSPACE:user_id}\" sessid=\"%{NOTSPACE:session_id}\"%{GREEDYDATA:extra_data}'

pattern_grok = Grok(access_log_entry_pattern)

field_types = {
    "response_status": int,
    "response_bytes_sent": int,
    "request_time": Decimal,
    "upstream_response_time": Decimal,
    "upstream_status": int,
    "upstream_response_length": int
}


def load_config_dict(config_path="vault/config.json"):
    with open(config_path) as configfile:
        return json.load(configfile)


def generate_sqla_connection_uri(dbconnstring):
    config = load_config_dict()
    db_params = config["DB_CONNECTION_PARAMS"][dbconnstring]
    return '{}://{}:{}@{}/{}'.format(
        dbconnstring,
        db_params["username"], db_params["password"],
        db_params["host"], db_params["db_name"]
    )


def column_names(modelcls):
    return [c.name for c in class_mapper(modelcls).columns]

def create_tables(WeblogEntry):
    weblog_entries = WeblogEntry.__table__
    weblog_entries.create()


def drop_tables(WeblogEntry):
    weblog_entries = WeblogEntry.__table__
    weblog_entries.drop()


def weblog_entry_dict(logline, log_entry_modelcls):
    entry = pattern_grok.match(logline)
    if entry: 
        entry = subdict(
            entry,
            column_names(log_entry_modelcls)
        )
        if entry['session_id'] == 'None':
            entry['session_id'] = None
        if entry['user_id'] == '_ANON_None':
            entry['user_id'] = None
        for k, v in entry.items():
            if v == "-":
                entry[k] = None

        timestamp_format = '%d/%b/%Y:%H:%M:%S %z'
        entry['created_on'] = datetime.strptime(
            entry['created_on'], timestamp_format)
        for k in ['created_on', 'remote_address', 'method', 'request_uri']:
            if entry.get(k) is None:
                return None
        for field, _type in field_types.items():
            entry[field] = null_safe_type_cast(_type, entry[field])

        entry['raw_text'] = logline
        for col in column_names(log_entry_modelcls):
            if col not in entry and col!='id':
                entry[col] = None
    return entry


def _insert_entries_from_log_file(filepath, session_factory, log_entry_modelcls):
    print("in insert entries")
    with session_scope(session_factory) as session:
        fopen = gzip.open if filepath.endswith(".gz") else open
        with fopen(filepath, "rt") as f:
            for chunk in filechunks(f, 5000):
                entries = []
                for l in chunk:
                    entry = weblog_entry_dict(l, log_entry_modelcls)
                    if entry:
                        entries.append(entry)
                session.execute(
                    log_entry_modelcls.__table__.insert(),
                    entries
                )
                print("Finished chunk")


def _import_logs_from_folder(folder_path, session_maker, log_entry_modelcls):
    for filename in os.listdir(folder_path):
        filepath = os.path.join(folder_path, filename)
        print(filepath)
        _insert_entries_from_log_file(
            filepath, session_maker, log_entry_modelcls)
        print("Finished \n")