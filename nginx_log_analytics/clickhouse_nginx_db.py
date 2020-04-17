from sqlalchemy import create_engine, Column, MetaData, literal, func
from clickhouse_sqlalchemy import make_session, get_declarative_base, types, engines
from toolspy import subdict, filechunks, null_safe_type_cast
from pygrok import Grok
from sqlalchemy.orm import class_mapper
from datetime import datetime
import json
import os
from decimal import Decimal
import boto3
import gzip
from sqlalchemy.orm import class_mapper
import traceback
from .common import (
    generate_sqla_connection_uri,
    _insert_entries_from_log_file,
    _import_logs_from_folder)


uri = generate_sqla_connection_uri("clickhouse")

engine = create_engine(uri)
session = make_session(engine)
metadata = MetaData(bind=engine)

Base = get_declarative_base(metadata=metadata)


def column_names(modelcls):
    return [c.name for c in class_mapper(WeblogEntry).columns]


class WeblogEntry(Base):

    __tablename__ = "weblog_entries"

    raw_text = Column(
        types.Nullable(types.String), primary_key=True)
    remote_address = Column(types.String)
    remote_user = Column(types.Nullable(types.String))
    created_on = Column(types.DateTime)
    method = Column(types.String)
    request_uri = Column(types.String)
    http_version = Column(types.Nullable(types.String))
    response_status = Column(types.Nullable(types.Int))
    response_bytes_sent = Column(types.Nullable(types.Int))
    http_referrer = Column(types.Nullable(types.String))
    http_user_agent = Column(types.Nullable(types.String))
    forwarded_for_ips = Column(types.Nullable(types.String))
    hostname = Column(types.Nullable(types.String))
    server_name = Column(types.Nullable(types.String))
    request_time = Column(
        types.Nullable(types.Decimal(precision=10, scale=4)))
    upstream_status = Column(types.Nullable(types.Int))
    upstream_response_time = Column(
        types.Nullable(types.Decimal(precision=10, scale=4)))
    upstream_response_length = Column(types.Nullable(types.Int))
    clientip = Column(types.Nullable(types.String))
    user_id = Column(types.Nullable(types.String))
    session_id = Column(types.Nullable(types.String))

    __table_args__ = (
        engines.ReplacingMergeTree(
            primary_key=(
                created_on, remote_address, method, request_uri),
            order_by=(
                created_on, remote_address, method, request_uri)
        ),
    )


def create_tables():
    weblog_entries = WeblogEntry.__table__
    weblog_entries.create()


def drop_tables():
    weblog_entries = WeblogEntry.__table__
    weblog_entries.drop()


def insert_entries_from_log_file(filepath):
    return _insert_entries_from_log_file(
        filepath, session, WeblogEntry)

def import_logs_from_folder(folder_path):
    return _import_logs_from_folder(
        folder_path, session, WeblogEntry)
