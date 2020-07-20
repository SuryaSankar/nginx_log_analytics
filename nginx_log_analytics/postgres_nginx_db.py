from sqlalchemy import (
    create_engine, Column, MetaData,
    Integer, String, Numeric, DateTime)
from sqlalchemy.orm import class_mapper
from .common import (
    generate_sqla_connection_uri,
    _insert_entries_from_log_file,
    _import_logs_from_folder)
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base


uri = generate_sqla_connection_uri("postgresql")

engine = create_engine(uri)
session_maker = sessionmaker(bind=engine)
metadata = MetaData(bind=engine)

Base = declarative_base(metadata=metadata)


def column_names(modelcls):
    return [c.name for c in class_mapper(WeblogEntry).columns]


class WeblogEntry(Base):

    __tablename__ = "weblog_entries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    raw_text = Column(String)
    remote_address = Column(String)
    remote_user = Column(String, nullable=True)
    created_on = Column(DateTime)
    method = Column(String)
    request_uri = Column(String)
    http_version = Column(String, nullable=True)
    response_status = Column(Integer, nullable=True)
    response_bytes_sent = Column(Integer, nullable=True)
    http_referrer = Column(String, nullable=True)
    http_user_agent = Column(String, nullable=True)
    forwarded_for_ips = Column(String, nullable=True)
    hostname = Column(String, nullable=True)
    server_name = Column(String, nullable=True)
    request_time = Column(
        Numeric(precision=10, scale=4))
    upstream_status = Column(Integer, nullable=True)
    upstream_response_time = Column(
        Numeric(precision=10, scale=4))
    upstream_response_length = Column(Integer, nullable=True)
    clientip = Column(String, nullable=True)
    user_id = Column(String, nullable=True)
    session_id = Column(String, nullable=True)


def create_tables():
    weblog_entries = WeblogEntry.__table__
    weblog_entries.create()


def drop_tables():
    weblog_entries = WeblogEntry.__table__
    weblog_entries.drop()


def insert_entries_from_log_file(filepath):
    _insert_entries_from_log_file(
        filepath, session_maker, WeblogEntry)


def import_logs_from_folder(folder_path):
    _import_logs_from_folder(
        folder_path, session_maker, WeblogEntry)
