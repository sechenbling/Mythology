from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from airflow.settings import SQL_ALCHEMY_CONN

sql_alchemy_engine = create_engine(SQL_ALCHEMY_CONN)
