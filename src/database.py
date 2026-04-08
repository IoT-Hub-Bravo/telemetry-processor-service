from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from src.config import ALEMBIC_DATABASE_URL

db_engine = create_engine(ALEMBIC_DATABASE_URL, echo=True)
SessionLocal = sessionmaker(bind=db_engine, class_=Session, expire_on_commit=False)

Base = declarative_base()
