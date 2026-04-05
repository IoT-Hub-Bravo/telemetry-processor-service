from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import os
from src.config import DATABASE_URL, ALEMBIC_DATABASE_URL

engine = create_async_engine(DATABASE_URL, echo=True)
db_engine = create_engine(ALEMBIC_DATABASE_URL, echo=True)
AsyncSessionLocal = sessionmaker(
    bind=engine, class_=AsyncSession, expire_on_commit=False
)

Base = declarative_base()