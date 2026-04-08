from sqlalchemy import Column, String, Integer
from src.database import Base


class Metric(Base):
    __tablename__ = "metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    data_type = Column(String, nullable=False)
    unit = Column(String, nullable=True)
