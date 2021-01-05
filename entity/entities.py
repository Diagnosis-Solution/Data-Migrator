from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sa

Base = declarative_base()


class Enterprise(Base):
    __tablename__ = 'enterprise'
    id = sa.Column('id', sa.Integer, primary_key=True)
    name = sa.Column('name', sa.String)

    def __init__(self, name):
        self.name = name