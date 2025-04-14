from sqlalchemy import Column, String, Integer, DateTime, JSON, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from il_supermarket_scarper import FileTypesFilters, ScraperFactory

Base = declarative_base()

class ParserStatus(Base):
    __tablename__ = "ParserStatus"
    
    index = Column(String, primary_key=True)
    ChainName = Column(String)
    timestamp = Column(String)
    limit = Column(String, nullable=True)
    store_enum = Column(String)
    file_type = Column(String)
    data_folder = Column(String)
    output_folder = Column(String)
    status = Column(String)

class ScraperStatus(Base):
    __tablename__ = "ScraperStatus"
    
    index = Column(String, primary_key=True)
    file_name = Column(String)
    timestamp = Column(String)
    status = Column(String)
    when = Column(String)
    limit = Column(String, nullable=True)
    files_requested = Column(JSON, nullable=True)
    store_id = Column(String, nullable=True)
    files_names_to_scrape = Column(JSON, nullable=True)
    when_date = Column(String)
    filter_null = Column(String)
    filter_zero = Column(String)
    suppress_exception = Column(String)

# Dynamic table class factory for CSV data
def create_dynamic_table_class(table_name):
    class DynamicTable(Base):
        __tablename__ = table_name
        
        row_index = Column(String, primary_key=True)
        found_folder = Column(String)
        file_name = Column(String)
        content = Column(JSON)

        
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)
    
    return DynamicTable


def init_dynamic_tables_from_parser_status(engine):
    """Initialize dynamic tables based on parser status file.
    
    Args:
        parser_status_path (str): Path to parser-status.json
        engine: SQLAlchemy engine instance
    """
    import os
        
    for file_type in FileTypesFilters.__members__.values():
        for chain in ScraperFactory.all_scrapers_name():
            table_name = f"{file_type.name.lower()}_{chain.lower()}"
            table_class = create_dynamic_table_class(table_name)
            table_class.__table__.create(engine, checkfirst=True)
                
                
# Database connection setup
def init_db(connection_string):
    engine = create_engine(connection_string)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()