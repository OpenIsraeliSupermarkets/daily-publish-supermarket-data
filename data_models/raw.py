from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, Type, ClassVar
from il_supermarket_scarper import FileTypesFilters, ScraperFactory
from datetime import datetime



class ParserStatus(BaseModel):
    index: str
    requested_limit: Optional[str] = None
    requested_store_enum: str
    requested_file_type: str
    scaned_data_folder: str
    output_folder: str
    status: bool

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    @classmethod
    def get_table_name(cls) -> str:
        if hasattr(cls, 'Config') and hasattr(cls.Config, 'table_name'):
            return cls.Config.table_name
        return cls.__name__
    
    def get_index(self) -> str:
        return "index"

class ScraperStatus(BaseModel):
    index: str
    file_name: str
    timestamp: datetime
    status: bool
    when: datetime
    limit: Optional[int] = None
    files_requested: Optional[Dict[str, Any]] = None
    store_id: Optional[str] = None
    files_names_to_scrape: Optional[Dict[str, Any]] = None
    when_date: datetime
    filter_null: bool
    filter_zero: bool
    suppress_exception: bool

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    @classmethod
    def get_table_name(cls) -> str:
        if hasattr(cls, 'Config') and hasattr(cls.Config, 'table_name'):
            return cls.Config.table_name
        return cls.__name__
    
    def get_index(self) -> str:
        return "index"
    
class DataTable(BaseModel):
    row_index: int
    found_folder: str
    file_name: str
    content: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    @classmethod
    def get_table_name(cls) -> str:
        if hasattr(cls, 'Config') and hasattr(cls.Config, 'table_name'):
            return cls.Config.table_name
        return cls.__name__

    def get_index(self) -> str:
        return "row_index"
    
def create_dynamic_table_class(create_table_name: str) -> type:
    """Create a dynamic Pydantic model class for a specific table.
    
    Args:
        create_table_name (str): Name of the table
        
    Returns:
        type: A Pydantic model class for the table
    """
    class DynamicTableModel(DataTable):
        class Config:
            table_name = create_table_name
            
    return DynamicTableModel


def get_table_name(file_type:str, chain:str):
    return f"{file_type.lower()}_{chain.lower()}"

def file_name_to_table(filename):
    return filename.split(".")[0]

def list_all_dynamic_tables():
    """Initialize dynamic table models based on parser status file.
    
    Returns:
        dict: Dictionary mapping table names to their Pydantic model classes
    """
    table_models = []
    for file_type in FileTypesFilters.__members__.values():
        for chain in ScraperFactory.all_scrapers_name():
            table_name = get_table_name(file_type.name, chain)
            table_models.append(create_dynamic_table_class(table_name))
    return table_models