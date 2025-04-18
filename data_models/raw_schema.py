from pydantic import BaseModel, Field, field_validator
from typing import Optional, Dict, Any, Type, ClassVar, List,Union
from il_supermarket_scarper import FileTypesFilters, ScraperFactory
from datetime import datetime
import json

class CommonModel(BaseModel):
    class Config:
        json_encoders = {
            datetime: lambda dt: dt.strftime("%Y-%m-%d %H:%M:%S")
        }


class ExecutionLog(CommonModel):
    loaded: bool
    succusfull: bool
    detected_num_rows: int
    store_folder: str
    file_name: str
    prefix_file_name: str
    extracted_store_number: str
    extracted_chain_id: str
    extracted_date: str
    detected_filetype: str
    size: str
    is_expected_to_have_records: bool
    
class Response(CommonModel):
    status: bool
    store_name: str
    files_types: str
    processed_files: bool
    execution_errors: bool
    file_was_created: bool
    file_created_path: str
    files_to_process: List[str]
    execution_log: List[ExecutionLog]
    
class ParserStatus(CommonModel):
    index: str
    requested_limit: Optional[str] = None
    requested_store_enum: str
    requested_file_type: str
    scaned_data_folder: str
    output_folder: str
    status: bool
    response: Response 

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    @classmethod
    def get_table_name(cls) -> str:
        if hasattr(cls, 'Config') and hasattr(cls.Config, 'table_name'):
            return cls.Config.table_name
        return cls.__name__
    
    @classmethod
    def get_index(cls) -> str:
        return "index"


class ScraperStartedStatus(CommonModel):
    limit: int
    files_requested: Optional[Dict[str, Any]] = None
    store_id: Optional[str] = None
    files_names_to_scrape: Optional[Dict[str, Any]] = None
    when_date: Optional[datetime] = None
    filter_nul: bool
    filter_zero: bool
    suppress_exception: bool

class ScraperCollectedStatus(CommonModel):
    file_name_collected_from_site: List[str]
    links_collected_from_site: List[str] 
    
    @field_validator('links_collected_from_site', mode='before')
    @classmethod
    def all_empty(cls, v):
        if v is "":
            return []
        return v

class DownloadedStatus(CommonModel):
    file_name: str
    downloaded: bool
    extract_succefully: bool
    error: Optional[str] = None
    restart_and_retry: bool
    
class ScraperDownloadedStatus(CommonModel):
    results: List[DownloadedStatus]

class FolderSize(CommonModel):
    size: float
    unit: str
    folder: str
    folder_content: List[str]

class ScraperEstimatedSizeStatus(CommonModel):
    folder_size: FolderSize
    completed_successfully: bool

class ScraperStatus(CommonModel):
    index: str
    file_name: str
    timestamp: datetime
    status: str
    when: datetime
    status_data: Union[ScraperStartedStatus, ScraperCollectedStatus, ScraperDownloadedStatus, ScraperEstimatedSizeStatus]

    def to_dict(self) -> Dict[str, Any]:
        return json.loads(self.model_dump_json())

    @classmethod
    def get_table_name(cls) -> str:
        if hasattr(cls, 'Config') and hasattr(cls.Config, 'table_name'):
            return cls.Config.table_name
        return cls.__name__
    
    @classmethod
    def get_index(cls) -> str:
        return "index"
    
class DataTable(CommonModel):
    row_index: int
    found_folder: str
    file_name: str
    content: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    @classmethod
    def by_file_name(cls, file_name: str):
        return {"file_name": file_name}

    @classmethod
    def get_table_name(cls) -> str:
        if hasattr(cls, 'Config') and hasattr(cls.Config, 'table_name'):
            return cls.Config.table_name
        return cls.__name__

    @classmethod
    def get_index(cls) -> str:
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
    return filename.split("/")[-1].split(".")[0]

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