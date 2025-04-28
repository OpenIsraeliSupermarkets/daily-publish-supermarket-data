from pydantic import BaseModel, field_validator
from typing import Optional, Dict, Any, List, Union
from il_supermarket_scarper import FileTypesFilters, ScraperFactory
from datetime import datetime
import json


class CommonModel(BaseModel):
    """Base model class that defines common configuration settings.
    
    Provides standard JSON encoding behavior for datetime objects.
    """
    class Config:
        json_encoders = {datetime: lambda dt: dt.strftime("%Y-%m-%d %H:%M:%S")}


class ExecutionLog(CommonModel):
    """Log entry for a single file execution process.
    
    Contains detailed information about the processing of a specific supermarket data file,
    including extraction status, file metadata, and processing results.
    """
    loaded: bool
    succusfull: bool = False
    detected_num_rows: Optional[int] = None
    store_folder: str
    file_name: str
    prefix_file_name: str
    extracted_store_number: str
    extracted_chain_id: str
    extracted_date: str
    detected_filetype: str
    size: str
    is_expected_to_have_records: bool

    @field_validator("extracted_store_number", mode="before")
    @classmethod
    def int_to_str(cls, v):
        if isinstance(v, int):
            return str(v)
        return v

class Response(CommonModel):
    """Response model containing the overall execution results.
    
    Provides a summary of the file processing operation, including which files were
    processed, any errors encountered, and execution logs for each file.
    """
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
    """Status model for a parser execution.
    
    Records the results and configuration of a parsing operation, including the store,
    file type, and execution response details.
    """
    index: str
    when_date: str
    requested_limit: Optional[str] = None
    requested_store_enum: str
    requested_file_type: str
    scaned_data_folder: str
    output_folder: str
    status: bool
    response: Response

    def to_dict(self) -> Dict[str, Any]:
        """Convert the model to a dictionary.
        
        Returns:
            Dict[str, Any]: Model data as a dictionary
        """
        return self.model_dump()

    @classmethod
    def get_table_name(cls) -> str:
        """Get the database table name for this model.
        
        Returns:
            str: Table name from Config or class name
        """
        if hasattr(cls, "Config") and hasattr(cls.Config, "table_name"):
            return cls.Config.table_name
        return cls.__name__

    @classmethod
    def get_index(cls) -> str:
        """Get the index field name.
        
        Returns:
            str: Name of the index field
        """
        return "index"

class ScraperException(CommonModel):
    """Exception model for a scraping operation.
    
    Records the details of an exception that occurred during a scraping operation.
    """
    execption: str
    download_urls: List[str]
    file_names: List[str]

class ScraperStartedStatus(CommonModel):
    """Status model for the start of a scraping operation.
    
    Contains the initial configuration and parameters for the scraper.
    """
    limit: int
    files_requested: Optional[Dict[str, Any]] = None
    store_id: Optional[str] = None
    files_names_to_scrape: Optional[Dict[str, Any]] = None
    when_date: Optional[datetime] = None
    filter_nul: bool
    filter_zero: bool
    suppress_exception: bool

    @field_validator("limit", mode="before")
    @classmethod
    def all_empty(cls, v):
        """Convert empty string to -1.
        
        Args:
            v: Value to validate
            
        Returns:
            List: Empty list if input is empty string, otherwise original value
        """
        if v == "":
            return -1
        return v

class ScraperCollectedStatus(CommonModel):
    """Status model for the file collection phase of scraping.
    
    Records the files and links that were collected from the supermarket website.
    """
    file_name_collected_from_site: List[str]
    links_collected_from_site: List[str]

    @field_validator("links_collected_from_site", mode="before")
    @classmethod
    def all_empty(cls, v):
        """Convert empty string to empty list.
        
        Args:
            v: Value to validate
            
        Returns:
            List: Empty list if input is empty string, otherwise original value
        """
        if v == "":
            return []
        return v


class DownloadedStatus(CommonModel):
    """Status model for a single downloaded file.
    
    Tracks the result of downloading and extracting a specific file.
    """
    file_name: str
    downloaded: bool
    extract_succefully: bool
    error: Optional[str] = None
    restart_and_retry: bool


class ScraperDownloadedStatus(CommonModel):
    """Status model for all downloaded files in a scraping operation.
    
    Contains a list of download results for each file.
    """
    results: List[DownloadedStatus]


class FolderSize(CommonModel):
    """Model for folder size information.
    
    Records the size and contents of a folder.
    """
    size: float
    unit: str
    folder: str
    folder_content: List[str]


class ScraperEstimatedSizeStatus(CommonModel):
    """Status model for the estimated size of scraped data.
    
    Provides information about the storage requirements for the scraped data.
    """
    folder_size: FolderSize
    completed_successfully: bool


class ScraperStatus(CommonModel):
    """Status model for the overall scraping process.
    
    Tracks the state and results of a scraping operation at different stages.
    """
    index: str
    file_name: str
    timestamp: datetime
    status: str
    when: datetime
    status_data: Union[
        ScraperStartedStatus,
        ScraperCollectedStatus,
        ScraperDownloadedStatus,
        ScraperEstimatedSizeStatus,
        ScraperException
    ]

    def to_dict(self) -> Dict[str, Any]:
        """Convert the model to a dictionary.
        
        Returns:
            Dict[str, Any]: Model data as a dictionary
        """
        return json.loads(self.model_dump_json())

    @classmethod
    def get_table_name(cls) -> str:
        """Get the database table name for this model.
        
        Returns:
            str: Table name from Config or class name
        """
        if hasattr(cls, "Config") and hasattr(cls.Config, "table_name"):
            return cls.Config.table_name
        return cls.__name__

    @classmethod
    def get_index(cls) -> str:
        """Get the index field name.
        
        Returns:
            str: Name of the index field
        """
        return "index"


class DataTable(CommonModel):
    """Base model for data table entries.
    
    Represents a row of data extracted from a supermarket file.
    """
    row_index: int
    found_folder: str
    file_name: str
    content: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert the model to a dictionary.
        
        Returns:
            Dict[str, Any]: Model data as a dictionary
        """
        return self.model_dump()

    @classmethod
    def by_file_name(cls, file_name: str):
        """Create a filter condition for querying by file name.
        
        Args:
            file_name (str): Name of the file to filter by
            
        Returns:
            dict: Query filter condition
        """
        return {"file_name": file_name}

    @classmethod
    def get_table_name(cls) -> str:
        """Get the database table name for this model.
        
        Returns:
            str: Table name from Config or class name
        """
        if hasattr(cls, "Config") and hasattr(cls.Config, "table_name"):
            return cls.Config.table_name
        return cls.__name__

    @classmethod
    def get_index(cls) -> str:
        """Get the index field name.
        
        Returns:
            str: Name of the index field
        """
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


def get_table_name(file_type: str, chain: str):
    """Generate a standardized table name based on file type and chain.
    
    Args:
        file_type (str): Type of the file
        chain (str): Name of the supermarket chain
        
    Returns:
        str: Combined table name in lowercase
    """
    return f"{file_type.lower()}_{chain.lower()}"


def file_name_to_table(filename):
    """Extract table name from a filename.
    
    Args:
        filename (str): Full file path
        
    Returns:
        str: Table name derived from the filename
    """
    return filename.split("/")[-1].split(".")[0]


def list_all_dynamic_tables():
    """Initialize dynamic table models based on parser status file.

    Returns:
        list: List of Pydantic model classes for all tables
    """
    table_models = []
    for file_type in FileTypesFilters.__members__.values():
        for chain in ScraperFactory.all_scrapers_name():
            table_name = get_table_name(file_type.name, chain)
            table_models.append(create_dynamic_table_class(table_name))
    return table_models
