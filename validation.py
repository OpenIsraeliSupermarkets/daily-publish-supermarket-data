import pymongo
from collections import defaultdict
import datetime
from itertools import chain
from il_supermarket_scarper import DumpFolderNames,ScraperFactory


# פונקציות עזר
def connect_to_mongodb(uri="mongodb://192.168.1.129:27017/"):
    """התחברות למסד הנתונים והגדרת האוספים"""
    client = pymongo.MongoClient(uri)
    db = client["supermarket_data"]
    return db["ScraperStatus"], db["ParserStatus"]

def get_file_timestamps(scraper_status_collection):
    """אחזור חותמות זמן ייחודיות לכל קובץ"""
    file_name_dict = defaultdict(set)
    for doc in scraper_status_collection.find({}, {"timestamp": 1, "file_name": 1, "_id": 0}):
        if "timestamp" in doc and "file_name" in doc:
            file_name_dict[doc["file_name"]].add(doc["timestamp"])
    return {file_name: sorted(timestamps) for file_name, timestamps in file_name_dict.items()}


def get_scraper_status(scraper_status_collection, chain_name, timestamp):
    """ניתוח קבצים של רשת ספציפית"""
    collected = scraper_status_collection.find_one(
        {"timestamp": timestamp, "file_name": chain_name.lower(), "status": "collected"})
    downloaded = scraper_status_collection.find_one(
        {"timestamp": timestamp, "file_name": chain_name.lower(), "status": "downloaded"})
    
    if not collected or not downloaded:
        return None, None, None
        
    files_saw = [f.replace(".gz", "") for f in collected['file_name_collected_from_site']]
    downloaded_files_success = [f['file_name'].replace(".gz", "") for f in downloaded['results'] if f['downloaded'] == True and f['extract_succefully'] == True]
    downloaded_files_failed = [f['file_name'].replace(".gz", "") for f in downloaded['results'] if not (f['downloaded'] == True and f['extract_succefully'] == True)]
    
    return files_saw, downloaded_files_success, downloaded_files_failed

def match_parsing_timestamps(parser_status_collection, sample_timestamp, chain_name):
    """התאמת חותמות זמן של הפרסור"""
    all_parsing_timestamps = list(set(
        doc['timestamp'] for doc in parser_status_collection.find(
            {"store_enum": chain_name}, 
            {"timestamp": 1, "_id": 0}
        )
    ))
    
    scraping_timestamp = datetime.datetime.strptime(sample_timestamp, "%Y%m%d%H%M%S")
    
    min_delta = None
    associated_stamp = None
    for parsing_timestamp in all_parsing_timestamps:
        parsing_timestamp_dt = datetime.datetime.strptime(parsing_timestamp, "%d%m%Y%H%M%S")
        diff = parsing_timestamp_dt - scraping_timestamp
        if min_delta is None or parsing_timestamp_dt > scraping_timestamp and diff < min_delta:
            associated_stamp = parsing_timestamp
            min_delta = diff
    return associated_stamp

def match_chain_names(chain_name):
    """התאמת שמות רשתות"""
    for k in DumpFolderNames:
        if k.value.lower() == chain_name.split(".")[0].lower():
            return str(k.name)
    return None

def get_parsing_status(parser_status_collection, matched_chain_name, matched_timestamp):
    """קבלת נתוני פרסור"""
    
    parsing_results = list(parser_status_collection.find({
            "store_enum": matched_chain_name,
            "timestamp": matched_timestamp
    }))
    assert len(parsing_results) == 5
    files_to_parse = list(chain.from_iterable(
        map(lambda x: list(map(
            lambda y: y.replace(".aspx", "").replace(".xml", ""),
            x['response']['files_to_process']
        )), 
        parsing_results
    )))
    
    parsing_results_success = list(chain.from_iterable(
        map(lambda x: [log['file_name'].replace(".aspx", "").replace(".xml", "") for log in x['response']['execution_log'] if log.get('status') == True],
        parsing_results
    )))
    
    parsing_results_failed = list(chain.from_iterable(
        map(lambda x: [log for log in x['response']['execution_log'] if log.get('status') != True],
        parsing_results
    )))
    
    return files_to_parse, parsing_results_success, parsing_results_failed

def collect_validation_results():
    """פונקציה ראשית"""
    scraper_status_collection, parser_status_collection = connect_to_mongodb()
    
    # קבלת חותמות זמן לכל קובץ
    file_name_dict = get_file_timestamps(scraper_status_collection)
    validation_results = dict()
    
    for chain in DumpFolderNames:
        
        chain_name  = chain.name
        folder_name = chain.value
        
        if folder_name.lower() not in file_name_dict:
            validation_results[folder_name.lower()] = []
            continue
        
        sample_timestamp = file_name_dict[folder_name.lower()][-1]
        validation_results[chain_name] = {sample_timestamp:[]}
        # ניתוח קבצים
        files_saw, downloaded_files_success, downloaded_files_failed = get_scraper_status(
            scraper_status_collection, folder_name, sample_timestamp)
        # התאמת נתוני פרסור
        matched_timestamp = match_parsing_timestamps(
            parser_status_collection, sample_timestamp, chain_name)
        
        # קבלת נתוני פרסור
        files_to_parse, parsing_results_success, parsing_results_failed = get_parsing_status(
            parser_status_collection, chain_name, matched_timestamp)
        
        # בדיקת סטטוס עבור כל קובץ
        for file in files_saw:
            result = {
                'file_name': file,
                'is_downloaded': file in downloaded_files_success,
                'is_saw_by_parser': file in files_to_parse,
                'is_parsed_successfully': file in parsing_results_success
            }
            validation_results[chain_name][sample_timestamp].append(result)
    return validation_results

if __name__ == "__main__":
    validation_results = collect_validation_results()



