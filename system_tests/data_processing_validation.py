import pymongo
from collections import defaultdict
import datetime
import json
from itertools import chain
from il_supermarket_scarper import DumpFolderNames


# פונקציות עזר
def connect_to_mongodb(uri="mongodb://192.168.1.129:27017/"):
    """התחברות למסד הנתונים והגדרת האוספים"""
    client = pymongo.MongoClient(uri)
    db = client["supermarket_data"]
    return db["ScraperStatus"], db["ParserStatus"]


def get_file_timestamps(scraper_status_collection):
    """אחזור חותמות זמן ייחודיות לכל קובץ"""
    file_name_dict = defaultdict(set)
    for doc in scraper_status_collection.find(
        {}, {"timestamp": 1, "file_name": 1, "_id": 0}
    ):
        if "timestamp" in doc and "file_name" in doc:
            file_name_dict[doc["file_name"]].add(doc["timestamp"])
    return {
        file_name: sorted(timestamps)
        for file_name, timestamps in file_name_dict.items()
    }


def get_scraper_status(scraper_status_collection, chain_name, timestamp):
    """ניתוח קבצים של רשת ספציפית"""
    collected = scraper_status_collection.find_one(
        {"timestamp": timestamp, "file_name": chain_name.lower(), "status": "collected"}
    )
    downloaded = scraper_status_collection.find_one(
        {
            "timestamp": timestamp,
            "file_name": chain_name.lower(),
            "status": "downloaded",
        }
    )

    if not collected or not downloaded:
        return None, None, None

    files_saw = [
        f.replace(".gz", "").replace(".xml", "")
        for f in collected["status_data"]["file_name_collected_from_site"]
    ]
    downloaded_files_success = [
        f["file_name"].replace(".gz", "").replace(".xml", "")
        for f in downloaded["status_data"]["results"]
        if f["downloaded"] == True and f["extract_succefully"] == True
    ]
    downloaded_files_failed = {
        f["file_name"].replace(".gz", "").replace(".xml", ""): f["error"]
        for f in downloaded["status_data"]["results"]
        if not (f["downloaded"] == True and f["extract_succefully"] == True)
    }

    return files_saw, downloaded_files_success, downloaded_files_failed


def match_parsing_timestamps(
    used_timestamp, parser_status_collection, sample_timestamp, chain_name
):
    """התאמת חותמות זמן של הפרסור"""
    all_parsing_timestamps = list(
        set(
            doc["when_date"]
            for doc in parser_status_collection.find(
                {"requested_store_enum": chain_name}, {"when_date": 1, "_id": 0}
            )
        )
    )

    scraping_timestamp = datetime.datetime.strptime(
        sample_timestamp, "%Y-%m-%d %H:%M:%S"
    )

    min_delta = None
    associated_stamp = None
    for parsing_timestamp in all_parsing_timestamps:
        parsing_timestamp_dt = datetime.datetime.strptime(
            parsing_timestamp.strip(), "%Y-%m-%d %H:%M:%S"
        )
        diff = parsing_timestamp_dt - scraping_timestamp
        if (
            parsing_timestamp_dt > scraping_timestamp
            and (min_delta is None or diff < min_delta)
            and parsing_timestamp not in used_timestamp
        ):
            associated_stamp = parsing_timestamp
            min_delta = diff
    return associated_stamp, used_timestamp + [associated_stamp]


def get_parsing_status(parser_status_collection, matched_chain_name, matched_timestamp):
    """קבלת נתוני פרסור"""

    parsing_results = list(
        parser_status_collection.find(
            {"requested_store_enum": matched_chain_name, "when_date": matched_timestamp}
        )
    )
    assert len(parsing_results) == 5
    files_to_parse = list(
        chain.from_iterable(
            map(
                lambda x: list(
                    map(
                        lambda y: y.replace(".aspx", "")
                        .replace(".xml", "")
                        .replace(".gz", ""),
                        x["response"]["files_to_process"],
                    )
                ),
                parsing_results,
            )
        )
    )

    parsing_results_success = list(
        chain.from_iterable(
            map(
                lambda x: [
                    log["file_name"]
                    .replace(".aspx", "")
                    .replace(".xml", "")
                    .replace(".gz", "")
                    for log in x["response"]["execution_log"]
                    if log.get("succusfull", False) == True
                ],
                parsing_results,
            )
        )
    )

    parsing_results_failed = dict(
        chain.from_iterable(
            map(
                lambda x: [
                    (
                        log["file_name"]
                        .replace(".aspx", "")
                        .replace(".xml", "")
                        .replace(".gz", ""),
                        log.get(
                            "error",
                            "empty file" if not log.get("loaded") else "unknown",
                        ),
                    )
                    for log in x["response"]["execution_log"]
                    if log.get("succusfull", False) != True
                ],
                parsing_results,
            )
        )
    )

    return files_to_parse, parsing_results_success, parsing_results_failed


def validate_data_processing(uri="mongodb://192.168.1.129:27017/"):
    """פונקציה ראשית"""
    scraper_status_collection, parser_status_collection = connect_to_mongodb(uri)

    # קבלת חותמות זמן לכל קובץ
    file_name_dict = get_file_timestamps(scraper_status_collection)
    validation_results = dict()
    aggregated_errors = {}

    for chain in DumpFolderNames:

        chain_name = chain.name
        folder_name = chain.value

        if folder_name.lower() not in file_name_dict:
            validation_results[folder_name.lower()] = {}
            continue

        validation_results[chain_name] = {}
        used_timestamp = []
        chain_errors = {
            "downloaded": [],
            "parsed": [],
            "overall_file_saw": [],
            "overall_lost_files": [],
            "overall_download_files": [],
        }
        for itreation_timestamp in file_name_dict[folder_name.lower()]:

            validation_results[chain_name][itreation_timestamp] = {}
            # ניתוח קבצים
            files_saw, downloaded_files_success, downloaded_files_failed = (
                get_scraper_status(
                    scraper_status_collection, folder_name, itreation_timestamp
                )
            )
            # התאמת נתוני פרסור
            matched_timestamp, used_timestamp = match_parsing_timestamps(
                used_timestamp,
                parser_status_collection,
                itreation_timestamp,
                chain_name,
            )

            if not matched_timestamp:
                validation_results[chain_name][itreation_timestamp] = {}
                continue
            # קבלת נתוני פרסור
            files_to_parse, parsing_results_success, parsing_results_failed = (
                get_parsing_status(
                    parser_status_collection, chain_name, matched_timestamp
                )
            )

            # בדיקת סטטוס עבור כל קובץ
            pipeline = {
                "new_files": list(set(files_saw) - set(chain_errors["overall_file_saw"])),
                "saw": len(files_saw),
                "fail_downloaded": 0,
                "not_collected_by_parser": 0,
                "fail_parsed": 0,
                "succesful_processed": 0,
            }
            for file in files_saw:
                # failed at download
                if file in downloaded_files_failed:
                    pipeline["fail_downloaded"] += 1
                    chain_errors["downloaded"].append(downloaded_files_failed[file])
                    chain_errors["overall_lost_files"].append(file)
                # not failed
                elif file in downloaded_files_success:

                    if file not in files_to_parse:
                        pipeline["not_collected_by_parser"] += 1
                    elif file in parsing_results_failed:
                        pipeline["fail_parsed"] += 1
                        chain_errors["parsed"].append(parsing_results_failed[file])
                        chain_errors["overall_lost_files"].append(file)
                        
                    elif file in parsing_results_success:
                        pipeline["succesful_processed"] += 1
                        chain_errors["overall_download_files"].append(file)
                    else:
                        raise ValueError(f"file {file} is not in any of the lists")
                else:
                    raise ValueError(f"file {file} is not in any of the lists")
            chain_errors["overall_file_saw"] = list(set(files_saw) | set(chain_errors["overall_file_saw"]))
            validation_results[chain_name][itreation_timestamp] = pipeline
        
        aggregated_errors[chain_name] = chain_errors

    with open("validation_results.json", "w") as f:
        json.dump(validation_results, f, indent=4)
    with open("aggregated_errors.json", "w") as f:
        json.dump(aggregated_errors, f, indent=4)

    for chain, errors in aggregated_errors.items():
        assert len(set(errors["overall_lost_files"]) - set(errors['overall_download_files'])) / len(errors['overall_file_saw']) < 0.02, f"chain {chain} data lost in the data processing pipeline is too high"


if __name__ == "__main__":
    validate_data_processing(
        uri="mongodb://your_mongo_user:your_mongo_password@localhost:27017/"
    )
