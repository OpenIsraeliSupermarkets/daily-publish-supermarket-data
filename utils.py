import os
from remotes import (
    KaggleUploader,
    MongoDbUploader,
    DummyFileStorage,
    DummyDocumentDbUploader,
)
import datetime
import pytz


storage_classes = {
    "KaggleUploader": KaggleUploader,
    "MongoDbUploader": MongoDbUploader,
    "DummyFileStorage": DummyFileStorage,
    "DummyDocumentDbUploader": DummyDocumentDbUploader,
}


def get_long_term_database_connector():
    return _get_class_from_env("LONG_TERM_MEMORY", KaggleUploader)


def get_short_term_database_connector():
    return _get_class_from_env("SHORT_TERM_MEMORY", MongoDbUploader)


def _get_class_from_env(env_var_name, default_class):
    class_name = os.environ.get(env_var_name, "KaggleUploader")
    if class_name in storage_classes:
        return storage_classes[class_name]
    else:
        return default_class


def now():
    return datetime.datetime.now(pytz.timezone("Asia/Jerusalem")).strftime(
        "%d/%m/%Y, %H:%M:%S"
    )