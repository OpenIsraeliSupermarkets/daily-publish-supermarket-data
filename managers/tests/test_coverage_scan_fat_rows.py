"""The coverage scan streams CSVs row by row, whatever the row width (#182)."""

import csv
import subprocess
import sys

import pandas as pd

from managers.indicators.coverage import published_file_names

FAT = 2_000_000  # bytes per fat field; prod Shufersal promo rows reach ~7 MB


def _write_fat_csv(path, rows):
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["found_folder", "file_name", "groups"])
        for n in range(rows):
            # Quoted commas, quotes and newlines inside the fat field.
            groups = ('{"item": "a,b", "q": "x""y"}\n' * (FAT // 30))[:FAT]
            writer.writerow(["app_data/dumps/Shufersal", f"Promo{n % 7}.xml", groups])


_MEASURE = """
import resource, sys
from managers.indicators.coverage import published_file_names
before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
names = published_file_names(sys.argv[1])
after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
print(len(names), (after - before) // 1024)
"""


def test_fat_rows_are_read_one_at_a_time(tmp_path):
    rows = 60  # ~120 MB file
    _write_fat_csv(tmp_path / "promo_file_shufersal.csv", rows=rows)

    # A fresh process, so the peak RSS growth is the scan's alone.
    result = subprocess.run(
        [sys.executable, "-c", _MEASURE, str(tmp_path)],
        capture_output=True,
        text=True,
        check=True,
    )
    # The scan logs to stdout too (#180); the measurement is the last line.
    count, growth_mb = (int(v) for v in result.stdout.splitlines()[-1].split())

    assert count == 7
    # About one row in flight (a few MB), not the 120 MB file. The pandas
    # scan this replaced grew by roughly the file size.
    assert growth_mb < 40, growth_mb


def test_matches_pandas_on_missing_values(tmp_path):
    frame = pd.DataFrame(
        {
            "found_folder": ["f"] * 4,
            "file_name": ["a/Price1.xml", "", "a/NULLPrice2.xml", None],
            "x": ["1", "2", "3", "4"],
        }
    )
    frame.to_csv(tmp_path / "price_file_x.csv", index=False)
    (tmp_path / "empty.csv").write_text("", encoding="utf-8")
    pd.DataFrame({"other": [1]}).to_csv(tmp_path / "no_file_name.csv", index=False)
    (tmp_path / "notes.txt").write_text("file_name\nPrice9.xml\n", encoding="utf-8")

    expected = {
        "Price1",
        "Price2",  # normalize_filename strips the NULL prefix, as before
    }
    via_pandas = set(
        pd.read_csv(tmp_path / "price_file_x.csv", usecols=["file_name"], dtype=str)[
            "file_name"
        ].dropna()
    )
    assert via_pandas == {"a/Price1.xml", "a/NULLPrice2.xml"}
    assert published_file_names(str(tmp_path)) == expected


def test_short_rows_and_missing_folder(tmp_path):
    (tmp_path / "price_file_x.csv").write_text(
        "found_folder,file_name\nonly_one_field\nf,Price3.xml\n", encoding="utf-8"
    )
    assert published_file_names(str(tmp_path)) == {"Price3"}
    assert published_file_names(str(tmp_path / "missing")) == set()
    assert published_file_names("") == set()
