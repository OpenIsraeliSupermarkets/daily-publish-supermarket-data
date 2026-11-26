"""Repeated file blocks in the output CSVs (#196)."""

import json

from managers.indicators.repeated_blocks import RepeatedFileBlocks, block_starts

A = "PriceFull7290027600007-001-202511250600.xml"
B = "PriceFull7290027600007-002-202511250600.xml"


def _csv(path, blocks):
    """One block per (file_name, rows); only a block's first row has the name."""
    lines = ["found_folder,file_name,itemcode"]
    for name, codes in blocks:
        lines.append(f"dumps,{name},{codes[0]}")
        lines.extend(f",,{code}" for code in codes[1:])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _status(folder, rows):
    folder.mkdir(exist_ok=True)
    (folder / "shufersal_price_full_file.json").write_text(
        json.dumps({"parsed_files": [{"file_name": n, "content_sha256": h} for n, h in rows]}),
        encoding="utf-8",
    )


def test_block_starts_streams_first_rows(tmp_path):
    path = tmp_path / "price_full_file_shufersal.csv"
    _csv(path, [(A, ["1", "2"]), (B, ["3"]), (A, ["4"])])
    assert list(block_starts(str(path))) == [(0, A), (2, B), (3, A)]


def test_same_hash_in_two_blocks_is_duplicated(tmp_path):
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    _csv(outputs / "price_full_file_shufersal.csv", [(A, ["1"]), (B, ["2"]), (A, ["1"])])
    _status(tmp_path / "status", [(A, "h1"), (B, "h2")])

    section = RepeatedFileBlocks.compute(str(outputs), str(tmp_path / "status"))

    assert section["duplicated_files"] == 1
    assert section["republished_files"] == 0
    assert section["by_csv"][0]["duplicated"] == [A]


def test_new_hash_in_second_block_is_republished(tmp_path):
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    _csv(outputs / "price_full_file_shufersal.csv", [(A, ["1"]), (A, ["9"])])
    _status(tmp_path / "status", [(A, "h1"), (A, "h2")])

    section = RepeatedFileBlocks.compute(str(outputs), str(tmp_path / "status"))

    assert (section["duplicated_files"], section["republished_files"]) == (0, 1)
    assert section["by_csv"][0]["republished"] == [A]


def test_entries_without_hash_count_as_duplicated(tmp_path):
    """Registry rows from before the parser recorded hashes can't prove new content."""
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    _csv(outputs / "price_full_file_shufersal.csv", [(A, ["1"]), (A, ["1"])])
    _status(tmp_path / "status", [(A, None), (A, None)])

    assert RepeatedFileBlocks.compute(str(outputs), str(tmp_path / "status"))[
        "duplicated_files"
    ] == 1


def test_no_repeats(tmp_path):
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    _csv(outputs / "price_full_file_shufersal.csv", [(A, ["1"]), (B, ["2"])])

    assert RepeatedFileBlocks.compute(str(outputs), str(tmp_path / "missing")) == {
        "duplicated_files": 0,
        "republished_files": 0,
        "by_csv": [],
    }
