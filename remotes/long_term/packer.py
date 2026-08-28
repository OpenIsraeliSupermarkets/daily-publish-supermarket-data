"""Pack staged long-term dataset files into one zip per scraper."""

from collections import defaultdict
import os
import re
import zipfile

from il_supermarket_scarper import DumpFolderNames, ScraperFactory
from utils import Logger


class StagedDatasetPacker:
    """Group and zip staged long-term files by supermarket scraper."""

    KEEP_AS_IS = frozenset(
        {"index.json", "scraper_quality.json", "parser_quality.json"}
    )

    @staticmethod
    def normalize_name(value: str) -> str:
        return re.sub(r"[^a-z0-9]", "", value.lower())

    def scraper_prefixes(self):
        """Return (normalized_prefix, zip_stem) pairs per scraper, longest first.

        Zip stems always use DumpFolderNames. Matching also accepts scraper enum
        names when they differ in spelling (e.g. meshmat_yosef vs MeshnatYosef).
        """
        prefixes = []
        for scraper in ScraperFactory.all_scrapers_name():
            dump_name = DumpFolderNames[scraper].value
            stem = dump_name.lower()
            dump_norm = self.normalize_name(dump_name)
            prefixes.append((dump_norm, stem))
            scraper_norm = self.normalize_name(scraper)
            if scraper_norm != dump_norm:
                prefixes.append((scraper_norm, stem))
        prefixes.sort(key=lambda item: len(item[0]), reverse=True)
        return prefixes

    def _match_candidates(self, filename: str):
        """Normalized filename stems to try when matching a scraper prefix."""
        normalized = self.normalize_name(os.path.splitext(filename)[0])
        candidates = [normalized]
        # Parser outputs sometimes append `_temp` after the chain name.
        if normalized.endswith("temp"):
            candidates.append(normalized[: -len("temp")])
        return candidates

    def group_files_by_scraper(self, filenames):
        """Group staged filenames into one bucket per scraper.

        Matches dump-folder (or scraper-name) prefixes at the start or end of the
        filename stem so both `bareket_price_file.json` and `price_file_bareket.csv`
        map to the same zip.
        """
        groups = defaultdict(list)
        prefixes = self.scraper_prefixes()
        for filename in filenames:
            matched_stem = None
            for candidate in self._match_candidates(filename):
                for prefix, stem in prefixes:
                    if (
                        candidate == prefix
                        or candidate.startswith(prefix)
                        or candidate.endswith(prefix)
                    ):
                        matched_stem = stem
                        break
                if matched_stem is not None:
                    break
            groups[matched_stem or "misc"].append(filename)
        return groups

    def pack(self, dataset_path: str):
        """Pack files in dataset_path into one zip per scraper."""
        if not os.path.isdir(dataset_path):
            return

        staged_files = sorted(
            name
            for name in os.listdir(dataset_path)
            if os.path.isfile(os.path.join(dataset_path, name))
        )
        to_pack = [
            name
            for name in staged_files
            if name not in self.KEEP_AS_IS and not name.endswith(".zip")
        ]
        if not to_pack:
            return

        groups = self.group_files_by_scraper(to_pack)
        Logger.info(
            "Packing %s staged files into %s per-scraper zip archives",
            len(to_pack),
            len(groups),
        )

        for stem, filenames in groups.items():
            zip_path = os.path.join(dataset_path, f"{stem}.zip")
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
                for filename in filenames:
                    file_path = os.path.join(dataset_path, filename)
                    zipf.write(file_path, arcname=filename)
            for filename in filenames:
                os.remove(os.path.join(dataset_path, filename))

        packed_count = len(
            [
                name
                for name in os.listdir(dataset_path)
                if os.path.isfile(os.path.join(dataset_path, name))
            ]
        )
        Logger.info("Staged dataset now has %s files after per-scraper packing", packed_count)

    def unpack_zip(self, zip_path: str, dest_dir: str):
        """Extract one zip into dest_dir and return its member names."""
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(dest_dir)
            return list(zip_ref.namelist())

    def unpack(self, directory: str, remove_zips: bool = False):
        """Extract all .zip files in directory into that directory.

        Returns:
            list: Member names from every extracted archive.
        """
        if not os.path.isdir(directory):
            return []

        unpacked = []
        for name in sorted(os.listdir(directory)):
            if not name.endswith(".zip"):
                continue
            zip_path = os.path.join(directory, name)
            if not os.path.isfile(zip_path):
                continue
            unpacked.extend(self.unpack_zip(zip_path, directory))
            if remove_zips:
                os.remove(zip_path)
        return unpacked
