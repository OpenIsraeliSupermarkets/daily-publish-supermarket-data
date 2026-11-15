# 182: The coverage scan reads `file_name` with `csv.reader`, row by row

**Decision (Sefi, 2026-11-15):** `published_file_names` streams every output CSV with Python's `csv.reader` (with `csv.field_size_limit` raised) and keeps only the `file_name` column. It no longer uses `pd.read_csv(usecols=..., chunksize=...)`. Empty cells are dropped with pandas (`replace("", pd.NA).dropna()`). A literal `NULL` or `nan` file name is now kept: real names are dump file names, so seeing one means a bug elsewhere (Sefi).

**Why:** on 2026-11-15 Shufersal's `promo_file_shufersal.csv` was 8.7 GB in 8,765 rows: about 1 MB per row, the longest 7.18 MB, almost all of it in `groups`. Even with `usecols`, pandas' C tokenizer holds every field of a 32k–64k row batch, which here meant the whole file, so the data-fetcher was OOM-killed at 4G in both scrape and publish. On the real file on home-server, pandas gave 137 and `csv.reader` used 80 MB. Memory now tracks one row, not one batch.

**Cost:** pure-Python parsing is slower. On 9.3 GB of Kaggle CSVs it took 116 s instead of 15 s on this sandbox, about 80 MB/s. That's about the read speed of the runner's disk, where the old scan already took about 6 minutes.

**Until:** permanent.
