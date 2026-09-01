---
name: quality-issue-triage
description: Opens GitHub issues on israeli-supermarket-scarpers and israeli-supermarket-parsers from published Kaggle quality JSON. Use when reviewing scraper_quality.json / parser_quality.json / pipeline_health.json, after a Kaggle publish, or when asked to file quality gaps. Downloads only those JSON files — never chain zips or CSVs.
---

You file processing-quality issues from the **published Kaggle quality JSON**, then stop. You do not scrape, parse, or download supermarket dumps.

**Goal**

Turn `pipeline_health.json` (or recomputed improvements from the quality JSON) into deduped GitHub issues:

| Stage | Repo |
|---|---|
| scraping | `OpenIsraeliSupermarkets/israeli-supermarket-scarpers` |
| parsing | `OpenIsraeliSupermarkets/israeli-supermarket-parsers` |

**Hard rules**

- Download **only** `scraper_quality.json`, `parser_quality.json`, and `pipeline_health.json`.
- Never call Kaggle download without a `path=` of one of those three names.
- Never download `.csv`, `.zip`, chain folders (`tivtaam/…`), or the full dataset.
- Chain keys in quality JSON are scraper enums (`TIV_TAAM`); Kaggle folders are dump stems (`tivtaam`). Issues use the dump stem in the title.
- Do not change scraper/parser code unless the user explicitly asks after issues exist.
- Do not skip fingerprint dedupe.

**How to run**

Use the project skill / script (do not invent a full-dataset download):

```bash
python scripts/kaggle_quality_issues.py plan
python scripts/kaggle_quality_issues.py apply --dry-run
python scripts/kaggle_quality_issues.py apply
```

1. Run `plan`. Confirm candidates look right (scrape failures vs parse gaps).
2. Run `apply --dry-run` unless the user already asked to file issues.
3. Run `apply` when asked to open issues. Existing open issues with the same `quality-fingerprint` get a comment, not a duplicate.

**Issue shape**

- Labels: `automation`, `quality`, `bug`
- One scrape issue per chain (`download_failures`, `no_data`, `saw_but_not_downloaded`)
- One parse issue per chain if total problem files ≥ 50 (grouped file types)
- Body cites the Kaggle quality JSON URLs, not zip members

**Done when**

You report the plan counts and the issue URLs (or dry-run titles). Link each URL.
