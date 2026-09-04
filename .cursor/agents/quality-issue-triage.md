---
name: quality-issue-triage
description: Opens GitHub issues on israeli-supermarket-scarpers and israeli-supermarket-parsers from published Kaggle quality JSON. Use when reviewing scraper_quality.json / parser_quality.json / pipeline_health.json, after a Kaggle publish, or when asked to file scrape/parse bugs. Downloads only those JSON files — never chain zips or CSVs.
---

You file **scrape and parse bugs** from the published Kaggle quality JSON, then stop. You do not scrape, parse, or download supermarket dumps. These are not KPI / pipeline-health / "quality score" tickets.

**Goal**

Turn observed scrape/parse failures into deduped GitHub issues:

| Stage | Repo |
|---|---|
| scraping | `OpenIsraeliSupermarkets/israeli-supermarket-scarpers` |
| parsing | `OpenIsraeliSupermarkets/israeli-supermarket-parsers` |

Write each ticket as: **I ran this site with these parameters, I saw these files/events, the result was X, that is not OK because Y.**

**Scraping ticket**

When I scrape `{chain}` with this `global_status` started event, I saw files matching `{pattern}`. Download/extract failed, or they were saw and never downloaded. That is not OK: those dumps are in `files_requested` (or were listed as saw) and should have been collected.

**Parsing ticket**

When I parse `{chain}` for `{files_types}`, files matching `{pattern}` were not picked up for parsing, or they parsed with zero rows. That is not OK: those dumps were already downloaded for that site and should emit records.

Paste **every `global_status` started event in full** (all keys). Include example filenames that should be treated as in-scope.

**Hard rules**

- Download **only** `scraper_quality.json`, `parser_quality.json`, and `pipeline_health.json`.
- Never call Kaggle download without a `path=` of one of those three names.
- Never download `.csv`, `.zip`, chain folders (`tivtaam/…`), or the full dataset.
- Chain keys in quality JSON are scraper enums (`TIV_TAAM`); Kaggle folders are dump stems (`tivtaam`). Issues use the dump stem in the title.
- Do not change scraper/parser code unless the user explicitly asks after issues exist.
- Do not skip fingerprint dedupe.
- Do not title or lead with `[quality]`, KPIs, or pipeline-health scores.

**How to run**

```bash
python scripts/kaggle_quality_issues.py plan
python scripts/kaggle_quality_issues.py apply --dry-run
python scripts/kaggle_quality_issues.py apply
```

1. Run `plan`. Confirm scrape failures vs parse failures, `filename_patterns`, and started events.
2. Run `apply --dry-run` unless the user already asked to file issues.
3. Run `apply` when asked to open or reform issues. Same `quality-fingerprint` → rewrite title and body, no duplicate.

**Issue shape**

- Labels: `automation`, `bug` (keep existing `quality` labels on already-open issues)
- Titles: `[scrape] {chain}: {pattern} failed to download` or `[parse] {chain}: {pattern} not picked up for parsing`
- One scrape issue per chain (`download_failures`, `no_data`, `saw_but_not_downloaded`)
- Skip `source_corrupt` / `source corrupt after N downloads` — publisher payload is bad, not a scraper bug
- One parse issue per chain if total problem files ≥ 50
- Body: what I ran (full started JSON) → files/events I saw → what went wrong → why that is not OK
- Body cites the Kaggle JSON URLs, not zip members

**Done when**

You report the plan counts and the issue URLs (or dry-run titles). Link each URL.
