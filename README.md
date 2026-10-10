# Bank Transaction Ingestion & Data Quality Pipeline

A Python (Pandas) batch pipeline that ingests daily bank-transaction files, removes duplicates,
quarantines invalid records with reasons, is safe to re-run, applies late-arriving corrections to
the right historical date and produces a reconciliation summary.

The full design is in [docs/Approach_Document_Transaction_DQ_Pipeline.docx](docs/Approach_Document_Transaction_DQ_Pipeline.docx),
and the architecture diagram is in [docs/architecture_diagram.png](docs/architecture_diagram.png).

## Results on the provided files

| Measure | Count |
|---|---|
| Total records received | 190 (187 + 3) |
| Successfully processed | 183 |
| Rejected (quarantined) | 3: `MISSING_REF_NO`, `BOTH_DEBIT_AND_CREDIT`, `NO_DEBIT_OR_CREDIT` (1 each) |
| Duplicates removed | 4 (exact copies inside `batch_2026-08-01.csv`) |
| Late correction impact | +3 rows into `txn_date=2026-08-01` (180 → 183), 0 updates, 0 conflicts |
| Missing files | `batch_2026-08-02.csv` (and nothing for 2026-08-03) |

Control check: 190 = 183 + 3 + 4. The reconciliation is in
[reports/reconciliation_summary.md](reports/reconciliation_summary.md) (with a JSON copy next to it).

| Deliverable | Location |
|---|---|
| Clean dataset | `output/clean/txn_date=2026-08-01/transactions.csv` (all dates combined: `output/clean/transactions_all.csv`) |
| Quarantine file | `output/quarantine/quarantine.csv` |
| Duplicates audit | `output/audit/duplicates_removed.csv` |
| Reconciliation summary | `reports/reconciliation_summary.md` / `.json` |
| Processing manifest | `state/manifest.json` |

## Setup and run

Requires Python 3.10 or later.

```bash
python -m venv .venv
.venv\Scripts\activate              # Windows; on Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt     # pandas, PyYAML, pytest + this project (editable)

python -m pipeline.run --config config/pipeline.yaml
pytest
```

Options:

| Flag | Effect |
|---|---|
| `--force-reprocess` | Re-read files even if already processed. Output does not change, by design. |
| `--dry-run` | Process and log counts without writing anything. |
| `--log-level DEBUG` | More detail in the console and `logs/pipeline.log`. |

The outputs from a run are committed, so running the pipeline on a fresh clone skips both files
and reports `no outputs changed`. To start from scratch, delete `output/`, `reports/` and `state/` first.

Exit codes: `0` success, `1` a file failed to load or a control check failed, `2` configuration error.

## How it works

```
input/*.csv ─► 1 Ingest & register ─► 2 Read & standardise ─► 3 Validate ─► 4 Deduplicate ─► 5 Merge & publish
                 (SHA-256, manifest,     (all text, strict       │ hard: quarantine  │ conflicts:      (txn_date partitions,
                  missing dates)          dates, Decimal 2dp)    │ soft: warnings     │ quarantine      atomic, sorted)
                                                                 ▼                    ▼
                                                    output/quarantine/      output/audit/duplicates_removed.csv
                                                                  6 Reconciliation report ◄── manifest + store + quarantine + audit
```

| Module | Responsibility |
|---|---|
| `config.py` | Loads and validates `config/pipeline.yaml` |
| `ingest.py` | Finds files by pattern, orders them by file date, hashes them, finds calendar gaps |
| `manifest.py` | Records each file (by content hash) with status and counts |
| `reader.py` | Reads every column as text, checks the header, adds lineage, parses dates, times and amounts |
| `validate.py` | Hard rules (reject) and soft rules (warn) |
| `dedup.py` | Business-key de-duplication |
| `store.py` | Clean store partitioned by `txn_date` |
| `quarantine.py` | Quarantine and duplicates-audit files |
| `report.py` | Reconciliation summary (Markdown + JSON) |
| `run.py` | Orchestration and command line |

Every received row ends up in exactly one place: the clean dataset, the quarantine file or the
duplicates audit. Each file's counts are checked with `received = processed + rejected + duplicates`
before anything is written.

## Duplicate handling

**Key: `account_number + ref_no`.** `ref_no` is the bank's transaction reference; the account is
included because a reference is only guaranteed unique within an account.

| Situation | Treatment |
|---|---|
| Same key, same values, different source line | Exact duplicate: removed and written to the audit file with the row it duplicates |
| Same key, same values, same file + line | Already loaded (a re-run): no change |
| Same key, different values | `DUPLICATE_KEY_CONFLICT`: quarantined for review; the loaded row is never overwritten |
| Different `ref_no` | Genuine transaction: always kept |

Values are compared after standardisation, so `100` vs `100.00` or `savings` vs `SAVINGS` still match.
A key such as account + date + amount was rejected because it would delete genuine repeats. For
example, `AC100000032` has three `INTEREST CREDIT SB AC` credits with different references.
Validation runs before de-duplication, so an invalid row is never kept as the surviving copy.

## Validation rules

| Rule | Reason code | Severity |
|---|---|---|
| Account number missing or not `AC` + 9 digits | `INVALID_ACCOUNT_NUMBER` | reject |
| `txn_date` missing, wrong format or impossible (`31-02-2026`) | `INVALID_TXN_DATE` | reject |
| `txn_date` after the file date, or more than 30 days before it | `INVALID_TXN_DATE` | reject |
| `txn_time` invalid | `INVALID_TXN_TIME` | reject |
| Both debit and credit present | `BOTH_DEBIT_AND_CREDIT` | reject |
| Neither debit nor credit present | `NO_DEBIT_OR_CREDIT` | reject |
| Amount not numeric, zero or negative | `INVALID_AMOUNT` | reject |
| `ref_no` missing | `MISSING_REF_NO` | reject |
| Balance missing or not numeric | `INVALID_BALANCE` | reject |
| Account type differs from the account's established type | `ACCOUNT_TYPE_MISMATCH` | warn |
| Balance ≠ previous balance − debit + credit (posting order) | `BALANCE_DISCONTINUITY` | warn |
| Negative balance on a SAVINGS account | `NEGATIVE_SAVINGS_BALANCE` | warn |

All rules run on every row, so a rejected row lists every reason (separated by `; `). The
quarantine file keeps the **original** values exactly as received, plus the source file, line number
and file hash. Warnings are stored in the `dq_warnings` column of the clean dataset.

## Re-processing (idempotency)

There are two independent safeguards:

1. **File level:** each file's SHA-256 is recorded in `state/manifest.json`. A file that was already
   processed is skipped. A file with a known name but new content is treated as a restated delivery.
2. **Record level:** the clean store is merged by key, never appended to. A row coming from the same
   file and line is recognised as already loaded, so `--force-reprocess`, or even deleting the
   manifest, leaves every output byte-identical (both are covered by tests).

Output is deterministic: fixed sort order and column order, 2-decimal amounts, LF line endings, and
no timestamps in data files or reports (run times live only in the manifest and the log). Files
are written via a temp file and an atomic rename, and only when their content changes. The
manifest is written last, so a failed run is simply redone next time.

## Late correction handling

`batch_2026-08-04_late_correction.csv` contains three transactions dated **01-08-2026**.

- A row is late when its `txn_date` is earlier than its file date (taken from the file name); it is
  flagged with `is_late_arrival = true`.
- Rows are written into the partition for their **business date** (`txn_date=2026-08-01`), not the
  arrival date, and only that partition is rewritten.
- They are de-duplicated against history: new keys are added, identical keys are skipped, and keys
  with different values are quarantined. Existing rows are never overwritten.
- Balances are kept as supplied. All three late rows break their account's running balance and
  disagree with the account type seen on 08-01, so each carries `ACCOUNT_TYPE_MISMATCH;
  BALANCE_DISCONTINUITY` for review.
- The report shows the impact: 180 → 183 rows, +18,500.00 debit, +97,000.00 credit, accounts
  AC100000002, AC100000005 and AC100000009.

Replacing the whole 01-08 partition with the correction file was rejected, because the file only
holds 3 rows and doing so would wipe out 180 valid transactions.

## Configuration

Everything specific to a source is in [config/pipeline.yaml](config/pipeline.yaml): the file name
pattern, column list (with an optional rename map), date and time formats, amount scale, the dedup
key, which rules are active, the backdating window, the delivery calendar (`daily` or
`business_days`) and the output paths. To onboard another feed, add another YAML file and run with
`--config` pointing to it.

## Assumptions and design decisions

1. `batch_2026-08-02.csv` is listed in the brief but was not provided, and it does not exist. It is
   reported as a gap, and it will be processed without code changes if it arrives.
2. `account_number + ref_no` uniquely identifies a transaction.
3. Dates are day-first (`DD-MM-YYYY`), consistent with the 08-01 file containing only `01-08-2026`.
4. Debit and credit are mutually exclusive positive amounts. A blank cell means no value; zero or a
   negative amount is invalid.
5. Rows without `ref_no` are rejected, because they cannot be de-duplicated safely.
6. A late correction file **adds** transactions. It does not update or delete existing rows, and
   conflicting values are quarantined, never applied silently.
7. `balance` is supplied by the source and never recalculated; breaks are warnings, not rejections.
   The same applies to account-type conflicts, because there is no account master data to settle them.
8. The order of rows in a file is the posting order: balances chain in file order even though
   `txn_time` does not (in 77 cases the time goes backwards within an account).
9. Files are processed in order of file date, then name. Every file counts as a delivery for its date
   when finding gaps.
10. Raw input files are never modified. They are stored in Git byte-for-byte (`.gitattributes`),
    because the pipeline identifies files by hash.
11. The data volume is small, so single-machine Pandas is sufficient. Stages are DataFrame-in,
    DataFrame-out, so they port to PySpark or Delta Lake `MERGE` without changing the design.

## Tests

```bash
pytest -q        # 43 tests
```

The suite covers every validation rule (including invalid dates that the sample data does not
contain), the de-duplication scenarios, idempotent re-runs, `--force-reprocess`, a lost manifest,
restated files, late corrections, schema drift, blank lines, the business-day calendar, CLI exit
codes, and an end-to-end run on the provided files asserting 190 / 183 / 3 / 4.

## Project structure

```
config/pipeline.yaml        source configuration
input/                      raw files as delivered (never modified)
src/pipeline/               pipeline package
tests/                      pytest suite
output/clean/               clean dataset, partitioned by txn_date
output/quarantine/          rejected records with reasons
output/audit/               removed duplicates
reports/                    reconciliation summary (.md, .json)
state/manifest.json         processing manifest
docs/                       approach document and architecture diagram
```

## Possible extensions

- Store the clean data in Delta Lake or Iceberg with `MERGE`, and add Parquet output.
- Make the conflict policy configurable (for example, latest wins, with history kept).
- Schedule runs with Airflow or Prefect, and alert on missing files or rejection spikes.
- Add an account master to validate account type, and a workflow to replay fixed quarantined records.
