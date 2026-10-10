# Reconciliation Summary: bank_feed_a

## Overview

| Measure | Count |
|---|---|
| Total records received | 190 |
| Total records successfully processed | 183 |
| Total records rejected (quarantined) | 3 |
| Duplicates removed | 4 |
| Rows in clean dataset | 183 |

Control check `received = processed + rejected + duplicates_removed`: **PASSED** (190 = 183 + 3 + 4)

## By file

| File | File date | Type | Received | Processed | Rejected | Duplicates removed |
|---|---|---|---|---|---|---|
| batch_2026-08-01.csv | 2026-08-01 | daily | 187 | 180 | 3 | 4 |
| batch_2026-08-04_late_correction.csv | 2026-08-04 | late_correction | 3 | 3 | 0 | 0 |

## Rejected records by reason

| Reason | Records |
|---|---|
| BOTH_DEBIT_AND_CREDIT | 1 |
| MISSING_REF_NO | 1 |
| NO_DEBIT_OR_CREDIT | 1 |

Records with more than one reason: 0. Details: `output/quarantine/quarantine.csv`.

## Duplicates removed

| Type | Records |
|---|---|
| EXACT_DUPLICATE_IN_FILE | 4 |

Duplicate key: `account_number + ref_no`. Details: `output/audit/duplicates_removed.csv`.

## Late correction impact

**batch_2026-08-04_late_correction.csv** (late_correction)

- Late rows loaded into earlier dates: 3
- Partition `txn_date=2026-08-01`: 180 -> 183 rows (+3)
- Existing rows updated: 0 (corrections never overwrite loaded rows)
- Key conflicts quarantined: 0; other rejections: 0; duplicates removed: 0
- Amounts added: debit 18500.00, credit 97000.00
- Accounts affected: AC100000002, AC100000005, AC100000009
- Data-quality warnings on these rows: ACCOUNT_TYPE_MISMATCH x3, BALANCE_DISCONTINUITY x3

## Missing files

| Date | Expected file |
|---|---|
| 2026-08-02 | batch_2026-08-02.csv |
| 2026-08-03 | batch_2026-08-03.csv |

## Data-quality warnings (rows kept)

| Warning | Rows |
|---|---|
| ACCOUNT_TYPE_MISMATCH | 3 |
| BALANCE_DISCONTINUITY | 3 |
| NEGATIVE_SAVINGS_BALANCE | 3 |
