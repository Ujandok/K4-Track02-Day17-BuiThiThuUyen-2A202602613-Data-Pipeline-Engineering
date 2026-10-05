# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Bùi Thị Thu Uyên / 2A202602613
**Repo:** https://github.com/Ujandok/K4-Track02-Day17-BuiThiThuUyen-2A202602613-DataPipelineEngineering
**Commit bài nộp:** `1b6491b`
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code — đọc code, chỉ ra 3 lỗi, đề xuất và viết bản sửa trong `pipeline/` (3 file), chạy các lệnh kiểm tra, soạn và rút gọn REPORT.
**Nguồn tham khảo khác (nếu có):** Slide Ngày 17; model dbt có sẵn trong `dbt_project/` (dùng để đối chiếu logic).

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | verify: `24 rows for 12 tickets`; T-91 có 3 hàng | `gold_feature_daily` lệch full recompute (`c50b8851affe ≠ 8630e04a61d1`); u05 ngày 08-12 = `(2, 0)` thay vì `(5, 1)` | T-97 trong Silver chưa `is_deleted`, còn PII; snapshot mới nhất và RAG vẫn có T-97 |
| **Nguyên nhân gốc** | `INSERT` append mỗi batch: không khoá, không so LSN | `LOOKBACK_DAYS = 0`: event 08-12 tới ở batch 08-15, partition 08-12 không được tính lại | Lấy `ticket_id` từ `after`; delete có `after = null` → khoá NULL → bị lọc |
| **Cách sửa** | `silver.py`: `MERGE … ON ticket_id`, chỉ `UPDATE` khi `s._lsn > t._lsn` | `config.py`: `LOOKBACK_DAYS = 3` | `staging.py`: `coalesce(after.ticket_id, before.ticket_id)` → MERGE đè thành tombstone |
| **Khái niệm trên slide** | Silver có khoá; MERGE + LSN guard | Data về muộn; lookback = ceil(P99) đo từ Bronze | CDC `before/after/op`; tombstone; xoá phải lan |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3` (nhỏ hơn thì sót event muộn của u05, lớn hơn chỉ tốn thêm tính lại)
- `submission/checksums.txt`: **PASS** — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: **PARITY**

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: Silver nhận delta, có thể bị replay lệch thứ tự nên cần khoá + LSN để bản mới nhất thắng; Gold là aggregate theo ngày, tính lại cả partition là tất định và hấp thụ được event muộn.
- Tombstone thay vì xoá hẳn hàng trong Silver: giữ `ticket_id` + `_lsn` để replay batch cũ không hồi sinh ticket và downstream thấy `is_deleted` mà lan xoá; đổi lại hàng (đã hết PII) nằm lại mãi.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: model train trên `v2026-08-12` luôn tái lập được đúng dữ liệu đó; feedback muộn tạo version mới.
- DuckDB / dbt cho bài toán cỡ này, chứ không phải Spark: vài chục bản ghi/ngày chạy trong mili-giây trên một máy; Spark chỉ thêm chi phí cluster.

## 4. Hai câu hỏi suy ngẫm

1. **Quyền xoá thắng**; "bất biến" hiểu ở mức quy trình, không ở mức byte. Dựng lại `v2026-08-12..14` không có T-97 thành version mới, ghi log yêu cầu xoá rồi purge bản cũ; lineage snapshot → model cho biết model nào cần train lại. Bronze dùng crypto-shredding: mã hoá PII theo khoá từng user, xoá khoá là xoá mọi bản sao.
2. **Chốt ở Bronze → Silver**, trước chunk/embed/snapshot: NER tiếng Việt + đối chiếu tên khách hàng đã biết, thay bằng `<NAME>`, nghi ngờ thì quarantine. Đo precision/recall trên tập câu gán nhãn tay (có/không dấu), ưu tiên recall, kèm contract verify: PII sau Silver = 0.

## 5. Output (dán nguyên văn)

Chạy trên Windows PowerShell, Python 3.11.7, dbt-core 1.12.5, dbt-duckdb 1.11.0 (lệnh tương đương theo [SUBMISSION.md](../docs/SUBMISSION.md)).

```text
PS> .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt
```

```text
PS> .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 3.76s
```

```text
PS> .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
```

```text
PS> .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
```

```text
PS> .\.venv\Scripts\python.exe main.py --land-only; cd dbt_project; ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
05:37:15  Running with dbt=1.12.5
05:37:16  Registered adapter: duckdb=1.11.0
05:37:16  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
05:37:16  
05:37:16  Concurrency: 1 threads (target='dev')
05:37:16  
05:37:17  1 of 19 START sql view model main.stg_events ................................... [RUN]
05:37:17  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.10s]
05:37:17  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
05:37:17  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.05s]
05:37:17  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
05:37:17  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.18s]
05:37:17  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
05:37:17  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.15s]
05:37:17  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
05:37:17  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.22s]
05:37:17  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
05:37:17  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.07s]
05:37:17  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
05:37:17  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.04s]
05:37:17  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
05:37:17  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.02s]
05:37:18  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
05:37:18  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.05s]
05:37:18  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
05:37:18  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.04s]
05:37:18  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
05:37:18  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.06s]
05:37:18  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
05:37:18  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.04s]
05:37:18  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
05:37:18  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.03s]
05:37:18  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
05:37:18  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.04s]
05:37:18  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
05:37:18  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.02s]
05:37:18  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
05:37:18  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
05:37:18  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.06s]
05:37:18  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
05:37:18  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.05s]
05:37:18  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
05:37:18  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.07s]
05:37:18  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
05:37:18  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.06s]
05:37:18  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
05:37:18  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.06s]
05:37:18  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
05:37:18  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.05s]
05:37:18  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
05:37:18  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.06s]
05:37:18  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.47s]
05:37:18  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
05:37:18  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.05s]
05:37:18  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
05:37:18  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.04s]
05:37:18  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
05:37:18  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.04s]
05:37:18  
05:37:18  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.96 seconds (1.96s).
05:37:19  
05:37:19  Completed successfully
05:37:19  
05:37:19  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
```

```text
PS> .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
