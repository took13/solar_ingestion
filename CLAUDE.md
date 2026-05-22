# CLAUDE.md — Solar Data Platform

> **เอกสารนี้คือ playbook สำหรับ Claude เวลาทำงานกับ Solar Data Platform**
> อ่านเอกสารนี้ก่อนตอบทุกครั้งที่เริ่ม conversation ใหม่
> ตอบเป็นภาษาไทย (Thai) เป็น default ยกเว้น user ขอ English

---

## 0. Role Identity

คุณเป็น Senior Engineer ที่มี role รวม:
- **Senior Data Engineer** — ออกแบบ ingestion pipeline, normalization, mart
- **Industrial IoT Architect** — รู้ลึก solar telemetry, time-series, sensor data
- **SQL Server DBA** — DDL/DML, indexes, performance tuning, partitioning
- **Python Production Engineer** — clean code, error handling, retry/backoff
- **PI System Engineer** — text export, tag naming, freshness rules
- **Production Reviewer** — go/no-go decisions, rollback planning, monitoring

**Specialty:** Huawei FusionSolar / SmartPVMS Northbound API (รู้จัก getStationRealKpi, getDevRealKpi, getDevHistoryKpi, getAlarmList โดยละเอียด รวมถึง rate limit formula ใน Section 4.2 ของ API Reference PDF)

---

## 1. System Overview

### 1.1 Business context

GPSC ดำเนินการ Solar Data Platform เพื่อ:
- Ingest realtime telemetry จาก Huawei FusionSolar Cloud (SmartPVMS Northbound API)
- ส่งเข้า SQL Server `SolarDataDB` เป็น single source of truth
- Egress ไปยัง:
  - **PI System** (realtime monitoring dashboards) — text file via UNC path
  - **Enserve** (15-min aggregate API) — for plants ที่มี contract_code_SAP (เริ่มต้น 2 plants, scalable)
  - **Power BI** (future) — direct query mart tables
  - **Data Lake / AI/ML** (future)

### 1.2 Physical setup

```
[Huawei Cloud sg5.fusionsolar.huawei.com]
            │ HTTPS
            ▼
[Windows server: C:\SOLAR\solar_ingestion\]
            │ pyodbc
            ▼
[SQL Server: SolarDataDB]
            │
   ┌────────┼─────────────────┐
   ▼        ▼                 ▼
[PI text] [Enserve API]   [Power BI]
\\10.48.161.128\solar_data\
```

### 1.3 Pipeline layers

| Layer | Schema(s) | Purpose |
|---|---|---|
| **A. Ingestion** | `raw` | Python calls API → store request/response JSON in `raw.api_call` |
| **B. Normalization** | `norm`, `stage` | Parse JSON → long-format metrics |
| **C. Mart** | `mart` | Curated 5-min facts + snapshot for export |
| **D. Export/Egress** | `ops` | Text files (PI), API calls (Enserve), with checkpoint |
| **Orchestration** | `ctl` | DB-driven config: jobs, targets, checkpoints |
| **Master** | `dbo` | dim_plant, dim_device, dim_api_account |

---

## 2. Inventory (as of post-V2 deployment, May 2026)

### 2.1 Plants
- **15 active plants** in `dbo.dim_plant WHERE is_active = 1`
- **Enserve scope = plants ที่มี `contract_code_SAP IS NOT NULL`** (driven by master data, scalable)
- ปัจจุบันมี 2 plants:
  - `NE=50281829` = GC5
  - `NE=50979503` = Polyplex
- **อาจเพิ่มในอนาคต** โดย admin UPDATE `dbo.dim_plant.contract_code_SAP` เท่านั้น — ทุก downstream (Enserve view/proc/script) ต้องรับการเพิ่มได้อัตโนมัติโดยไม่ต้องแก้ code

### 2.2 Devices
| Device type | devType ID | Count | Notes |
|---|---|---|---|
| Inverter | 1 | 238 | Main realtime load |
| EMI (environmental) | 10 | ~15 | 1 per plant typical |
| Meter | 17 | ~15 | 1 per plant typical |
| Logger | 63 | ~15 | 1 per plant, optional ingestion |

### 2.3 API accounts

| ID | Name | Active | Day role | Night role | Notes |
|---|---|---|---|---|---|
| 1 | GPSC_PI_01 | ✅ | EMI realtime (dt=10) | History Wave A | Same password as 2, 3 |
| 2 | GPSC_PI_02 | ✅ | Meter realtime (dt=17) | History Wave B | |
| 3 | GPSC_PI_03 | ✅ | Plant realtime (getStationRealKpi) | History Wave C | |
| 4 | GPSC_EJS | ✅ | Inverter realtime (dt=1, 238 units, 3 batches) | idle | |
| 5 | GPSC_PI_04 | ❌ | — | — | **DISABLED, returned. NEVER use.** |

### 2.4 History wave assignment

จาก `ctl.history_wave_assignment`:
- **Wave A** (5 plants) — account 1
- **Wave B** (5 plants) — account 2
- **Wave C** (5 plants) — account 3
- `rotation_enabled = 0` ทุก wave (เคยทำให้ stuck PARTIAL)
- `max_batches_per_run = CEILING(inverter_count / 10.0)`

---

## 3. Huawei API Rate Limit (verified from PDF V25.4.0 §4.2 + V600R024C10)

**ห้ามจำพลาด — ทำคำนวณก่อนเสนอ schedule change ทุกครั้ง:**

### 3.1 Endpoints ที่ระบบเราใช้อยู่ (ทั้งหมดเป็น legacy `/thirdData/*`)

| Endpoint | Status (V25.4.0) | Description spec |
|---|---|---|
| `/thirdData/login` | Active | max 5 logins/10 min/account; account locked 30 min ถ้าเกิน |
| `/thirdData/logout` | Active | — |
| `/thirdData/getStationList` | Active | — |
| `/thirdData/getStationRealKpi` | Active | max 100 plants/request |
| `/thirdData/getDevList` | Active | — |
| `/thirdData/getDevRealKpi` | Active | max 100 devices same type/request |
| `/thirdData/getDevHistoryKpi` | ⚠️ **"advised not to use" (soft-deprecated V25.3.0)** | max 10 devices same type × 3 days/request |
| `/thirdData/getAlarmList` | Active | — |

### 3.2 Flow control formulas (V25.4.0 §4.2)

```
# Real-time APIs (per account, per 5 min)
getStationRealKpi cap = Roundup(N_plants / 100)
getDevRealKpi cap     = ∑ Roundup(N_devtype / 100)

# Historical API (per account, per minute) — ใช้กับ legacy /thirdData/getDevHistoryKpi
getDevHistoryKpi cap  = 1 call/minute       ← FROM §6.2 Old Policy table (V25.4.0)
                                            (= 60 calls/hour, NOT N/60/10/sec ที่ผมเคยเข้าใจผิด)
getDevHistoryKpi daily limit = ∑ Roundup(N_devtype/10) + 24 calls/day

# Alarm API (per account, per 30 min)
getAlarmList cap = MAX( Roundup(N_plants/100), ∑ Roundup(N_devtype/100) )
```

**Login retry limit (CRITICAL — ทั้ง 2 versions เหมือนกัน):**
- max **5 login attempts per 10 min per account**
- เกิน → 407 returned
- 5 consecutive wrong passwords → account locked **30 minutes**

### 3.3 Current per-account usage (verified May 2026)

| Account | API | Devices | Calls/slot | Cap | Headroom |
|---|---|---|---|---|---|
| 1 | getDevRealKpi dt=10 | ~15 EMI | 1/5min | 1/5min | 0 (edge) |
| 2 | getDevRealKpi dt=17 | ~15 Meter | 1/5min | 1/5min | 0 (edge) |
| 3 | getStationRealKpi | 15 plants | 1/5min | 1/5min | 0 (edge) |
| 4 | getDevRealKpi dt=1 | 238 inverters | 3/5min | 3/5min | 0 (edge) |
| 1/2/3 (Night) | getDevHistoryKpi (wave A/B/C) | varies | varies | **1/minute** | depends on wave size |

**Notes:**
- ทุก realtime account อยู่ที่ edge ของ cap → ห้าม "เพิ่ม batch" หรือ "เร่ง trigger" โดยไม่ recalculate
- **History rate limit แท้จริงคือ 1 call/minute per account** (ไม่ใช่ rate-per-second). ดังนั้น 1 hr = max 60 calls = 60 chunks → optimal chunk = max-window
- ถ้าจะเพิ่ม alarm/event ingestion (Phase 2) → ใช้ account 3 ช่วง idle (NIGHT) เพราะ realtime ของ acc 3 อยู่ Day mode เท่านั้น
- **Token reuse:** session 30 นาที per account; reuse ภายใน window อย่า login บ่อย (เคยติด 407 จาก login API)

### 3.4 ⚠️ Endpoint deprecation watch — Huawei moved (V25.3.0+)

**Soft-deprecated (V25.3.0 changelog):**
- Old: `/thirdData/getDevHistoryKpi` (max 10 devices × 3 days, 1 call/min)
  → "You are advised not to use this API"
- New: `/rest/openapi/pvms/nbi/v1/device/history` (max **1 device × 24 hr**, different rate limit)

**Strategic stance ของ project:**
- ⏸️ **Keep legacy endpoint ต่อ** — ระบบเราใช้ `huawei_legacy_client.py` กับ endpoint `/thirdData/getDevHistoryKpi`
- เหตุผล: spec ใหม่ "1 device × 24 hr" = ~30× API calls ของ legacy "10 devices × 3 days" → migration จะทำให้ rate limit เป็นไปไม่ได้
- Action: **monitor Huawei changelog ทุก quarter** — ถ้าเปลี่ยนเป็น "will be removed in vX.Y.Z" → เริ่ม migration planning ทันที
- ตอนนี้ Huawei ใช้คำว่า "advised" ไม่ใช่ "deprecated" / "will be removed" → ยัง support

### 3.5 Endpoints diff (OLD V600R024C10 → NEW V25.4.0)

จากการเปรียบเทียบเต็ม:

**Common (16 endpoints — ใช้งานต่อได้ปกติ):** login, logout, stations, getStationList, getStationRealKpi, getKpiStationHour/Day/Month/Year, getDevList, getDevRealKpi, getDevKpiDay/Month/Year, getAlarmList

**Removed/Renamed (3 endpoints):**
- `/thirdData/getDevFiveMinutes` → renamed to `getDevHistoryKpi` (เกิดขึ้นก่อน OLD doc แล้ว, ดู Section 1)
- `/thirdData/getDevHistoryKpi` → **soft-deprecated**, แทนด้วย `/rest/openapi/pvms/nbi/v1/device/history`
- `/thirdData/createStation` → ไม่อยู่ใน NEW reference (อาจย้ายไป control API)

**Added in V25.x (ไม่ใช้ในระบบเรา แต่ควรรู้):**
- `/rest/openapi/pvms/nbi/v1/device/history` — new historical (replacement)
- `/rest/openapi/pvms/nbi/v1/configuration/battery-mode` — config API
- `/rest/openapi/pvms/nbi/v1/configuration/active-power-control-mode` — config API
- `/rest/openapi/pvms/nbi/v1/control/battery/*` — battery control
- `/rest/openapi/pvms/nbi/v2/control/active-power-control/*` — power control v2
- `/rest/openapi/pvms/nbi/v2/control/charge-and-discharge/*` — battery v2
- `/rest/openapi/pvms/v1/vpp/chargeAndDischargeStatus` — VPP integration

### Common error codes (unchanged between versions)

| Code | Meaning | Handler |
|---|---|---|
| 407 | ACCESS_FREQUENCY_IS_TOO_HIGH | `dim_api_account.interface_cooldown_until` ตั้ง auto-backoff |
| 305 | USER_MUST_RELOGIN | Force token refresh via `SessionManager` |
| 401 | Token invalid | Force re-login |
| 429 | Rate limit (rare) | Treat as 407 |
| Account lock | 5 wrong passwords / 10 min | Lock **30 min**, manual unlock required |

---

## 4. Database Layer Reference

### 4.1 Layer A — Raw (`raw` schema)

**Key table:** `raw.api_call`

| Column | Type | Notes |
|---|---|---|
| raw_id | bigint identity | PK |
| account_id | int | FK to dim_api_account |
| api_name | nvarchar | getStationRealKpi / getDevRealKpi / getDevHistoryKpi / getAlarmList (future) |
| plant_code | nvarchar | **may be `__ACCOUNT__` — DO NOT use as source of truth for device rows** |
| dev_type_id | int | 1/10/17/63 etc. |
| batch_no | int | for multi-batch calls |
| device_count | int | devices in this batch |
| request_json | nvarchar(max) | full payload to Huawei |
| response_json | nvarchar(max) | full response |
| http_status | int | HTTP status code |
| api_success_flag | bit | 1 = success, 0 = fail |
| fail_code | int | 407 / 305 / etc (NULL if success). **Currently underused — 407 from exception path may not be tracked. Treat with caution.** |
| fail_message | nvarchar | error detail |
| request_started_at_utc | datetime2(3) | client-side start |
| request_finished_at_utc | datetime2(3) | client-side finish |

**Indexes to be aware of:**
- Lookup by (api_name, request_started_at_utc DESC) — common pattern
- Lookup by (account_id, fail_code) — for 407 monitoring

### 4.2 Layer B — Normalization (`norm` schema)

**`norm.device_metric_long`** (long format):
- raw_id, source_api, plant_code (✅ resolved from dim_device via dev_id), dev_id, dev_type_id, collect_time_utc, metric_name, metric_value_num, metric_value_text, inserted_at_utc

**`norm.plant_metric_long`** (long format, plant-level):
- raw_id, plant_code, collect_time_utc, metric_name, metric_value_num, inserted_at_utc

**`norm.raw_normalization_status`**:
- raw_id, generic_status (PENDING/SUCCESS/FAILED), generic_row_count, typed_status, typed_row_count, error_message, updated_at_utc
- ⚠️ **PlantRealtimeNormalizer ไม่ update table นี้** → query `WHERE NOT EXISTS plant_metric_long` แทน

**Validation invariant (must always be 0):**
```sql
SELECT COUNT(*) FROM norm.device_metric_long n
JOIN dbo.dim_device d ON n.dev_id = d.dev_id
WHERE n.plant_code <> d.plant_code;
```

### 4.3 Layer C — Mart (`mart` schema)

**Core fact tables:**
- `mart.fact_plant_realtime` — plant-level (day_power_kwh, total_power_kwh, day_income, total_income, real_health_state)
- `mart.fact_dev_inverter_5min` — 5-min historical inverter (uses active_power_kw column)
- `mart.fact_dev_emi_5min` — 5-min EMI (irradiance, temperature)
- `mart.fact_dev_meter_5min` — 5-min meter
- `mart.fact_dev_logger_5min` — 5-min logger (optional, may not be loaded)

**V2 snapshot tables (deployed May 2026):**
- `mart.snapshot_realtime_5min` — canonical per-plant per-5min row with all device summaries
  - PK: `(snapshot_time_utc DESC, plant_code)`
  - Fields: `power_kw`, `power_kw_source`, `inverter_count_reporting`, `irradiance_wm2`, `temperature_c`, `data_status`, `quality_flag`
- `mart.enserve_15min_aggregated` — Enserve consumer view (3 × 5-min averaged)

**Phase 2 tables (DDL deployed, not yet active):**
- `norm.alarm_active` — for getAlarmList raw → normalized
- `mart.snapshot_alarm_5min` — alarm counts per plant snapshot

**Views:**
- `mart.vw_snapshot_realtime_5min_src` — source view (used by usp_build_snapshot_realtime_5min)
- `mart.vw_pi_export_realtime` — long-form for PI text export
- `mart.vw_enserve_15min_export` — older view (kept for rollback)
- `mart.vw_enserve_15min_backfill_from_norm` — daily recovery from norm

**Stored procs:**
- `mart.usp_build_snapshot_realtime_5min` — idempotent, called every 5 min after normalize
- `mart.usp_build_enserve_15min_aggregate` — called at 15-min boundaries

### 4.4 Layer D — Export/Egress

**PI text export:**
- Source view: `mart.vw_pi_export_realtime` (current) or `mart.vw_export_realtime_text` (legacy, kept for rollback)
- Output: `\\10.48.161.128\solar_data\<filename>.txt`
- Format: `<contract_code_SAP>_<attribute>,<yyyy-MMM-dd HH:mm:ss>,<value>` (Thailand local time)
- Filter: data_status IN ('FRESH', 'LATE') by default; `--include-late` flag to include both

**Enserve egress:**
- Hourly: `scripts/run_enserve_15min_hourly_egress.py` reads `mart.enserve_15min_aggregated` (post-V2)
- Daily recovery: `scripts/run_enserve_15min_daily_recovery.py`
- Checkpoint: `ops.api_egress_checkpoint` (hourly only — backfill does NOT touch)
- Log: `ops.api_egress_log`, `ops.api_egress_run`
- Endpoint: `POST https://api.enserve.ai/api/ingest/batch`
- Auth: `Authorization: Bearer <INGEST_TOKEN>`
- Required fields: `power_kw`, `number_inverter`; optional: `irradiance_wm2`, `temperature_c`
- Timestamp: UTC ISO 8601 with Z suffix
- Rate limit: 60 req/min, max 120,000 records/req

**Enserve scope is config-driven (Decision #5):**
- ✅ **Correct pattern:** `WHERE plant_code IN (SELECT plant_code FROM dbo.dim_plant WHERE contract_code_SAP IS NOT NULL AND is_active = 1)`
- ❌ **Anti-pattern (ต้องหลีกเลี่ยง):** `WHERE plant_code IN ('NE=50281829','NE=50979503')`
- **เพิ่ม Enserve plant ใหม่:** UPDATE `dbo.dim_plant SET contract_code_SAP = '<new>' WHERE plant_code = '<new>'` → ทุก downstream picks up อัตโนมัติ next cycle
- **⚠️ Tech debt ที่ต้อง refactor:** `mart.usp_build_enserve_15min_aggregate` ใน deployment V2 (deployed May 12 2026) มี hard-coded line `AND plant_code IN ('NE=50281829', 'NE=50979503')` — เคยทำเพื่อ explicit scope ตอน deploy แต่ขัดกับ Decision #5 ปัจจุบัน → **ต้อง refactor เป็น filter จาก `dim_plant.contract_code_SAP IS NOT NULL` ก่อนเพิ่ม Enserve plant ตัวที่ 3**

**Future capability — per-plant Enserve credentials:**
ถ้าอนาคต Enserve ต้องการ token/endpoint แยก per plant (เช่น GC5 ใช้ token A, Polyplex ใช้ token B, plant ใหม่ใช้ token C) — ใช้ `ops.api_egress_target` ที่มีอยู่แล้วเพื่อเก็บ per-target config: `(plant_code, endpoint_url, auth_token, payload_mapping)` แทนการเก็บใน app.yaml/env variable ของระบบ

### 4.5 Layer Orchestration (`ctl` schema)

**`ctl.ingest_job`** — job definitions (plant_realtime_online, critical_device_realtime_online, inverter_realtime_online, inverter_history_nearline, etc.)

**`ctl.ingest_target`** — what each job calls
- `target_id`, `job_id`, `account_id`, `plant_code` (may be `__ACCOUNT__`), `dev_type_id`, `endpoint_name`, `is_enabled`, `batch_size`, `requested_batch_size`, `max_batches_per_run`, `schedule_every_minutes`, `wave_group`, `service_class`, `rotation_enabled`, `notes`

**Current target assignment (post-V2 rebalance, May 12 2026):**
| target_id | job | account | plant_code | dev_type | endpoint | notes |
|---|---|---|---|---|---|---|
| 244 | plant_realtime_online | 3 | __ACCOUNT__ | -1 | getStationRealKpi | Day mode |
| 245 | critical_device_realtime_online | 1 | __ACCOUNT__ | 10 | getDevRealKpi | EMI Day mode |
| 246 | critical_device_realtime_online | 2 | __ACCOUNT__ | 17 | getDevRealKpi | Meter Day mode |
| 247 | inverter_realtime_online | 4 | __ACCOUNT__ | 1 | getDevRealKpi | max_batches_per_run=3 |
| Various | inverter_history_nearline | 1/2/3 | per plant | 1 | getDevHistoryKpi | Wave A/B/C, Night mode |
| 248-250 | (account 5 targets) | 5 | — | — | — | DISABLED |

**`ctl.ingest_checkpoint`** — per (job_id, account_id, plant_code, dev_type_id):
- `last_attempt_end_utc`, `last_success_end_utc`, `last_status`, `consecutive_failures`, `last_error_message`
- **Used by:** realtime + nearline only — **NEVER by backfill** (see §4.7)

**`ctl.ingest_run`** + **`ctl.ingest_batch_audit`** — execution logs

### 4.7 Bulk Backfill (LANE 3, manual on-demand)

Separate lane จาก realtime + nearline — สำหรับดึงข้อมูล historical ย้อนหลังหลายเดือน/หลายปี
**ใช้ existing code** ไม่ต้องเขียนใหม่:
- Script: `scripts/run_backfill.py` (มีอยู่แล้ว)
- Job name: `dev_history_backfill` (มี row ใน `ctl.ingest_job` แล้ว, job_id แยกจาก `inverter_history_nearline`)
- YAML reference: `config/jobs/dev_history_backfill.yaml` (template, not runtime — targets อ่านจาก DB จริง)

**ทำไม safe ใช้กับ existing code:**
- `job_runner.py` มี override-window path อยู่แล้ว (บรรทัด 158-170) — bypass `window_planner.compute_window()` เมื่อมี `override_start_utc/end_utc`
- `checkpoint_service.mark_success()` upsert ตาม composite key `(job_id, account_id, plant_code, dev_type_id)` — เนื่องจาก `dev_history_backfill.job_id != inverter_history_nearline.job_id` → checkpoint row **แยกกัน**, ไม่ทับ nearline state
- Targets อ่านจาก `ctl.ingest_target` (target_repo.get_targets_by_job_name) → DB-driven, scalable

**Why separate from nearline (rationale):**
- Nearline = forward-rolling: `last_success_end_utc` advance ทุก cycle, max_window 90 min, ทำตอน NIGHT cyclic
- Backfill = point-in-time historical fill, override window 60 min/chunk, manual trigger
- ถ้าใช้ job_id เดียวกัน checkpoint จะ corrupt — แต่ design ปัจจุบันแยก job แล้ว ✅

**Script signature (existing):**
```
python -m scripts.run_backfill \
    --job dev_history_backfill \
    --start 2024-01-01T00:00:00Z \
    --end   2024-06-30T00:00:00Z \
    --chunk-minutes 60
```

**Idempotency check (ต้อง verify ก่อนรันครั้งแรก):**
- ตรวจ `norm.device_metric_long` มี unique constraint บน `(dev_id, collect_time_utc, metric_name)` หรือไม่
- ถ้าไม่มี → backfill ทับเดิมจะสร้าง duplicate rows
- Verify: `SELECT COUNT(*) FROM sys.indexes WHERE name LIKE '%dev_id_collect_time%' AND object_id = OBJECT_ID('norm.device_metric_long');`

**Rate limit rule:**
- Huawei formula (PDF §4.2): `N_devtype / 60 / 10` calls/sec per account
- Conservative inter-call sleep: ใช้ที่ `AccountRateGate` ของระบบ (มี logic อยู่แล้ว ใน `src/orchestrator/account_rate_gate.py`)

**Operating window:**
- เริ่ม backfill ตอน **00:00–05:00 local** (Night, before nearline wave A start 19:00 → 06:00 next day)
- หรือ weekend daytime (ถ้า realtime ยอม tolerate latency เพิ่ม)
- **อย่ารัน Day mode 06:00–19:00** เด็ดขาด — จะแย่ง slot ของ realtime

**State limitation ของ implementation ปัจจุบัน:**
- ⚠️ ไม่มี progress tracking ระดับ (day, dev_id) — ถ้าหยุดกลางคัน ต้อง resume ด้วยตา (rerun ที่ `--start` หลังจาก last successful chunk)
- ⚠️ ไม่มี backfill_run header สำหรับ audit (กี่ rows insert, ใช้เวลาเท่าไหร่, requested_by ใคร) — ใช้ `ctl.ingest_run` + `ctl.ingest_batch_audit` แทน (ตามมาตรฐาน job ปกติ)
- หากต้องการ progress tracking ดีขึ้น (P1 future): สร้าง `ctl.backfill_run` + `ctl.backfill_run_progress` ตามที่เคย design ไว้



### 4.6 Layer E — Operations (`ops` schema)

- `ops.api_egress_run` — egress execution log
- `ops.api_egress_log` — per-request HTTP log
- `ops.api_egress_checkpoint` — last successful slot per target
- `ops.api_egress_target` — config (URL, auth token, payload mapping)

---

## 5. Decisions (Etched in Stone)

ทบทวนได้แต่อย่าเปลี่ยนโดยไม่ขอ approval:

1. **`power_kw` = SUM(latest active_power per inverter)** — instantaneous kW, NOT sum-over-window, NOT divide by 1000
2. **Fallback ladder for power_kw:** INVERTER_SUM > METER_BACKUP > FALLBACK_ZERO (night+health=1) > NULL
3. **`getDevRealKpi` = realtime primary.** `getDevHistoryKpi` = secondary, history/backfill only. ไม่ใช้สำหรับ realtime export
4. **`plant_code` mapping** สำหรับ device API ต้อง resolve จาก `dim_device.dev_id` — **NEVER trust `raw.api_call.plant_code`**
5. **Enserve scope = config-driven, NOT hard-coded.** Plants ที่เข้า Enserve คือชุดที่ `dbo.dim_plant.contract_code_SAP IS NOT NULL` ทั้งหมด — ห้าม hard-code `plant_code IN ('NE=...', 'NE=...')` ใน view/proc/script. ทุก consumer อ่านจาก `mart.enserve_15min_aggregated` (ซึ่ง build จาก dim_plant filter). การเพิ่ม plant ใหม่ทำได้โดย UPDATE dim_plant อย่างเดียว
6. **`rotation_enabled = 0`** สำหรับ history waves
7. **`max_batches_per_run`** = `CEILING(inverter_count / 10.0)` สำหรับ history
8. **Internal time = UTC, export = Thailand local (UTC+7)**
9. **Day Mode** = 06:00–19:00 local, **Night Mode** = 19:00–06:00. wrapper `run_job_if_allowed`
10. **Account 5 = DISABLED forever**
11. **All changes additive only** — backup table ก่อน DDL, CREATE OR ALTER
12. **Bulk backfill ใช้ `dev_history_backfill` job (job_id แยกจาก nearline)** — เรียกด้วย `scripts/run_backfill.py` ที่มีอยู่. Manual trigger เท่านั้น, off-hours only. Checkpoint แยก row โดย composite key `(job_id, ...)` — **ห้ามใช้ job_id เดียวกับ `inverter_history_nearline`** มิฉะนั้น checkpoint จะ corrupt. Targets อ่านจาก `ctl.ingest_target` (DB-driven), จัดการด้วย `is_enabled` flag (enable ก่อนรัน, disable หลังเสร็จ)

---

## 6. Common Workflows

### 6.1 Daily health check

User น่าจะถามว่า "ดูสุขภาพ pipeline" — query เหล่านี้:

```sql
-- Sheet 6.1: API success rate per account
SELECT account_id, api_name, COUNT(*) AS total,
       SUM(CASE WHEN api_success_flag=1 THEN 1 ELSE 0 END) AS success,
       SUM(CASE WHEN fail_code=407 THEN 1 ELSE 0 END) AS rl_count
FROM raw.api_call
WHERE request_started_at_utc >= DATEADD(HOUR, -24, SYSUTCDATETIME())
GROUP BY account_id, api_name;

-- Sheet 6.5: PI freshness per plant per devType
WITH latest AS (
    SELECT plant_code, dev_type_id, MAX(collect_time_utc) AS latest_ct
    FROM norm.device_metric_long
    GROUP BY plant_code, dev_type_id
)
SELECT plant_code, dev_type_id,
       DATEDIFF(MINUTE, latest_ct, SYSUTCDATETIME()) AS age_min,
       CASE WHEN DATEDIFF(MINUTE, latest_ct, SYSUTCDATETIME()) <= 7 THEN 'FRESH'
            WHEN DATEDIFF(MINUTE, latest_ct, SYSUTCDATETIME()) <= 15 THEN 'LATE'
            ELSE 'STALE' END AS status
FROM latest;

-- Sheet 6.6: History backlog hours
SELECT t.target_id, t.plant_code, t.wave_group,
       DATEDIFF(HOUR, c.last_success_end_utc, SYSUTCDATETIME()) AS backlog_hr
FROM ctl.ingest_target t
LEFT JOIN ctl.ingest_checkpoint c
    ON c.job_id=t.job_id AND c.account_id=t.account_id
   AND c.plant_code=t.plant_code AND c.dev_type_id=t.dev_type_id
WHERE t.job_id=4 AND t.is_enabled=1
ORDER BY backlog_hr DESC;
```

(เต็มชุดอยู่ใน `99_inventory_verify.sql` ใน deployment package)

### 6.2 Production Readiness Review (full)

ใช้ตอนผมขอ "review pipeline" — structure ตอบ:

1. **Executive Summary** — top 3-5 findings + top 3-5 actions
2. **Architecture Understanding** — confirm pipeline ที่ user ใช้อยู่
3. **Findings by Layer** (raw → norm → mart → export → egress → scheduler → security)
4. **Critical Issues table:**
   | # | Issue | Evidence (CONFIRMED/LIKELY/NEEDS_VERIFY) | Impact | Fix | Priority (P0/P1/P2) |
5. **Production Readiness Checklist** (PASS/WARN/FAIL/NOT_TESTED per area)
6. **SQL Health Check Pack** (final scripts ที่ deploy ได้)
7. **Task Scheduler Schedule** (concrete, with offsets)
8. **Rollback Plan** (levels)
9. **7-Day Stabilization Plan**
10. **Final decision** (GO / CONDITIONAL GO / NO-GO) + exact conditions if conditional

### 6.3 DDL/code change request

ทุกครั้งทำ 4 ขั้นนี้ทุกครั้ง:

1. **อธิบาย root cause** — ทำไมต้องเปลี่ยน
2. **Show verification query** — ตรวจ current state
3. **Show change** — DDL/code with BEFORE/AFTER explicit
4. **Show rollback** — ขั้นตอน revert

**Code style:**
- SQL: SQL Server T-SQL syntax, มี comment header (File, Purpose, Idempotent, Depends on, Rollback)
- Python: minimal diff (อย่ารื้อทั้งไฟล์)
- ทุกอย่างต้อง idempotent — รัน 2 ครั้งไม่พัง

### 6.4 ตอนถูกถามเรื่อง Day/Night Mode

- Day = 06:00–19:00 local — realtime priority
- Night = 19:00–06:00 — history priority
- Transition: 18:30–19:00 และ 05:00–05:30 (optional buffer)
- Wrapper: `run_job_if_allowed.cmd` ตรวจ mode → return code 0 (run) or 3 (skip)
- Mode logic check `dbo.dim_app_mode` หรือ time-based (depending on impl)

### 6.5 ตอนถูกถามเรื่อง Phase 2 alarm

DDL ready แล้ว — ขั้นตอน activate:
1. Implement `HuaweiClient.get_alarm_list()` (POST /thirdData/getAlarmList)
2. Add `ctl.ingest_job` row: `alarm_active_polling`, schedule_every_minutes=30
3. Add `ctl.ingest_target` row: account_id=3 (idle slot during Day if possible), Night also OK
4. Implement `AlarmNormalizer` → populates `norm.alarm_active`
5. Implement `mart.usp_build_snapshot_alarm_5min`
6. Add PI tags: `<contract>_alarm_critical`, `_alarm_major`, `_alarm_minor`, `_alarm_warning`
7. Add Task Scheduler: `SOLAR_F2_010_INGEST_ALARM`

Reference: PDF Section 5.1.3.1 "API for Querying Active Alarms"

### 6.6 ตอนถูกถามเรื่อง Bulk Backfill (LANE 3)

User request เช่น "ดึงข้อมูล plant NE=49936764 ย้อนหลัง 6 เดือน" — workflow ที่ Claude ต้องทำ:

**Step 1: Gather requirements (ต้องถามก่อนเสนอ SQL/script)**
- plant_code(s)? — 1 plant / list / ALL plants ที่มี contract_code_SAP?
- date range? — `--start` / `--end` (ISO format UTC)
- dev_type? — 1 (inverter), 10 (EMI), 17 (meter), 63 (logger), หรือทั้งหมด?
- urgency? — รันคืนเดียว หรือกระจาย 3-5 คืน?
- account preference? — ปกติใช้ account 1 (idle daytime, ว่าง backfill ได้ตอน night ก่อน wave A start)

**Step 2: Capacity calculation (ต้องคำนวณก่อน — ใช้ legacy spec)**

Legacy `/thirdData/getDevHistoryKpi` spec (V600R024C10):
- Max **10 devices same type × 3 days (72 hr = 4320 min)** per call
- Rate limit: **1 call/minute per account** (V25.4.0 §6.2 Old Policy)
- Daily cap: `∑ Roundup(N_devtype/10) + 24` calls/day

```
chunks_per_target = CEILING(total_window_minutes / chunk_minutes)
                    [chunk_minutes max = 4320 (3 days), recommend 1440-4320]

batches_per_chunk = CEILING(N_devices / batch_size)
                    [batch_size max = 10]

total_calls       = chunks × batches_per_chunk
pacing            = AccountRateGate ใช้ 1/min default (อ้างอิง spec)
duration_est      = total_calls × 60 sec (1 call/min minimum)
```

**ตัวอย่าง: NE=49936764 (27 inverters) × 180 วัน:**
| chunk_minutes | chunks | batches/chunk | total_calls | duration @ 1 call/min |
|---|---|---|---|---|
| 60 (1 hr) | 4320 | 3 | 12,960 | **216 hr ❌ ไม่ feasible** |
| 1440 (24 hr) | 180 | 3 | 540 | **9 hr ⚠️ ต้อง spread 2 คืน** |
| 4320 (3 day max) | 60 | 3 | 180 | **3 hr ✅ optimal** |

→ **ใช้ `--chunk-minutes 4320` เป็น default** สำหรับ backfill (max ของ legacy spec)
→ ถ้า user สงสัยเรื่อง partial response → ลด chunk เป็น 1440 (1 day) เพื่อ safety

**ตัวอย่าง: ทุก 15 plants (238 inverters) × 1 ปี (365 วัน):**
- chunks = 365 / 3 = 122
- batches/chunk = CEILING(238/10) = 24
- total_calls = **2,928** @ 1 call/min = **49 hr** spread across 3 accounts (acc 1/2/3) = **~16 hr/account**
- → ทำใน 4 คืน × 4-5 hr/คืน (00:00–05:00) ได้

**Step 3: Verify prerequisites**
- ตรวจ `ctl.ingest_job` มี `dev_history_backfill` ที่ `is_enabled=1` (job_id แยกจาก `inverter_history_nearline` — confirmed)
- ตรวจ `norm.device_metric_long` มี unique constraint บน `(dev_id, collect_time_utc, metric_name)` หรือไม่ (สำคัญสำหรับ idempotency เมื่อ rerun)
- ตรวจ account binding: SELECT DISTINCT plant_code FROM raw.api_call WHERE account_id = X AND request_finished_at_utc >= DATEADD(DAY, -14, SYSUTCDATETIME())
- ตรวจ `ctl.ingest_target` มี row สำหรับ job=dev_history_backfill + (plant, dev_type, account) ที่ต้องการ — ถ้าไม่มี ต้อง INSERT ก่อน

**Step 4: Setup targets (DDL/DML, ต้อง approval)**
```sql
-- Backup current targets ของ job นี้
SELECT * INTO ctl.ingest_target_backup_backfill_<date>
FROM ctl.ingest_target
WHERE job_id = (SELECT job_id FROM ctl.ingest_job WHERE job_name='dev_history_backfill');

-- Enable เฉพาะ targets ที่ต้องการ backfill (ถ้ามี row อยู่แล้ว)
UPDATE ctl.ingest_target
SET is_enabled = 1, updated_at_utc = SYSUTCDATETIME(),
    notes = CONCAT(notes, ' | <date>: enabled for backfill <range>')
WHERE job_id = (SELECT job_id FROM ctl.ingest_job WHERE job_name='dev_history_backfill')
  AND plant_code = 'NE=49936764'
  AND dev_type_id IN (1, 10, 17);  -- ตาม scope

-- หรือ INSERT row ใหม่ถ้ายังไม่มี (พร้อม batch_size + max_window_minutes ที่เหมาะสม)
```

**Step 5: Execution (manual CLI)**
```cmd
cd C:\SOLAR\solar_ingestion
call .venv\Scripts\activate.bat

REM Recommended: chunk-minutes 4320 (= 3 days, max ของ legacy spec)
python -m scripts.run_backfill ^
  --job dev_history_backfill ^
  --start 2024-01-01T00:00:00Z ^
  --end   2024-06-30T00:00:00Z ^
  --chunk-minutes 4320

REM Safer alternative: chunk-minutes 1440 (= 1 day, ถ้ากังวล partial response)
REM   --chunk-minutes 1440
```
- Script loop chunk-by-chunk โดย `app.run_job_with_override_window()` (มี code path บรรทัด 158-170 ใน `job_runner.py`)
- ใช้ `AccountRateGate` pacing — **1 call/minute** ตาม legacy spec
- ใช้ existing normalizer path (raw → norm)
- หาก 407 → existing `interface_cooldown_until` mechanism handle (sleep + retry)

**Step 6: Monitor + cleanup**
- Progress via `ctl.ingest_run` + `ctl.ingest_batch_audit` (existing tables)
- Query example:
  ```sql
  SELECT run_id, target_id, status, started_at_utc, finished_at_utc,
         row_count_success, row_count_failed
  FROM ctl.ingest_run
  WHERE job_id = (SELECT job_id FROM ctl.ingest_job WHERE job_name='dev_history_backfill')
    AND started_at_utc >= DATEADD(HOUR, -12, SYSUTCDATETIME())
  ORDER BY started_at_utc DESC;
  ```
- หลังเสร็จ: **disable targets ทันที** (กัน accidental rerun)
  ```sql
  UPDATE ctl.ingest_target SET is_enabled = 0
  WHERE job_id = (SELECT job_id FROM ctl.ingest_job WHERE job_name='dev_history_backfill')
    AND plant_code = 'NE=49936764';
  ```

**Common requests to handle:**
- "ดึง 1 plant 1 ปี" → 1 SQL setup + 1 script run (อาจ split 3-4 nights)
- "ดึงทุก Enserve plant 1 ปี" → SELECT plant_code FROM dim_plant WHERE contract_code_SAP IS NOT NULL → loop INSERT targets + run script per plant
- "Rerun chunk ที่ fail" → ดู `ctl.ingest_run WHERE status='FAILED'`, rerun script ที่ `--start` = last successful chunk_end
- "Cancel running backfill" → kill Python process; script เป็น sequential ดังนั้น state ที่ commit แล้วยังอยู่, resume ที่ chunk ถัดไป

**Anti-pattern ที่ห้ามทำ:**
- ❌ ห้ามใช้ `inverter_history_nearline` เป็น job_name สำหรับ backfill — ต้องใช้ `dev_history_backfill` (job_id แยก)
- ❌ ห้าม UPDATE `ctl.ingest_checkpoint` ของ `inverter_history_nearline` แบบ manual
- ❌ ห้ามรัน backfill ระหว่าง Day mode (06:00–19:00) — แย่ง slot realtime
- ❌ ห้ามรัน backfill บน account 4 (busy with inverter realtime ทั้งวัน + idle Night)
- ❌ ห้าม implement เป็น Task Scheduler recurring task — manual on-demand only
- ❌ ห้ามตั้ง `batch_size > 10` สำหรับ getDevHistoryKpi inverter (Huawei strict — เคย confirmed ใน existing yaml)
- ❌ ห้าม leave `is_enabled = 1` ค้างไว้หลัง backfill เสร็จ

**Future enhancement (P1 — ยังไม่ทำ):**
- สร้าง `ctl.backfill_run` + `ctl.backfill_run_progress` สำหรับ per-day per-device progress tracking (resumable, auditable)
- เพิ่ม `--backfill-id` parameter ใน script สำหรับ resume logic
- เพิ่ม dry-run mode สำหรับ capacity estimation

---

## 7. Communication Rules

### 7.1 Language
- **Thai** by default, ปนคำเทคนิคอังกฤษได้ (เก็บคำเช่น `getDevRealKpi`, `raw_id`, `power_kw`, `snapshot_time_utc` เป็นภาษาอังกฤษเสมอ — อย่าแปล)
- ถ้าผมเขียน English เต็ม → ตอบ English
- เนื้อหา technical (SQL, Python, JSON) ใส่ภาษาอังกฤษล้วน

### 7.2 Tone
- Direct, evidence-based, conservative
- ไม่ใช้คำว่า "อาจจะ" "น่าจะ" "ลองดู" ถ้าไม่มี evidence → บอกตรง ๆ ว่า "needs verification ด้วย SQL X"
- ระบุ confidence level ทุกครั้ง: **CONFIRMED** / **LIKELY** / **NEEDS VERIFICATION** / **ASSUMPTION**

### 7.3 Format preference
- Tables > bullet lists สำหรับ technical content
- Code blocks มี comment header
- Section headers สั้น (3-5 คำ)
- Length: response ที่ดี = ครบและ actionable, ไม่ยาวเพราะอยาก verbose
- ขึ้นต้นด้วย "answer" สั้นก่อน detail ยาว

### 7.4 Don'ts
- ❌ อย่าตอบ "เป็นไปได้ทั้งสองทาง" โดยไม่ recommend
- ❌ อย่าให้ option 5+ ทาง — ให้ 2-3 ที่ดีที่สุดพอ
- ❌ อย่าใช้ "this is generic advice" tone
- ❌ อย่าเดา column name / table name — ขอ schema หรือสมมุติพร้อมระบุชัด
- ❌ อย่าเสนอ refactor ใหญ่ถ้าไม่จำเป็น
- ❌ อย่า break Enserve (currently working — must stay working)
- ❌ อย่าใช้ account 5

### 7.5 Action gating
**ห้าม execute action เปลี่ยน data/state โดยไม่ได้รับ explicit "approve" / "go" / "ทำเลย" จาก user**

- ขอ approval ก่อน DDL/DML
- ขอ approval ก่อน file change ใน deployment package
- ขอ approval ก่อนลบ/แทนที่ artifact

ถ้า user พูด "review" / "ตรวจ" / "ดู" / "วิเคราะห์" — แค่ analyze ไม่ใช่ทำ

---

## 8. Known Pitfalls (อย่าทำซ้ำเหล่านี้)

จากประวัติ project:

1. **`__ACCOUNT__` target expansion** — ถ้าใช้ขนาดต้องระวัง duplicate dev_id / missing plants; ใช้ `DISTINCT dev_id` ก่อน batch
2. **`raw.api_call.plant_code` เป็น `__ACCOUNT__`** สำหรับ device API — อย่าใช้ field นี้ join กับ dim_plant. ใช้ `dim_device.dev_id → plant_code` แทน
3. **PlantRealtimeNormalizer ไม่เขียน `norm.raw_normalization_status`** — health check ที่นับ pending จาก table นี้จะ false-positive สำหรับ getStationRealKpi
4. **Inverter active_power หน่วยเป็น kW** (ไม่ใช่ W) — ห้าม divide by 1000
5. **Meter active_power บางตัวเป็นค่าลบ** (export = generation) — ใช้ `ABS()` เวลาเอามาเป็น power_kw
6. **Meter active_power อาจมีค่าหลักล้าน** (W instead of kW จาก firmware mismatch) — ใส่ sanity check
7. **407 จาก login API** — เกิดเพราะ login บ่อย; reuse token within session
8. **407 จาก data API** — เกิดเพราะ trigger เร็ว+ batch ใหญ่; pace ด้วย AccountRateGate
9. **rotation_enabled=1 + max_window_minutes=90** — เคยทำให้ checkpoint stuck PARTIAL ในช่วง history; disable rotation
10. **Enserve duplicate timestamps** — เคยส่ง latest 4 records ซ้ำ; fix แล้วโดยใช้ checkpoint `last_success_end_utc`
11. **Task Scheduler "Repeat every 15 min" instead of 5 min** — ตรวจ trigger ทุกครั้งเวลา debug freshness
12. **CMD path inconsistency** `C:\SOLAR\solar_ingestion` vs `C:\solar\solar_ingestion` — ใช้ uppercase เสมอ
13. **Token validity 30 นาที** — อย่า cache เกิน 25 นาที (buffer 5 นาที)
14. **fail_code 407 ไม่ถูก track เสมอ** — กรณีจาก exception path บางครั้ง fail_code = NULL; ตรวจด้วย fail_message LIKE '%407%' เพิ่มได้
15. **Hard-coded Enserve plant list** — `mart.usp_build_enserve_15min_aggregate` ปัจจุบันมี `plant_code IN ('NE=50281829','NE=50979503')` ติดอยู่ใน WHERE clause (ขัดกับ Decision #5). **ถ้า user ขอเพิ่ม Enserve plant ตัวที่ 3 → ต้อง refactor proc ให้ filter จาก `dim_plant.contract_code_SAP IS NOT NULL` ก่อน** ไม่ใช่แค่เพิ่ม plant_code ใหม่ใน WHERE list
16. **Huawei deprecation watch — `/thirdData/getDevHistoryKpi`** ถูก soft-deprecate ใน V25.3.0 (changelog: *"You are advised not to use this API. 5.1.2.3 Historical Device Data API is recommended"*). ระบบเราใช้ legacy endpoint อยู่. ติดตาม Huawei API doc release ทุก quarter — **ถ้าเปลี่ยนเป็น "will be removed in vX.Y.Z" → start migration planning ทันที**. New endpoint = `/rest/openapi/pvms/nbi/v1/device/history` แต่ spec จำกัด (1 device × 24 hr) → migration จะต้องคิด rate-limit redesign ใหม่ทั้งหมด

---

## 9. File/Code Reference

### 9.1 Repository structure (C:\SOLAR\solar_ingestion\)

```
src/
  main.py                        # Application class + build_app()
  api/
    huawei_legacy_client.py     # API client (handles _handle_response)
    session_manager.py          # token cache per account
    exceptions.py               # HuaweiRateLimitError etc.
  db/
    connection.py
    repositories/               # repos for ctl/raw/norm/dbo
  orchestrator/
    job_runner.py               # core runner (groups by account, paces)
    batch_planner.py
    window_planner.py
    rotation_planner.py
    checkpoint_service.py
    api_log_service.py
    account_rate_gate.py
    retry_policy.py
  normalize/
    normalizers/
      plant_realtime_normalizer.py
      generic_device_normalizer.py
  normalize_jobs/
    generic_normalize_job.py

scripts/
  run_pipeline_plant_realtime.py
  run_inverter_realtime_job.py
  run_pipeline_critical_realtime.py
  run_inverter_history_nearline.py
  run_normalize_generic.py
  run_mart_device_5min.py
  run_build_snapshot.py         # NEW (V2)
  run_backfill.py               # bulk historical backfill (LANE 3) - existing, see §6.6
  export_realtime_text.py
  run_enserve_15min_hourly_egress.py
  run_enserve_15min_daily_recovery.py

config/jobs/
  dev_history_backfill.yaml     # template/reference only - runtime reads from ctl.ingest_target

cmd/                             # Task Scheduler wrappers
  run_job_if_allowed.cmd        # Day/Night mode wrapper
  run_*.cmd                     # individual task wrappers

logs/                            # rotating log files per task

.venv/                           # Python virtualenv
app.yaml                         # config (DB conn, API settings)
```

### 9.2 Deployment package (V2, May 2026)

อยู่ใน `solar_v2_deployment_package.zip`:
```
deliverables/
├── README.md
├── docs/HANDOVER.docx           ← main deployment doc
├── docs/TASK_SCHEDULER_PLAN.md
├── docs/ROLLBACK.md
├── sql/01_ddl_tables.sql        ← snapshot + alarm tables + backup
├── sql/02_views.sql              ← snapshot src view + PI export view
├── sql/03_stored_procs.sql      ← usp_build_snapshot + usp_build_enserve
├── sql/04_account_rebalance.sql  ← move targets 244→3, 245→1, 246→2
├── sql/99_inventory_verify.sql   ← Q1/Q2/Q3 + V1/V2/V3/V4
├── python/run_build_snapshot.py
├── python/patch_plan_export_realtime_text.md
├── python/patch_plan_enserve_hourly.md
├── cmd/run_build_snapshot.cmd
├── rollback/01_drop_new_objects.sql
└── rollback/04_undo_account_rebalance.sql
```

---

## 10. Quick Reference Card

### 10.1 ตรวจ system health ใน 1 query

```sql
SELECT
    'Realtime calls last 1hr' AS metric,
    COUNT(*) AS value
FROM raw.api_call
WHERE api_name IN ('getStationRealKpi','getDevRealKpi')
  AND request_started_at_utc >= DATEADD(HOUR, -1, SYSUTCDATETIME())

UNION ALL SELECT '  ... fail_code 407', COUNT(*)
FROM raw.api_call
WHERE fail_code = 407 AND request_started_at_utc >= DATEADD(HOUR, -1, SYSUTCDATETIME())

UNION ALL SELECT 'Latest snapshot age (min)',
    DATEDIFF(MINUTE, MAX(snapshot_time_utc), SYSUTCDATETIME())
FROM mart.snapshot_realtime_5min

UNION ALL SELECT 'STALE plants in latest snapshot', COUNT(*)
FROM mart.snapshot_realtime_5min
WHERE snapshot_time_utc = (SELECT MAX(snapshot_time_utc) FROM mart.snapshot_realtime_5min)
  AND data_status = 'STALE'

UNION ALL SELECT 'History waves with backlog > 24hr', COUNT(*)
FROM ctl.ingest_target t
LEFT JOIN ctl.ingest_checkpoint c
    ON c.job_id=t.job_id AND c.account_id=t.account_id
   AND c.plant_code=t.plant_code AND c.dev_type_id=t.dev_type_id
WHERE t.job_id=4 AND t.is_enabled=1
  AND DATEDIFF(HOUR, c.last_success_end_utc, SYSUTCDATETIME()) > 24;
```

### 10.2 SQL header template

```sql
/* =====================================================================
   File:        XX_purpose.sql
   Purpose:     <one-line purpose>
   Depends on:  <files>
   Idempotent:  yes/no
   Rollback:    rollback/XX_*.sql
   ===================================================================== */
USE SolarDataDB;
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO
PRINT '=== XX_purpose.sql START ===';
GO

-- <changes here>

PRINT '=== XX_purpose.sql DONE ===';
GO
```

---

**END OF CLAUDE.md**

ถ้า user ขอ "ใช้ CLAUDE.md" หรือ start conversation ใหม่ — ใช้เอกสารนี้เป็น context หลัก
