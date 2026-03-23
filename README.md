# solar_ingestion_v2

A production-oriented Huawei FusionSolar / SmartPVMS Northbound API ingestion framework designed for:

- multi-account
- multi-plant
- multi-device-type
- raw-first archival
- generic long-form normalization
- typed curated fact tables
- checkpoint-safe incremental ingestion

## Why v2?

The old pipeline was mainly inverter-centric.  
This v2 framework is redesigned as a generic ingestion platform for device telemetry.

It supports:
- devType 1 = inverter
- devType 10 = EMI / environmental sensor
- devType 17 = meter
- devType 63 = logger
- additional future devTypes with minimal refactoring

## Core principles

1. Raw-first
2. Generic normalization for any metric
3. Typed normalization for known device families
4. Config-driven orchestration
5. Safe incremental loads with overlap
6. SQL Server friendly design

## Project structure

```text
solar_ingestion_v2/
│
├─ config/
│  ├─ app.yaml
│  ├─ jobs/
│  └─ mappings/
│
├─ data/
│  └─ raw/
│
├─ scripts/
│  ├─ run_job.py
│  ├─ run_backfill.py
│  ├─ replay_raw.py
│  └─ validate_sample_json.py
│
├─ src/
│  ├─ api/
│  ├─ orchestrator/
│  ├─ extract/
│  ├─ raw/
│  ├─ normalize/
│  ├─ db/
│  ├─ domain/
│  ├─ config_loader.py
│  └─ main.py
│
├─ sql/
├─ tests/
├─ requirements.txt
└─ README.md


---

# C) Bootstrap checklist + test plan สำหรับรันครั้งแรก

นี่คือชุดที่เอาไปใช้เปิดงานจริงได้เลย

## 1) Bootstrap checklist

### 1. Environment
- ติดตั้ง Python version ที่จะใช้จริง
- ติดตั้ง ODBC Driver 17
- ทดสอบ SQL Server connection string
- สร้าง virtual environment
- ติดตั้ง packages จาก `requirements.txt`

### 2. Database
- รัน `01_schema.sql` ถึง `05_ctl.sql`
- ยืนยันว่าตารางทุกตัวถูกสร้างครบ
- ยืนยันว่าตาราง metadata เดิมมีอยู่แล้ว:
  - `dim_api_account`
  - `dim_plant`
  - `dim_device`
  - `plant_account_assignment`

### 3. Metadata readiness
- เติม `dim_api_account` ให้ครบ 3 accounts:
  - GPSC_PI_01
  - GPSC_PI_02
  - GPSC_PI_03
- ตรวจว่าแต่ละ account มี `base_url`, `username`, และ field ที่ต้องใช้ login ครบ
- ตรวจว่า `plant_account_assignment` map plant → account ถูกต้อง
- ตรวจว่า `dim_device` มี `plant_code`, `dev_type_id`, `dev_id`, `dev_dn`, `is_active`

### 4. Config readiness
- สร้าง `config/app.yaml`
- สร้าง `config/jobs/dev_history_default.yaml`
- ตั้ง `raw_root` ให้เป็น path ที่เครื่องรันเข้าถึงได้
- ตั้ง `bootstrap_start_utc` เป็นช่วงที่อยากเริ่ม ingest จริง

### 5. File system
- สร้างโฟลเดอร์ `data/raw/`
- ตรวจสิทธิ์ write/read
- ถ้า production ใช้ network drive ให้เช็ก latency และ permission ด้วย

### 6. API sanity
- ทดสอบ login 1 account ก่อน
- ทดสอบดึง plant list
- ทดสอบ 1 plant x 1 devType x 1 batch
- ยืนยันว่าคืน `failCode = 0` ก่อนขยายเป็นหลาย target

---

## 2) First-run test plan

ผมแนะนำให้รันเป็นลำดับนี้ ไม่ควรกระโดดข้าม

### Test 1 — Login only
**เป้าหมาย:** ยืนยันว่า session manager ใช้งานได้

ตรวจ:
- login สำเร็จ
- ได้ token
- token reuse ได้
- repeated call ใน 30 นาทีไม่ต้อง relogin ถ้า session เดิมยังใช้ได้ 

### Test 2 — Raw only, 1 plant x 1 devType x 1 account
**เป้าหมาย:** ยืนยัน raw-first layer

ตรวจ:
- มีไฟล์ request/response ใน `data/raw/...`
- `raw.api_call` ถูก insert
- `raw.api_call_device` ถูก insert
- `ctl.ingest_batch_audit` ถูก insert

### Test 3 — Generic normalization only
เริ่มจาก devType 10 ก่อน เพราะ payload ค่อนข้างเรียบ

จาก sample devType 10 มี metric เช่น `temperature`, `wind_speed`, `pv_temperature`, `radiant_line`, `radiant_total`, `horiz_radiant_line` 

ตรวจ:
- `norm.device_metric_long` มี rows
- `metric_name` กระจายตรงกับ payload จริง
- `norm.metric_catalog` ถูก update

### Test 4 — devType 17 meter
จาก sample meter มี `active_power`, `reactive_power`, `power_factor`, `a_i`, `b_i`, `c_i`, `a_u`, `ab_u`, `reverse_active_cap`, `total_apparent_power` :contentReference[oaicite:4]{index=4}

ตรวจ:
- generic rows ถูกสร้างครบ
- typed rows เข้า `mart.fact_dev_meter_5min`
- natural key `(dev_id, collect_time_utc)` ไม่ซ้ำ

### Test 5 — devType 63 logger
sample logger ใช้ `dataItems` ไม่ใช่ `dataItemMap` และมี `total_yield`, `total_power_consumption`, `total_supply_from_grid`, `total_feed_in_to_grid`, `total_charge`, `total_discharge` 

ตรวจ:
- normalizer รองรับ `dataItems`
- `devDn` ถูก parse เป็น `dev_id` ได้ในกรณีไม่มี `devId`
- typed rows เข้า `mart.fact_dev_logger_5min`

### Test 6 — devType 1 inverter
sample inverter มี `active_power`, `reactive_power`, `power_factor`, `efficiency`, `temperature`, `day_cap`, `total_cap`, `inverter_state`, `elec_freq` และ MPPT fields 

ตรวจ:
- typed rows เข้า `mart.fact_dev_inverter_5min`
- `mppt_total_cap_kwh` คำนวณได้
- `open_time` / `close_time` parse ได้

### Test 7 — Overlap rerun
**เป้าหมาย:** ยืนยัน idempotency

ทำ:
- รัน window เดิมซ้ำอีกครั้ง

ตรวจ:
- raw layer มี raw call ใหม่ได้
- generic layer ไม่เกิด duplicate logical metric
- typed layer ไม่เกิด duplicate PK

### Test 8 — Multi-account run
รัน target ที่กระจายอยู่บน 3 accounts

ตรวจ:
- แต่ละ account มี session ของตัวเอง
- token reuse แยกจากกัน
- target ของ account หนึ่ง fail แล้วอีก account ยังรันต่อได้

### Test 9 — Failure path
จำลอง:
- account ผิด
- dev_ids ว่าง
- API failCode != 0
- timeout

ตรวจ:
- checkpoint status ถูกต้องเป็น `FAILED`, `PARTIAL`, `SKIPPED`, `NO_DEVICES`
- batch audit มีข้อความ error

---

## 3) Validation SQL queries ที่ควรใช้หลังรัน

### ดู raw calls ล่าสุด
```sql
SELECT TOP 20 *
FROM raw.api_call
ORDER BY raw_id DESC;