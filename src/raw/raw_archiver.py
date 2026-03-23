import json
from pathlib import Path
from hashlib import sha256


class RawArchiver:
    def __init__(self, raw_root: str):
        self.raw_root = Path(raw_root)

    def archive(self, plant_code: str, dev_type_id: int, batch_hash: str, request_payload: dict, response_payload: dict):
        folder = self.raw_root / plant_code / f"devtype_{dev_type_id}" / batch_hash[:8]
        folder.mkdir(parents=True, exist_ok=True)

        req_text = json.dumps(request_payload, ensure_ascii=False, indent=2)
        res_text = json.dumps(response_payload, ensure_ascii=False, indent=2)

        req_path = folder / f"{batch_hash}_request.json"
        res_path = folder / f"{batch_hash}_response.json"

        req_path.write_text(req_text, encoding="utf-8")
        res_path.write_text(res_text, encoding="utf-8")

        return {
            "request_file_path": str(req_path),
            "response_file_path": str(res_path),
            "request_sha256": sha256(req_text.encode("utf-8")).hexdigest(),
            "response_sha256": sha256(res_text.encode("utf-8")).hexdigest(),
            "request_size_bytes": len(req_text.encode("utf-8")),
            "response_size_bytes": len(res_text.encode("utf-8")),
        }