from __future__ import annotations

import argparse
import hashlib
import json
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path


def sha256_text(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test mínimo do CEP_BUNDLE_CORE")
    parser.add_argument(
        "--evidence-dir",
        default="outputs/governanca/bootstrap_repo/20260215/evidence",
        help="Diretório de evidências do smoke test",
    )
    args = parser.parse_args()

    # Importações base para validar ambiente e runtime mínimo.
    import pathlib  # noqa: F401
    import typing  # noqa: F401

    evidence_dir = Path(args.evidence_dir)
    evidence_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "status": "SMOKE_OK",
        "sys_executable": sys.executable,
        "python_version": platform.python_version(),
    }
    payload_text = json.dumps(payload, ensure_ascii=True, indent=2)
    payload_hash = sha256_text(payload_text)

    evidence_file = evidence_dir / "smoke_evidence.txt"
    evidence_file.write_text(
        f"{payload_text}\nsha256_payload={payload_hash}\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

