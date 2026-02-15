from __future__ import annotations

import hashlib
import json
import platform
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


TASK_ID = "TASK_CEP_BUNDLE_CORE_F0_001_BOOTSTRAP_REPO_AND_VENV_GATES_V1"
OFFICIAL_PYTHON = "/home/wilson/PortfolioZero/.venv/bin/python"
OFFICIAL_VENV_PREFIX = "/home/wilson/PortfolioZero/.venv/"
LEGACY_REPOS = [
    "/home/wilson/CEP_NA_BOLSA",
    "/home/wilson/CEP_COMPRA",
]
OUT_DIR_REL = "outputs/governanca/bootstrap_repo/20260215"


@dataclass
class GateResult:
    name: str
    passed: bool
    details: str


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_cmd(command: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
    )


def build_tree_lines(root: Path) -> list[str]:
    lines: list[str] = [f"{root.name}/"]
    for path in sorted(root.rglob("*")):
        if any(part == "__pycache__" for part in path.parts):
            continue
        rel = path.relative_to(root)
        indent = "  " * (len(rel.parts) - 1)
        suffix = "/" if path.is_dir() else ""
        lines.append(f"{indent}- {rel.name}{suffix}")
    return lines


def ensure_official_interpreter() -> None:
    current = str(Path(sys.executable))
    expected = OFFICIAL_PYTHON
    if current != expected:
        raise RuntimeError(
            f"Gate S3 falhou: sys.executable={current} difere de official_python={expected}"
        )
    if not current.startswith(OFFICIAL_VENV_PREFIX):
        raise RuntimeError(
            f"Gate S3 falhou: sys.executable={current} fora de {OFFICIAL_VENV_PREFIX}"
        )


def gather_git_status(root: Path) -> str:
    result = run_cmd(["git", "-C", str(root), "status", "--short", "--branch"], cwd=root)
    if result.returncode != 0:
        return "git status indisponivel (diretorio nao e repo git)"
    return result.stdout.strip() or "working tree limpo"


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    out_dir = repo_root / OUT_DIR_REL
    evidence_dir = out_dir / "evidence"
    report_path = out_dir / "report.md"
    manifest_path = out_dir / "manifest.json"
    requirements_lock_path = repo_root / "requirements.lock.txt"
    timestamp = datetime.now(timezone.utc).isoformat()

    out_dir.mkdir(parents=True, exist_ok=True)
    evidence_dir.mkdir(parents=True, exist_ok=True)

    gate_results: list[GateResult] = []

    # S1_GATE_ALLOWLIST
    repo_str = str(repo_root)
    in_home = repo_str.startswith("/home/wilson/")
    not_legacy = all(repo_str != legacy for legacy in LEGACY_REPOS)
    gate_results.append(
        GateResult(
            "S1_GATE_ALLOWLIST",
            in_home and not_legacy,
            f"repo_root={repo_str}",
        )
    )

    # S2_CHECK_COMPILE_OR_IMPORTS
    compile_result = run_cmd(
        [OFFICIAL_PYTHON, "-m", "py_compile", "scripts/run_task.py", "scripts/smoke.py"],
        cwd=repo_root,
    )
    gate_results.append(
        GateResult(
            "S2_CHECK_COMPILE_OR_IMPORTS",
            compile_result.returncode == 0,
            (compile_result.stderr or compile_result.stdout).strip() or "py_compile ok",
        )
    )

    # S3_GATE_VENV_OFFICIAL_PYTHON
    try:
        ensure_official_interpreter()
        gate_results.append(
            GateResult(
                "S3_GATE_VENV_OFFICIAL_PYTHON",
                True,
                f"sys.executable={sys.executable}",
            )
        )
    except RuntimeError as exc:
        gate_results.append(
            GateResult("S3_GATE_VENV_OFFICIAL_PYTHON", False, str(exc))
        )

    # requirements.lock via official python
    freeze_result = run_cmd([OFFICIAL_PYTHON, "-m", "pip", "freeze"], cwd=repo_root)
    if freeze_result.returncode != 0:
        raise RuntimeError(f"pip freeze falhou: {freeze_result.stderr.strip()}")
    requirements_lock_path.write_text(freeze_result.stdout, encoding="utf-8")

    # S4_RUN_SMOKE
    smoke_result = run_cmd(
        [
            OFFICIAL_PYTHON,
            "scripts/smoke.py",
            "--evidence-dir",
            str(evidence_dir),
        ],
        cwd=repo_root,
    )
    gate_results.append(
        GateResult(
            "S4_RUN_SMOKE",
            smoke_result.returncode == 0,
            (smoke_result.stderr or smoke_result.stdout).strip() or "smoke ok",
        )
    )

    outputs_map: dict[str, Path] = {
        "report_md": report_path,
        "manifest_json": manifest_path,
        "requirements_lock": requirements_lock_path,
    }
    for evidence_file in sorted(evidence_dir.glob("*")):
        outputs_map[f"evidence/{evidence_file.name}"] = evidence_file

    # S5_VERIFY_OUTPUTS_EXIST_AND_NONEMPTY (parcial, sem report/manifest ainda)
    s5_targets = [requirements_lock_path] + sorted(evidence_dir.glob("*"))
    s5_ok = bool(s5_targets) and all(p.exists() and p.stat().st_size > 0 for p in s5_targets)
    gate_results.append(
        GateResult(
            "S5_VERIFY_OUTPUTS_EXIST_AND_NONEMPTY",
            s5_ok,
            f"checked={len(s5_targets)} arquivos",
        )
    )

    # Primeiro passe de hashes sem manifest (evita autorreferencia)
    hashes: dict[str, str] = {}
    for key, path in outputs_map.items():
        if path.exists() and path.stat().st_size > 0 and path != manifest_path:
            hashes[key] = sha256_file(path)

    gate_results.append(
        GateResult(
            "S6_WRITE_MANIFEST_HASHES",
            bool(hashes),
            f"hashes_registrados={len(hashes)}",
        )
    )

    gate_overall = all(g.passed for g in gate_results)
    python_info = {
        "sys_executable": sys.executable,
        "python_version": platform.python_version(),
    }
    git_status = gather_git_status(repo_root)

    manifest: dict[str, Any] = {
        "task_id": TASK_ID,
        "timestamp_utc": timestamp,
        "inputs": {
            "new_repo_root": str(repo_root),
            "official_python": OFFICIAL_PYTHON,
            "legacy_repo_1_root": LEGACY_REPOS[0],
            "legacy_repo_2_root": LEGACY_REPOS[1],
        },
        "outputs": {
            "out_dir": OUT_DIR_REL,
            "report_md": str(report_path.relative_to(repo_root)),
            "manifest_json": str(manifest_path.relative_to(repo_root)),
            "evidence_dir": str(evidence_dir.relative_to(repo_root)),
            "requirements_lock": str(requirements_lock_path.relative_to(repo_root)),
            "constitution_md": "docs/CONSTITUICAO.md",
            "amendments_dir": "docs/emendas/",
        },
        "hashes_sha256": hashes,
        "gate_results": [g.__dict__ for g in gate_results],
        "overall_pass": gate_overall,
        "sys.executable": python_info["sys_executable"],
        "python_version": python_info["python_version"],
        "git_status": git_status,
    }

    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )

    # Atualiza hashes com o manifest final e regrava.
    hashes["manifest_json"] = sha256_file(manifest_path)
    manifest["hashes_sha256"] = hashes
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )

    output_lines = [f"- `{k}`: `{v}`" for k, v in sorted(hashes.items())]
    gates_lines = [
        f"- `{g.name}`: {'PASS' if g.passed else 'FAIL'} - {g.details}"
        for g in gate_results
    ]
    report_text = "\n".join(
        [
            "# Bootstrap Report - CEP_BUNDLE_CORE",
            "",
            f"- task_id: `{TASK_ID}`",
            f"- timestamp_utc: `{timestamp}`",
            f"- overall: `{'PASS' if gate_overall else 'FAIL'}`",
            f"- sys.executable: `{python_info['sys_executable']}`",
            f"- python_version: `{python_info['python_version']}`",
            "",
            "## Gates",
            *gates_lines,
            "",
            "## Smoke Summary",
            f"- returncode: `{smoke_result.returncode}`",
            f"- evidence_dir: `{evidence_dir.relative_to(repo_root)}`",
            "",
            "## Repository Tree",
            "```text",
            *build_tree_lines(repo_root),
            "```",
            "",
            "## Outputs e Hashes",
            *output_lines,
            "",
            "## Git Status",
            "```text",
            git_status,
            "```",
            "",
        ]
    )
    report_path.write_text(report_text, encoding="utf-8")

    # hash final do report e sincroniza no manifest
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["hashes_sha256"]["report_md"] = sha256_file(report_path)
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )

    return 0 if gate_overall else 1


if __name__ == "__main__":
    raise SystemExit(main())

