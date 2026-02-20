from __future__ import annotations

import argparse
import csv
import fnmatch
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


TASK_001 = "TASK_CEP_BUNDLE_CORE_RAG_001_DISCOVER_AND_PIN_CORPUS_V1"
TASK_002 = "TASK_CEP_BUNDLE_CORE_RAG_002_BUILD_LOCAL_VECTOR_INDEX_V1"
TASK_003 = "TASK_CEP_BUNDLE_CORE_RAG_003_APPEND_DELTA_SINCE_V3_AND_REINDEX_V1"


@dataclass
class Gate:
    gate_id: str
    status: str
    details: str


@dataclass
class Step:
    step_id: str
    status: str
    details: str


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def list_files(root: Path) -> list[Path]:
    return sorted([p for p in root.rglob("*") if p.is_file()])


def is_excluded(relpath: str, exclude_globs: list[str]) -> bool:
    parts = set(Path(relpath).parts)
    if ".git" in parts or ".venv" in parts or "node_modules" in parts or "artifacts_large" in parts:
        return True
    return any(fnmatch.fnmatch(relpath, pat) for pat in exclude_globs)


def is_textual_small(path: Path, max_bytes: int, allowed_ext: set[str] | None = None) -> bool:
    if not path.is_file():
        return False
    if path.stat().st_size > max_bytes:
        return False
    if allowed_ext is not None and path.suffix.lower() not in allowed_ext:
        return False
    if path.suffix.lower() in {".parquet", ".pdf", ".ipynb"}:
        return False
    return True


def safe_read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="latin-1", errors="ignore")


def copy_file_preserving_rel(repo_root: Path, src_rel: str, dest_root: Path) -> Path:
    src = repo_root / src_rel
    dst = dest_root / src_rel
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_bytes(src.read_bytes())
    return dst


def copy_tree_preserving_rel(repo_root: Path, src_rel_dir: str, dest_root: Path) -> list[Path]:
    src_dir = repo_root / src_rel_dir
    copied: list[Path] = []
    for path in sorted(src_dir.rglob("*")):
        if path.is_file():
            rel = str(path.relative_to(repo_root))
            copied.append(copy_file_preserving_rel(repo_root, rel, dest_root))
    return copied


def relpaths_with_hashes(root: Path) -> tuple[list[dict[str, Any]], int]:
    files = list_files(root)
    total = 0
    rows: list[dict[str, Any]] = []
    for f in files:
        rel = str(f.relative_to(root))
        size = f.stat().st_size
        total += size
        rows.append(
            {
                "relpath": rel,
                "sha256": sha256_file(f),
                "bytes": size,
            }
        )
    return rows, total


def tokenize(text: str) -> list[str]:
    return re.findall(r"[a-zA-Z0-9_]{2,}", text.lower())


def hash_embed(text: str, dim: int = 384) -> list[float]:
    vec = [0.0] * dim
    for tok in tokenize(text):
        h = int(hashlib.sha256(tok.encode("utf-8")).hexdigest(), 16)
        idx = h % dim
        sign = -1.0 if ((h >> 8) & 1) else 1.0
        vec[idx] += sign
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]


def cosine(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def markdown_aware_chunks(text: str, target: int, overlap: int) -> list[tuple[int, int, str]]:
    sections = re.split(r"(?m)^#{1,6}\s+", text)
    if len(sections) <= 1:
        sections = [text]
    chunks: list[tuple[int, int, str]] = []
    cursor = 0
    for sec in sections:
        if not sec.strip():
            cursor += len(sec)
            continue
        local = sec
        start = 0
        while start < len(local):
            end = min(start + target, len(local))
            chunk = local[start:end]
            chunks.append((cursor + start, cursor + end, chunk))
            if end == len(local):
                break
            start = max(0, end - overlap)
        cursor += len(sec)
    return chunks


def build_index_from_manifest(
    repo_root: Path,
    manifest_path: Path,
    source_root: Path,
    index_root: Path,
    config_path: Path,
    version_tag: str,
    target_chunk_chars: int,
    overlap_chars: int,
    allowed_ext: set[str],
) -> dict[str, Any]:
    manifest = read_json(manifest_path)
    files = manifest.get("files", [])

    detected_options = []
    try:
        import sklearn  # type: ignore  # noqa: F401

        detected_options.append("sklearn_detected")
    except Exception:
        pass
    try:
        import sentence_transformers  # type: ignore  # noqa: F401

        detected_options.append("sentence_transformers_detected")
    except Exception:
        pass
    detected_options.append("local_hashing_v1")

    # Politica offline e sem instalacao: backend local deterministico sem download.
    selected_backend = "local_hashing_v1"

    chunks: list[dict[str, Any]] = []
    seen_docs = 0
    for row in files:
        rel = row["relpath"]
        src = source_root / rel
        if src.suffix.lower() not in allowed_ext:
            continue
        if not src.exists():
            continue
        seen_docs += 1
        text = safe_read_text(src)
        for i, (cstart, cend, ctext) in enumerate(markdown_aware_chunks(text, target_chunk_chars, overlap_chars)):
            if not ctext.strip():
                continue
            chunks.append(
                {
                    "source_relpath": rel,
                    "chunk_id": f"{rel}::c{i}",
                    "char_start": cstart,
                    "char_end": cend,
                    "text": ctext,
                    "vector": hash_embed(ctext),
                }
            )

    index_root.mkdir(parents=True, exist_ok=True)
    index_payload = {
        "index_version": version_tag,
        "created_at_utc": now_utc(),
        "embedding_backend": selected_backend,
        "embedding_detected_options": detected_options,
        "dim": 384,
        "n_docs": seen_docs,
        "n_chunks": len(chunks),
        "chunks": chunks,
    }
    index_path = index_root / f"index_{version_tag}.json"
    write_json(index_path, index_payload)

    rag_config = {
        "index_version": version_tag,
        "index_path": str(index_path.relative_to(repo_root)),
        "embedding_backend": selected_backend,
        "embedding_detected_options": detected_options,
        "chunking": {
            "strategy": "markdown_aware_with_fallback",
            "target_chunk_chars": target_chunk_chars,
            "overlap_chars": overlap_chars,
        },
        "top_k_default": 8,
        "updated_at_utc": now_utc(),
    }
    write_json(config_path, rag_config)
    return {
        "index_path": index_path,
        "rag_config": rag_config,
        "n_docs": seen_docs,
        "n_chunks": len(chunks),
        "selected_backend": selected_backend,
    }


def run_query(index_path: Path, query: str, top_k: int) -> list[dict[str, Any]]:
    payload = read_json(index_path)
    chunks = payload.get("chunks", [])
    qv = hash_embed(query)
    scored: list[tuple[float, dict[str, Any]]] = []
    for c in chunks:
        score = cosine(qv, c["vector"])
        scored.append((score, c))
    scored.sort(key=lambda x: x[0], reverse=True)
    out = []
    for score, c in scored[:top_k]:
        out.append(
            {
                "score": score,
                "source_relpath": c["source_relpath"],
                "chunk_id": c["chunk_id"],
                "char_start": c["char_start"],
                "char_end": c["char_end"],
                "text_preview": c["text"][:240],
            }
        )
    return out


def create_run_dir(repo_root: Path, task_id: str) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = repo_root / "planning" / "runs" / task_id / f"run_{ts}"
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir


def run_task_001(repo_root: Path, task_spec: dict[str, Any]) -> int:
    outputs_root = repo_root / "outputs/governanca/rag/20260220/corpus_discovery"
    evidence_dir = outputs_root / "evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    run_dir = create_run_dir(repo_root, TASK_001)

    corpus_root = repo_root / "corpus"
    source_root = corpus_root / "source"
    manifests_root = corpus_root / "manifests"
    corpus_root.mkdir(parents=True, exist_ok=True)
    source_root.mkdir(parents=True, exist_ok=True)
    manifests_root.mkdir(parents=True, exist_ok=True)

    required = task_spec["inputs"]["must_include_minimum"]
    exclude_globs = task_spec["inputs"]["exclude_globs"]
    max_bytes = int(task_spec["inputs"]["size_policy"]["max_file_bytes_default"])

    gates: list[Gate] = []
    steps: list[Step] = []

    g1_ok = str(repo_root.resolve()) == "/home/wilson/CEP_BUNDLE_CORE" and corpus_root.exists()
    gates.append(Gate("G1_ALLOWLIST_AND_ROOT", "PASS" if g1_ok else "FAIL", f"repo_root={repo_root.resolve()}"))

    missing = [p for p in required if not (repo_root / p).exists()]
    g2_ok = len(missing) == 0
    gates.append(Gate("G2_MINIMUM_PRESENT", "PASS" if g2_ok else "FAIL", f"missing={missing}"))
    if not g2_ok:
        report = {
            "task_id": TASK_001,
            "status": "ABORT",
            "overall_pass": False,
            "gates": [g.__dict__ for g in gates],
            "steps": [],
            "reason": "SSOT obrigatorio ausente",
            "missing_required": missing,
            "timestamp_utc": now_utc(),
        }
        write_json(outputs_root / "report.json", report)
        write_json(run_dir / "run_summary.json", report)
        return 2

    copied: set[str] = set()
    for rel in required:
        src = repo_root / rel
        if src.is_dir():
            for p in copy_tree_preserving_rel(repo_root, rel, source_root):
                copied.add(str(p.relative_to(source_root)))
        else:
            copied.add(str(copy_file_preserving_rel(repo_root, rel, source_root).relative_to(source_root)))
    steps.append(Step("S1_COPY_MINIMUM_REQUIRED", "PASS", f"copied_required_files={len(copied)}"))

    additional: set[str] = set()
    candidates_roots = [
        ("docs", {"scope": "docs"}),
        ("planning/task_specs", {"scope": "task_specs"}),
        ("planning/runs", {"scope": "runs"}),
        ("outputs", {"scope": "outputs"}),
    ]
    allowed_outputs_patterns = [
        "*report.md",
        "*report.json",
        "*summary*.json",
        "*manifest*.json",
        "*inventory*.csv",
    ]
    for root_rel, meta in candidates_roots:
        base = repo_root / root_rel
        if not base.exists():
            continue
        for f in sorted(base.rglob("*")):
            if not f.is_file():
                continue
            rel = str(f.relative_to(repo_root))
            if rel in required:
                continue
            if is_excluded(rel, exclude_globs):
                continue
            if not is_textual_small(f, max_bytes):
                continue
            if meta["scope"] in {"runs", "outputs"}:
                name = f.name.lower()
                if not any(fnmatch.fnmatch(name, p) for p in allowed_outputs_patterns):
                    continue
            copy_file_preserving_rel(repo_root, rel, source_root)
            additional.add(rel)
    steps.append(Step("S2_DISCOVER_AND_COPY_ADDITIONALS", "PASS", f"additional_files={len(additional)}"))

    files_manifest, total_bytes = relpaths_with_hashes(source_root)
    manifest = {
        "task_id": TASK_001,
        "generated_at_utc": now_utc(),
        "source_root": str(source_root.relative_to(repo_root)),
        "num_files": len(files_manifest),
        "total_bytes": total_bytes,
        "files": files_manifest,
    }
    manifest_path = manifests_root / "corpus_manifest_v1.json"
    write_json(manifest_path, manifest)

    missing_rel = [row["relpath"] for row in files_manifest if not (source_root / row["relpath"]).exists()]
    g3_ok = manifest_path.exists() and len(files_manifest) > 0 and len(missing_rel) == 0
    gates.append(Gate("G3_MANIFEST_VALID", "PASS" if g3_ok else "FAIL", f"missing_relpaths={len(missing_rel)}"))
    steps.append(Step("S3_WRITE_MANIFEST", "PASS" if g3_ok else "FAIL", f"num_files={len(files_manifest)}"))

    corpus_readme = corpus_root / "README.md"
    corpus_readme.write_text(
        "\n".join(
            [
                "# Corpus Local do CEP_BUNDLE_CORE",
                "",
                "Corpus consolidado em top-level `corpus/` para suporte ao RAG local.",
                "Origem em `docs/`, `planning/task_specs/`, `planning/runs/` e `outputs/` curados.",
                "",
                "Arquivos canonicos em `corpus/source/`.",
                "Manifest em `corpus/manifests/corpus_manifest_v1.json`.",
                "",
            ]
        ),
        encoding="utf-8",
    )

    idx_path = corpus_root / "CORPUS_INDEX.md"
    top_rows = files_manifest[:120]
    lines = ["# CORPUS INDEX", "", f"- num_files: {len(files_manifest)}", f"- total_bytes: {total_bytes}", "", "## Primeiros itens"]
    lines.extend([f"- `{row['relpath']}` ({row['bytes']} bytes)" for row in top_rows])
    idx_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    steps.append(Step("S4_WRITE_README_AND_INDEX", "PASS", "README e CORPUS_INDEX gerados"))

    write_json(
        evidence_dir / "required_copy_check.json",
        {
            "required": required,
            "missing_required": missing,
            "required_copied": sorted([r for r in copied if any(r == x or r.startswith(x.rstrip("/") + "/") for x in required)]),
        },
    )
    write_json(
        evidence_dir / "discovery_stats.json",
        {
            "required_files_copied_count": len(copied),
            "additional_files_copied_count": len(additional),
            "num_files_manifest": len(files_manifest),
            "total_bytes": total_bytes,
        },
    )

    overall = all(g.status == "PASS" for g in gates) and all(s.status == "PASS" for s in steps)
    report = {
        "task_id": TASK_001,
        "status": "PASS" if overall else "FAIL",
        "overall_pass": overall,
        "gates": [g.__dict__ for g in gates],
        "steps": [s.__dict__ for s in steps],
        "deliverables": {
            "corpus_readme": str(corpus_readme.relative_to(repo_root)),
            "corpus_index": str(idx_path.relative_to(repo_root)),
            "corpus_manifest": str(manifest_path.relative_to(repo_root)),
            "corpus_source_root": str(source_root.relative_to(repo_root)),
        },
        "metrics": {
            "n_files": len(files_manifest),
            "total_bytes": total_bytes,
            "required_minimum_items": len(required),
            "additional_discovered_files": len(additional),
        },
        "timestamp_utc": now_utc(),
    }
    write_json(outputs_root / "report.json", report)
    write_json(run_dir / "run_summary.json", report)
    return 0 if overall else 1


def run_task_002(repo_root: Path, task_spec: dict[str, Any]) -> int:
    outputs_root = repo_root / "outputs/governanca/rag/20260220/rag_build"
    evidence_dir = outputs_root / "evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    run_dir = create_run_dir(repo_root, TASK_002)

    manifest_path = repo_root / task_spec["inputs"]["corpus_manifest"]
    source_root = repo_root / task_spec["inputs"]["corpus_source_root"]
    rag_root = repo_root / "corpus/rag"
    index_root = repo_root / "corpus/rag/index"
    rag_config = repo_root / "corpus/rag/rag_config.json"
    rag_readme = repo_root / "corpus/rag/README.md"
    tools_root = repo_root / "tools/rag"
    tools_root.mkdir(parents=True, exist_ok=True)
    rag_root.mkdir(parents=True, exist_ok=True)

    gates: list[Gate] = []
    steps: list[Step] = []

    g1_ok = manifest_path.exists() and source_root.exists()
    gates.append(Gate("G1_CORPUS_READY", "PASS" if g1_ok else "FAIL", f"manifest_exists={manifest_path.exists()}"))
    if not g1_ok:
        report = {
            "task_id": TASK_002,
            "status": "ABORT",
            "overall_pass": False,
            "gates": [g.__dict__ for g in gates],
            "steps": [],
            "reason": "corpus v1 ausente",
            "timestamp_utc": now_utc(),
        }
        write_json(outputs_root / "report.json", report)
        write_json(run_dir / "run_summary.json", report)
        return 2

    start = time.time()
    allowed_ext = set(task_spec["inputs"]["allowed_extensions"])
    chunk_cfg = task_spec["inputs"]["chunking_policy"]
    built = build_index_from_manifest(
        repo_root=repo_root,
        manifest_path=manifest_path,
        source_root=source_root,
        index_root=index_root,
        config_path=rag_config,
        version_tag="v1",
        target_chunk_chars=int(chunk_cfg["target_chunk_chars"]),
        overlap_chars=int(chunk_cfg["overlap_chars"]),
        allowed_ext=allowed_ext,
    )
    elapsed = time.time() - start
    steps.append(Step("S1_BUILD_INDEX", "PASS", f"n_docs={built['n_docs']} n_chunks={built['n_chunks']}"))

    build_tool = repo_root / "tools/rag/build_index.py"
    query_tool = repo_root / "tools/rag/query.py"
    build_tool.write_text(
        "\n".join(
            [
                "from pathlib import Path",
                "import json",
                "import subprocess",
                "import sys",
                "",
                "def main() -> int:",
                "    repo_root = Path(__file__).resolve().parents[2]",
                "    task_spec = repo_root / 'planning/task_specs/TASK_CEP_BUNDLE_CORE_RAG_002_BUILD_LOCAL_VECTOR_INDEX_V1.json'",
                "    cmd = [sys.executable, str(repo_root / 'scripts/agno_rag_runner.py'), '--task-spec', str(task_spec)]",
                "    return subprocess.call(cmd)",
                "",
                "if __name__ == '__main__':",
                "    raise SystemExit(main())",
                "",
            ]
        ),
        encoding="utf-8",
    )
    query_tool.write_text(
        "\n".join(
            [
                "import argparse",
                "import json",
                "import math",
                "import hashlib",
                "import re",
                "from pathlib import Path",
                "",
                "def tokenize(text: str):",
                "    return re.findall(r'[a-zA-Z0-9_]{2,}', text.lower())",
                "",
                "def embed(text: str, dim: int = 384):",
                "    v = [0.0] * dim",
                "    for t in tokenize(text):",
                "        h = int(hashlib.sha256(t.encode('utf-8')).hexdigest(), 16)",
                "        i = h % dim",
                "        s = -1.0 if ((h >> 8) & 1) else 1.0",
                "        v[i] += s",
                "    n = math.sqrt(sum(x*x for x in v)) or 1.0",
                "    return [x / n for x in v]",
                "",
                "def cos(a, b):",
                "    return sum(x*y for x, y in zip(a, b))",
                "",
                "def main() -> int:",
                "    parser = argparse.ArgumentParser()",
                "    parser.add_argument('--query', required=True)",
                "    parser.add_argument('--top-k', type=int, default=8)",
                "    parser.add_argument('--index-path', default='corpus/rag/index/index_v1.json')",
                "    args = parser.parse_args()",
                "    repo_root = Path(__file__).resolve().parents[2]",
                "    payload = json.loads((repo_root / args.index_path).read_text(encoding='utf-8'))",
                "    qv = embed(args.query)",
                "    scored = []",
                "    for c in payload.get('chunks', []):",
                "        scored.append((cos(qv, c['vector']), c))",
                "    scored.sort(key=lambda x: x[0], reverse=True)",
                "    out = []",
                "    for score, c in scored[:args.top_k]:",
                "        out.append({",
                "            'score': score,",
                "            'source_relpath': c['source_relpath'],",
                "            'chunk_id': c['chunk_id'],",
                "            'char_start': c['char_start'],",
                "            'char_end': c['char_end'],",
                "            'text_preview': c['text'][:240],",
                "        })",
                "    print(json.dumps({'query': args.query, 'top_k': args.top_k, 'results': out}, ensure_ascii=True, indent=2))",
                "    return 0",
                "",
                "if __name__ == '__main__':",
                "    raise SystemExit(main())",
                "",
            ]
        ),
        encoding="utf-8",
    )
    steps.append(Step("S2_WRITE_TOOLS", "PASS", "tools/rag/build_index.py e tools/rag/query.py gerados"))

    smoke_results = run_query(built["index_path"], "objetivo real ssot e governanca", top_k=8)
    smoke_ok = len(smoke_results) > 0 and all("source_relpath" in r for r in smoke_results)
    write_json(evidence_dir / "smoke_query_results.json", {"results": smoke_results, "ok": smoke_ok})
    write_json(
        evidence_dir / "index_stats.json",
        {
            "n_docs": built["n_docs"],
            "n_chunks": built["n_chunks"],
            "elapsed_seconds": elapsed,
            "backend": built["selected_backend"],
        },
    )
    steps.append(Step("S3_SMOKE_QUERY", "PASS" if smoke_ok else "FAIL", f"results={len(smoke_results)}"))

    rag_readme.write_text(
        "\n".join(
            [
                "# RAG Local",
                "",
                "Indice local persistido em `corpus/rag/index/`.",
                "Configuracao em `corpus/rag/rag_config.json`.",
                "Build: `python tools/rag/build_index.py`.",
                "Query: `python tools/rag/query.py --query \"...\" --top-k 8`.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    steps.append(Step("S4_WRITE_RAG_README", "PASS", "README do RAG gerado"))

    g2_ok = index_root.exists() and any(index_root.glob("index_v1.json")) and rag_config.exists()
    gates.append(Gate("G2_INDEX_PERSISTED", "PASS" if g2_ok else "FAIL", "index e config gerados"))
    g3_ok = smoke_ok
    gates.append(Gate("G3_SMOKE_QUERY", "PASS" if g3_ok else "FAIL", f"smoke_results={len(smoke_results)}"))

    overall = all(g.status == "PASS" for g in gates) and all(s.status == "PASS" for s in steps)
    report = {
        "task_id": TASK_002,
        "status": "PASS" if overall else "FAIL",
        "overall_pass": overall,
        "gates": [g.__dict__ for g in gates],
        "steps": [s.__dict__ for s in steps],
        "deliverables": {
            "rag_readme": str(rag_readme.relative_to(repo_root)),
            "rag_config": str(rag_config.relative_to(repo_root)),
            "rag_index_root": str(index_root.relative_to(repo_root)),
            "tool_build": "tools/rag/build_index.py",
            "tool_query": "tools/rag/query.py",
        },
        "metrics": {
            "n_docs": built["n_docs"],
            "n_chunks": built["n_chunks"],
            "elapsed_seconds": elapsed,
            "backend": built["selected_backend"],
        },
        "timestamp_utc": now_utc(),
    }
    write_json(outputs_root / "report.json", report)
    write_json(run_dir / "run_summary.json", report)
    return 0 if overall else 1


def git_changed_since_commit(repo_root: Path, commit: str) -> list[str]:
    cmd = ["git", "-C", str(repo_root), "diff", "--name-only", f"{commit}..HEAD"]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        return []
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def latest_baseline_commit(repo_root: Path, baseline_files: list[str]) -> str | None:
    cmd = ["git", "-C", str(repo_root), "log", "-1", "--format=%H", "--"] + baseline_files
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        return None
    out = proc.stdout.strip()
    return out or None


def run_task_003(repo_root: Path, task_spec: dict[str, Any]) -> int:
    outputs_root = repo_root / "outputs/governanca/rag/20260220/delta_since_v3"
    evidence_dir = outputs_root / "evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    run_dir = create_run_dir(repo_root, TASK_003)

    baseline_files = task_spec["inputs"]["baseline_marker"]
    baseline_exists = all((repo_root / p).exists() for p in baseline_files)
    gates: list[Gate] = [Gate("G1_BASELINE_EXISTS", "PASS" if baseline_exists else "FAIL", f"baseline_files={baseline_files}")]
    steps: list[Step] = []
    if not baseline_exists:
        report = {
            "task_id": TASK_003,
            "status": "ABORT",
            "overall_pass": False,
            "gates": [g.__dict__ for g in gates],
            "steps": [],
            "reason": "baseline V3 ausente",
            "timestamp_utc": now_utc(),
        }
        write_json(outputs_root / "report.json", report)
        write_json(run_dir / "run_summary.json", report)
        return 2

    method = "mtime_fallback"
    why = "commit baseline nao encontrado"
    changed: list[str] = []
    commit = latest_baseline_commit(repo_root, baseline_files)
    if commit:
        changed = git_changed_since_commit(repo_root, commit)
        method = "git_history_since_baseline_marker_commit"
        why = f"commit_baseline={commit}"
    if not changed:
        baseline_mtime = max((repo_root / p).stat().st_mtime for p in baseline_files if (repo_root / p).exists())
        changed = []
        for f in sorted(repo_root.rglob("*")):
            if f.is_file() and f.stat().st_mtime >= baseline_mtime:
                changed.append(str(f.relative_to(repo_root)))
        method = "file_mtime_since_baseline_marker_mtime"
        why = "fallback por ausencia de diff util no git"

    write_json(evidence_dir / "delta_detection_method.json", {"method": method, "why": why, "n_changed_raw": len(changed)})
    steps.append(Step("S1_DETECT_DELTA", "PASS", f"method={method} n_changed_raw={len(changed)}"))

    exclude_globs = [
        "**/*.parquet",
        "**/*.pdf",
        "**/*.ipynb",
        "**/.git/**",
        "**/.venv/**",
        "**/node_modules/**",
        "**/artifacts_large/**",
    ]
    max_bytes = 2_000_000
    delta_files: list[str] = []
    for rel in changed:
        path = repo_root / rel
        if not path.exists() or not path.is_file():
            continue
        if rel.startswith("corpus/") or rel.startswith("_tentativa2_reexecucao_completa_20260220/"):
            continue
        if is_excluded(rel, exclude_globs):
            continue
        if not is_textual_small(path, max_bytes):
            continue
        delta_files.append(rel)
    delta_files = sorted(set(delta_files))

    delta_root = repo_root / "corpus/source/delta_since_v3"
    copied_delta: list[str] = []
    for rel in delta_files:
        dst = delta_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes((repo_root / rel).read_bytes())
        copied_delta.append(str(dst.relative_to(repo_root / "corpus/source")))
    steps.append(Step("S2_COPY_DELTA", "PASS", f"delta_files={len(copied_delta)}"))

    docs_rag = repo_root / "docs/rag"
    docs_rag.mkdir(parents=True, exist_ok=True)
    delta_md = docs_rag / "DELTA_SINCE_V3.md"
    delta_json = docs_rag / "DELTA_SINCE_V3.json"
    delta_md.write_text(
        "\n".join(
            [
                "# DELTA SINCE V3",
                "",
                f"- method: `{method}`",
                f"- why: `{why}`",
                f"- n_delta_files: `{len(copied_delta)}`",
                "",
                "## Arquivos de delta",
                *[f"- `{x}`" for x in copied_delta[:500]],
                "",
            ]
        ),
        encoding="utf-8",
    )
    write_json(
        delta_json,
        {
            "method": method,
            "why": why,
            "baseline_marker": baseline_files,
            "n_delta_files": len(copied_delta),
            "delta_files": copied_delta,
            "generated_at_utc": now_utc(),
        },
    )
    steps.append(Step("S3_WRITE_DELTA_DOCS", "PASS", "docs/rag/DELTA_SINCE_V3.md e .json gerados"))

    source_root = repo_root / "corpus/source"
    files_manifest, total_bytes = relpaths_with_hashes(source_root)
    manifest_v2 = {
        "task_id": TASK_003,
        "generated_at_utc": now_utc(),
        "source_root": str(source_root.relative_to(repo_root)),
        "num_files": len(files_manifest),
        "total_bytes": total_bytes,
        "files": files_manifest,
    }
    manifest_v1_path = repo_root / "corpus/manifests/corpus_manifest_v1.json"
    n_v1 = read_json(manifest_v1_path).get("num_files", 0) if manifest_v1_path.exists() else 0
    manifest_v2_path = repo_root / "corpus/manifests/corpus_manifest_v2.json"
    write_json(manifest_v2_path, manifest_v2)
    steps.append(Step("S4_WRITE_MANIFEST_V2", "PASS", f"num_files_v2={len(files_manifest)}"))

    g2_ok = (evidence_dir / "delta_detection_method.json").exists()
    gates.append(Gate("G2_DELTA_METHOD_EVIDENCED", "PASS" if g2_ok else "FAIL", f"method={method}"))
    g3_ok = manifest_v2_path.exists() and len(files_manifest) >= int(n_v1)
    gates.append(Gate("G3_MANIFEST_V2_VALID", "PASS" if g3_ok else "FAIL", f"num_files_v1={n_v1} num_files_v2={len(files_manifest)}"))

    allowed_ext = {".md", ".txt", ".json", ".yaml", ".yml", ".csv", ".py"}
    built = build_index_from_manifest(
        repo_root=repo_root,
        manifest_path=manifest_v2_path,
        source_root=source_root,
        index_root=repo_root / "corpus/rag/index",
        config_path=repo_root / "corpus/rag/rag_config.json",
        version_tag="v2",
        target_chunk_chars=1600,
        overlap_chars=200,
        allowed_ext=allowed_ext,
    )
    smoke_results = run_query(built["index_path"], "delta desde v3 e transfer package v3", 8)
    smoke_ok = len(smoke_results) > 0
    write_json(evidence_dir / "reindex_smoke_results.json", {"ok": smoke_ok, "results": smoke_results})
    steps.append(Step("S5_REINDEX_AND_SMOKE", "PASS" if smoke_ok else "FAIL", f"n_chunks={built['n_chunks']}"))
    g4_ok = smoke_ok
    gates.append(Gate("G4_REINDEX_SMOKE_OK", "PASS" if g4_ok else "FAIL", f"results={len(smoke_results)}"))

    overall = all(g.status == "PASS" for g in gates) and all(s.status == "PASS" for s in steps)
    report = {
        "task_id": TASK_003,
        "status": "PASS" if overall else "FAIL",
        "overall_pass": overall,
        "gates": [g.__dict__ for g in gates],
        "steps": [s.__dict__ for s in steps],
        "deliverables": {
            "delta_md": "docs/rag/DELTA_SINCE_V3.md",
            "delta_json": "docs/rag/DELTA_SINCE_V3.json",
            "corpus_manifest_v2": "corpus/manifests/corpus_manifest_v2.json",
            "updated_index_root": "corpus/rag/index/",
        },
        "metrics": {
            "n_delta_files": len(copied_delta),
            "num_files_v1": int(n_v1),
            "num_files_v2": len(files_manifest),
            "n_chunks_v2": built["n_chunks"],
            "total_bytes_v2": total_bytes,
        },
        "timestamp_utc": now_utc(),
    }
    write_json(outputs_root / "report.json", report)
    write_json(run_dir / "run_summary.json", report)
    return 0 if overall else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Agno runner local para tasks de RAG.")
    parser.add_argument("--task-spec", required=True, help="Path absoluto ou relativo para o task spec JSON.")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    task_path = Path(args.task_spec)
    if not task_path.is_absolute():
        task_path = repo_root / task_path
    if not task_path.exists():
        raise FileNotFoundError(f"Task spec nao encontrado: {task_path}")

    task_spec = read_json(task_path)
    task_id = task_spec.get("task_id", "")
    if task_id == TASK_001:
        return run_task_001(repo_root, task_spec)
    if task_id == TASK_002:
        return run_task_002(repo_root, task_spec)
    if task_id == TASK_003:
        return run_task_003(repo_root, task_spec)

    raise RuntimeError(f"Task nao suportada por este runner: {task_id}")


if __name__ == "__main__":
    raise SystemExit(main())

