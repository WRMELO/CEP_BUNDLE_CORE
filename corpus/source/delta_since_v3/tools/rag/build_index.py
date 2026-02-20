from pathlib import Path
import json
import subprocess
import sys

def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]
    task_spec = repo_root / 'planning/task_specs/TASK_CEP_BUNDLE_CORE_RAG_002_BUILD_LOCAL_VECTOR_INDEX_V1.json'
    cmd = [sys.executable, str(repo_root / 'scripts/agno_rag_runner.py'), '--task-spec', str(task_spec)]
    return subprocess.call(cmd)

if __name__ == '__main__':
    raise SystemExit(main())
