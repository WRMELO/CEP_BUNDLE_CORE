import argparse
import json
import math
import hashlib
import re
from pathlib import Path

def tokenize(text: str):
    return re.findall(r'[a-zA-Z0-9_]{2,}', text.lower())

def embed(text: str, dim: int = 384):
    v = [0.0] * dim
    for t in tokenize(text):
        h = int(hashlib.sha256(t.encode('utf-8')).hexdigest(), 16)
        i = h % dim
        s = -1.0 if ((h >> 8) & 1) else 1.0
        v[i] += s
    n = math.sqrt(sum(x*x for x in v)) or 1.0
    return [x / n for x in v]

def cos(a, b):
    return sum(x*y for x, y in zip(a, b))

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--query', required=True)
    parser.add_argument('--top-k', type=int, default=8)
    parser.add_argument('--index-path', default='corpus/rag/index/index_v1.json')
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    payload = json.loads((repo_root / args.index_path).read_text(encoding='utf-8'))
    qv = embed(args.query)
    scored = []
    for c in payload.get('chunks', []):
        scored.append((cos(qv, c['vector']), c))
    scored.sort(key=lambda x: x[0], reverse=True)
    out = []
    for score, c in scored[:args.top_k]:
        out.append({
            'score': score,
            'source_relpath': c['source_relpath'],
            'chunk_id': c['chunk_id'],
            'char_start': c['char_start'],
            'char_end': c['char_end'],
            'text_preview': c['text'][:240],
        })
    print(json.dumps({'query': args.query, 'top_k': args.top_k, 'results': out}, ensure_ascii=True, indent=2))
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
