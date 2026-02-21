import argparse
import json
import math
import hashlib
import re
from pathlib import Path

GENERIC_TOKENS = {'pass', 'gates', 'gate', 'verify', 'evidence', 'report', 'outputs', 'summary'}

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

def keyword_overlap(q_tokens, text_tokens):
    q = set(q_tokens)
    t = set(text_tokens)
    if not q:
        return 0.0
    return len(q & t) / len(q)

def path_boost(query_l, source_relpath, text_preview):
    b = 0.0
    p = source_relpath.lower()
    t = text_preview.lower()
    if 'f2_004' in query_l and ('f2_004' in p or 'f2_004' in t):
        b += 0.35
    if 'f2_003' in query_l and ('f2_003' in p or 'f2_003' in t):
        b += 0.30
    if 'contestado' in query_l or 'pass contestado' in query_l:
        if ('session_state_transfer_package_v3' in p) or ('f2_004/report.md' in p) or ('f2_003/report.md' in p):
            b += 0.35
    if 'reproducao' in query_l or 'reprodução' in query_l:
        if ('reproduction_commands' in t) or ('task_spec' in t) or ('run_' in t):
            b += 0.25
    return b

def anti_noise_penalty(text_preview):
    toks = tokenize(text_preview)
    if not toks:
        return 0.0
    dens = sum(1 for x in toks if x in GENERIC_TOKENS) / len(toks)
    return 0.30 * dens

def query_general(repo_root: Path, index_path: str, query: str, top_k: int):
    payload = json.loads((repo_root / index_path).read_text(encoding='utf-8'))
    qv = embed(query)
    q_tokens = tokenize(query)
    ql = query.lower()
    scored = []
    for c in payload.get('chunks', []):
        text = c.get('text', '')
        tks = tokenize(text)
        s_cos = cos(qv, c['vector']) if 'vector' in c else 0.0
        s_kw = keyword_overlap(q_tokens, tks)
        source = c.get('source_relpath', '')
        preview = text[:240]
        s_boost = path_boost(ql, source, preview)
        s_pen = anti_noise_penalty(preview)
        final = (0.55 * s_cos) + (0.55 * s_kw) + s_boost - s_pen
        scored.append((final, c))
    scored.sort(key=lambda x: x[0], reverse=True)
    out = []
    for score, c in scored[:top_k]:
        out.append({
            'score': score,
            'source_relpath': c['source_relpath'],
            'chunk_id': c['chunk_id'],
            'char_start': c['char_start'],
            'char_end': c['char_end'],
            'text_preview': c['text'][:240],
        })
    return out

def query_lessons(repo_root: Path, kb_json_path: str, query: str, top_k: int, min_score_lessons: float):
    kb = json.loads((repo_root / kb_json_path).read_text(encoding='utf-8'))
    lessons = kb.get('lessons', []) if isinstance(kb, dict) else []
    q_tokens = tokenize(query)
    q_set = set(q_tokens)
    scored = []
    for row in lessons:
        text = ' '.join([
            str(row.get('title', '')),
            str(row.get('context', '')),
            str(row.get('problem', '')),
            str(row.get('decision', '')),
            str(row.get('impact', '')),
            ' '.join(row.get('tags', [])),
        ])
        t_set = set(tokenize(text))
        overlap = (len(q_set & t_set) / len(q_set)) if q_set else 0.0
        tag_set = set(tokenize(' '.join(row.get('tags', []))))
        tag_overlap = (len(q_set & tag_set) / len(q_set)) if q_set else 0.0
        score = (0.7 * overlap) + (0.6 * tag_overlap)
        if score == 0.0:
            continue
        if score < min_score_lessons:
            continue
        scored.append((score, row))
    scored.sort(key=lambda x: x[0], reverse=True)
    out = []
    for score, row in scored[:top_k]:
        evidence_items = row.get('evidence_items', [])
        if not isinstance(evidence_items, list):
            evidence_items = []
        evidence_preview = []
        for item in evidence_items:
            if not isinstance(item, dict):
                continue
            p = str(item.get('path', ''))
            a = item.get('anchor')
            evidence_preview.append(f"{p}{a}" if (p and isinstance(a, str) and a.startswith('#L')) else p)
        out.append({
            'score': score,
            'lesson_id': row.get('lesson_id'),
            'title': row.get('title'),
            'tags': row.get('tags', []),
            'evidence_paths': row.get('evidence_paths', []),
            'evidence_items': evidence_items,
            'evidence_preview': evidence_preview,
            'context': row.get('context', ''),
            'external_ref': row.get('external_ref', False),
            'external_repo_hint': row.get('external_repo_hint', None),
        })
    no_hits = len(out) == 0
    return out, no_hits

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--query', required=True)
    parser.add_argument('--top-k', type=int, default=8)
    parser.add_argument('--index-path', default='corpus/rag/index/index_v4.json')
    parser.add_argument('--collection', choices=['general', 'lessons'], default='general')
    parser.add_argument('--lessons-json', default='corpus/lessons/LESSONS_LEARNED.json')
    parser.add_argument('--min-score-lessons', type=float, default=0.20)
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    payload = {'query': args.query, 'top_k': args.top_k, 'collection': args.collection}
    if args.collection == 'lessons':
        results, no_hits = query_lessons(repo_root, args.lessons_json, args.query, args.top_k, args.min_score_lessons)
        payload['min_score_lessons'] = args.min_score_lessons
        payload['no_hits'] = no_hits
    else:
        results = query_general(repo_root, args.index_path, args.query, args.top_k)
    payload['results'] = results
    print(json.dumps(payload, ensure_ascii=True, indent=2))
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
