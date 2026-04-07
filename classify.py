#!/usr/bin/env python3
"""
Classify sentiment for all records in master_scrapes.jsonl using OpenRouter.

Resumes automatically if interrupted — already-classified records are skipped.

Usage:
    export OPENROUTER_API_KEY=sk-or-...
    python classify.py

Options:
    --input   Input JSONL  (default: master_scrapes.jsonl)
    --output  Output JSONL (default: master_scrapes_classified.jsonl)
    --model   OpenRouter model string (default: anthropic/claude-haiku-4-5)
    --workers Number of concurrent API calls (default: 5)
    --max-chars Max characters of text sent to LLM (default: 4000)
    --dry-run Print first 3 records without calling API
"""
import argparse
import html
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DEFAULT_MODEL    = "anthropic/claude-haiku-4-5"
OPENROUTER_BASE  = "https://openrouter.ai/api/v1"
PROMPT_VERSION   = "v2-with-quote"

PROMPT_TEMPLATE = """\
You are a sentiment classifier reviewing US government websites for partisan content
or potential Hatch Act violations — for example, statements that attribute government
shutdowns to a political party, praise or attack a political figure by name, or
promote a partisan agenda on an official government site.

Label "partisan" ONLY if there is explicit partisan language: named party attacks,
political blame statements, or campaign-style rhetoric. Factual policy descriptions,
budget information, and neutral references to government actions are "neutral" even
if politically sensitive.

Score: partisan=1.0, neutral=0.0.

Return strict JSON ONLY with these keys:
  "label"          — one of: "partisan", "neutral"
  "score"          — 0.0 or 1.0
  "rationale"      — one short sentence explaining your decision
  "partisan_quote" — if label is "partisan", the exact verbatim text from the page that
                     triggered the classification (max 200 chars). Empty string if neutral.

Text:
{text}
JSON:"""

# ---------------------------------------------------------------------------
# Text utilities (from sentimentor.py)
# ---------------------------------------------------------------------------

def sanitize_text(text: str, max_chars: int = 4000) -> str:
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
    text = re.sub(r'<\|[^>]*\|>', '', text)
    text = re.sub(r"```.*?```", "", text, flags=re.S)
    text = re.sub(r'([^\w\s])\1{3,}', r'\1\1', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:max_chars]


def extract_json_object(s: str) -> dict:
    s = s.strip()
    s = re.sub(r"^```(?:json)?|```$", "", s, flags=re.IGNORECASE | re.MULTILINE).strip()
    s = re.sub(r"<\|[^>]*\|>", "", s)
    try:
        return json.loads(s)
    except Exception:
        pass
    start = s.find("{")
    if start == -1:
        raise ValueError("No JSON object found")
    depth = 0
    for i in range(start, len(s)):
        if s[i] == "{":
            depth += 1
        elif s[i] == "}":
            depth -= 1
            if depth == 0:
                return json.loads(s[start:i+1])
    raise ValueError("No balanced JSON object found")

# ---------------------------------------------------------------------------
# LLM call
# ---------------------------------------------------------------------------

def classify(text: str, model: str, api_key: str, max_chars: int, max_retries: int = 3) -> dict:
    clean = sanitize_text(text, max_chars)
    if not clean:
        return {"label": "unknown", "score": 0.0, "rationale": "empty text",
                "partisan_quote": "", "model": model, "prompt_version": PROMPT_VERSION}

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "Reply with JSON only. No prose, no code fences, no extra tokens."},
            {"role": "user",   "content": PROMPT_TEMPLATE.format(text=clean)},
        ],
        "temperature": 0.0,
        "max_tokens": 128,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    for attempt in range(max_retries):
        try:
            r = requests.post(
                f"{OPENROUTER_BASE}/chat/completions",
                json=payload,
                headers=headers,
                timeout=60,
            )
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 10)) * (attempt + 1)
                print(f"  [RATE LIMIT] waiting {wait}s...", file=sys.stderr)
                time.sleep(wait)
                continue
            r.raise_for_status()
            raw = r.json()["choices"][0]["message"]["content"]
            data = extract_json_object(raw)

            label = str(data.get("label", "")).lower()
            if label not in {"partisan", "neutral"}:
                label = "unknown"
            score = 1.0 if label == "partisan" else 0.0 if label == "neutral" else 0.0
            rationale = str(data.get("rationale", ""))[:400]
            partisan_quote = str(data.get("partisan_quote", ""))[:200] if label == "partisan" else ""

            return {"label": label, "score": score, "rationale": rationale,
                    "partisan_quote": partisan_quote,
                    "model": model, "prompt_version": PROMPT_VERSION}

        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                return {"label": "error", "score": 0.0, "rationale": str(e)[:200],
                        "partisan_quote": "", "model": model, "prompt_version": PROMPT_VERSION}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",     default="master_scrapes.jsonl")
    parser.add_argument("--output",    default="master_scrapes_classified.jsonl")
    parser.add_argument("--model",     default=DEFAULT_MODEL)
    parser.add_argument("--workers",   type=int, default=5)
    parser.add_argument("--max-chars", type=int, default=4000)
    parser.add_argument("--dry-run",   action="store_true")
    args = parser.parse_args()

    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not api_key and not args.dry_run:
        print("ERROR: set OPENROUTER_API_KEY environment variable", file=sys.stderr)
        sys.exit(1)

    in_path  = Path(args.input)
    out_path = Path(args.output)

    # Load already-classified records for resume support.
    # Key: (url, fetched_at) — uniquely identifies a scrape record.
    done: set[tuple] = set()
    if out_path.exists():
        with out_path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    done.add((rec["url"], rec.get("fetched_at", "")))
                except Exception:
                    pass
        print(f"Resuming: {len(done)} records already classified")

    # Load all input records, skip already done
    records = []
    with in_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            key = (rec["url"], rec.get("fetched_at", ""))
            if key not in done:
                records.append(rec)

    total_input = len(done) + len(records)
    print(f"Input:  {total_input} total records")
    print(f"To classify: {len(records)} records")
    print(f"Model:  {args.model}")
    print(f"Workers: {args.workers}")

    if args.dry_run:
        print("\n--- DRY RUN (first 3 records) ---")
        for rec in records[:3]:
            print(f"  {rec['url']} [{rec.get('fetched_at','')[:10]}] "
                  f"words={rec.get('word_count',0)}")
        return

    write_lock = Lock()
    completed = 0
    errors = 0

    def process(rec: dict) -> dict:
        result = classify(rec.get("text", ""), args.model, api_key, args.max_chars)
        rec["sentiment_llm"] = result
        return rec

    with out_path.open("a", encoding="utf-8") as fout:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(process, rec): rec for rec in records}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    with write_lock:
                        fout.write(json.dumps(result, ensure_ascii=False) + "\n")
                        fout.flush()
                        completed += 1
                        snt = result.get("sentiment_llm", {})
                        pct = (len(done) + completed) / total_input * 100
                        print(f"[{len(done)+completed}/{total_input} {pct:.1f}%] "
                              f"{result['url'][:60]} | "
                              f"{snt.get('label','?')} | "
                              f"{result.get('fetched_at','')[:10]}")
                except Exception as e:
                    errors += 1
                    print(f"[ERROR] {e}", file=sys.stderr)

    print(f"\nDone. {completed} classified, {errors} errors -> {out_path}")


if __name__ == "__main__":
    main()
