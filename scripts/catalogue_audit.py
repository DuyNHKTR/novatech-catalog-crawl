#!/usr/bin/env python3
"""Extract image-free catalogue metadata and deterministic review shards.

The source is opened read-only, processed one JSON line at a time, and hashed.
No image payload is decoded or written. Review decisions are made separately.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import shutil
import unicodedata

CATEGORIES = ('laptop monitor gpu motherboard prebuilt_pc keyboard audio_headset ram case '
              'mouse cooler cpu psu ssd network hdd accessory fan webcam mini_pc aio_pc').split()


def ascii_text(value):
    return ''.join(c for c in unicodedata.normalize('NFD', value.lower().replace('đ', 'd'))
                   if unicodedata.category(c) != 'Mn')


def suspect_reasons(row):
    """Ranking hints only; these never become keep/remove decisions."""
    name = ascii_text(row['name'])
    hints = []
    household = r'may loc nuoc|may giat|tu lanh|noi com|lo vi song|bep tu|bep dien|may hut bui|may rua bat|dieu hoa|may lanh|may say toc|noi chien|am sieu toc|may xay|may ep|may loc khong khi|ban ui|ban la|binh nong lanh'
    if re.search(household, name):
        hints.append('household keyword')
    rules = {
        'laptop': r'\blaptop\b|\bmacbook\b|\bchromebook\b',
        'monitor': r'man hinh|\bmonitor\b',
        'gpu': r'card do hoa|card man hinh|\bvga\b|\bgeforce\b|\brtx\s*\d|\bgtx\s*\d|\bradeon\b',
        'motherboard': r'\bmainboard\b|bo mach chu',
        'keyboard': r'ban phim|\bkeyboard\b',
        'mouse': r'\bchuot\b|\bmouse\b',
        'audio_headset': r'tai nghe|\bheadset\b|\bheadphone\b|\bloa\b|\bspeaker\b',
        'cooler': r'tan nhiet|\bcooler\b|\baio\b',
        'cpu': r'bo vi xu ly|\bcpu\b|\bprocessor\b',
        'psu': r'\bnguon\b|\bpsu\b|power supply',
        'webcam': r'\bwebcam\b',
    }
    hits = [cat for cat, pattern in rules.items() if re.search(pattern, name)]
    if row['category'] not in hits and hits:
        hints.append('name suggests ' + ','.join(hits))
    return hints


def validate_metadata_record(record, line_number):
    if not isinstance(record, dict):
        raise ValueError(f'Invalid record at line {line_number}: expected JSON object')
    for field in ('id', 'category', 'brand', 'name'):
        if not isinstance(record.get(field), str):
            raise ValueError(f'Invalid record at line {line_number}: {field} must be a string')
    if not isinstance(record.get('specs'), dict):
        raise ValueError(f'Invalid record at line {line_number}: specs must be an object')
    if record['category'] not in CATEGORIES:
        raise ValueError(f'Unknown category at line {line_number}')


def write_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n')


def extract(source, out, shards):
    if out.exists() and any(out.iterdir()):
        raise ValueError('Output directory must be empty (refusing to overwrite an audit).')
    created_output = not out.exists()
    out.mkdir(parents=True, exist_ok=True)
    rows, counts, digest = [], Counter(), hashlib.sha256()
    before = source.stat()
    try:
        with source.open('rb') as src, (out / 'metadata.jsonl').open('w') as dst:
            for number, line in enumerate(src, 1):
                digest.update(line)
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f'Invalid JSON at line {number}: {exc}') from exc
                validate_metadata_record(record, number)
                row = {key: record.get(key) for key in
                       ('id', 'category', 'brand', 'name', 'model', 'mpn', 'gtin', 'specs')}
                row['source_line'] = number
                row['source_sha1'] = hashlib.sha1(line).hexdigest()
                row['suspect_reasons'] = suspect_reasons(row)
                dst.write(json.dumps(row, ensure_ascii=False, separators=(',', ':')) + '\n')
                rows.append(row)
                counts[row['category']] += 1
                if number % 2000 == 0:
                    print(f'Extracted {number} metadata records', flush=True)
    except Exception:
        if created_output:
            shutil.rmtree(out, ignore_errors=True)
        raise
    try:
        after = source.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise ValueError('Source changed during extraction')
        if len({r['id'] for r in rows}) != len(rows):
            raise ValueError('Duplicate source ids')
        # Keep related products together and put suspect rows first within categories.
        rows.sort(key=lambda r: (r['category'], -len(r['suspect_reasons']), r['name'], r['id']))
        (out / 'shards').mkdir()
        (out / 'decisions').mkdir()
        manifest = []
        for index in range(shards):
            part = rows[len(rows) * index // shards:len(rows) * (index + 1) // shards]
            name = f'shard-{index + 1:02d}'
            with (out / 'shards' / f'{name}.jsonl').open('w') as dst:
                for row in part:
                    dst.write(json.dumps(row, ensure_ascii=False, separators=(',', ':')) + '\n')
            # Human/agent-readable index; full specs are in the shard above.
            with (out / 'shards' / f'{name}.tsv').open('w') as dst:
                for n, row in enumerate(part, 1):
                    fields = [str(n), row['id'], row['category'], row['name'], str(row['model'] or ''),
                              '; '.join(row['suspect_reasons'])]
                    dst.write('\t'.join(v.replace('\t', ' ').replace('\n', ' ') for v in fields) + '\n')
            manifest.append({'shard': name, 'records': len(part), 'categories': dict(Counter(r['category'] for r in part))})
        write_json(out / 'manifest.json', {'source': str(source.resolve()), 'source_bytes': before.st_size,
                   'source_sha256': digest.hexdigest(), 'records': len(rows), 'categories': dict(counts),
                   'ranked_suspects': sum(bool(r['suspect_reasons']) for r in rows), 'shards': manifest})
        print(json.dumps({'records': len(rows), 'metadata_bytes': (out / 'metadata.jsonl').stat().st_size,
                          'source_sha256': digest.hexdigest(), 'shards': shards}), flush=True)

    except Exception:
        if created_output:
            shutil.rmtree(out, ignore_errors=True)
        raise

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--shards', type=int, default=40)
    args = parser.parse_args()
    if not 1 <= args.shards <= 100:
        parser.error('--shards must be between 1 and 100')
    extract(args.source, args.output, args.shards)
