#!/usr/bin/env python3
"""Deterministically rank service, Mini PC, and All-in-One candidates.

Input is the image-free metadata JSONL produced by catalogue_audit.py. The
output is only a review queue; agents must make the final decisions.
"""
import argparse
import json
import re
from pathlib import Path
import unicodedata

# Patterns run on accent-folded text (see ascii_text), so one spelling covers both forms.
SERVICE = re.compile(r'\b(sua chua|lap dat|ve sinh|bao hanh|thay the|cai dat|nap muc|bao tri|dich vu|gia han|bao ve tai nan|bao hiem|'
                     r'service|installation|repair|cleaning|warranty|care\s*pack)\b', re.I)
MINI = re.compile(r'\b(mini\s*pc|mini-pc|nuc|beelink|cubi|tiny|(?:pro|elite|prodesk|elitedesk|z2|ideacentre|expertcenter)\s*mini|'
                  r'optiplex\s*micro|micro\s*form\s*factor|asus\s*pn\d|mac\s*mini|macmini|mac\s*studio|deskmini|zbox|brix|'
                  r'mini computer|micro pc)\b', re.I)
AIO = re.compile(r'\b(all\s*in\s*one|all-in-one|aio|imac|onebot\s*aio|proone|eliteone|ideacentre\s*aio|thinkcentre\s*aio|vivocentre|aspire\s*aio|zen\s*aios?)\b', re.I)
MONITOR = re.compile(r'\b(monitor|man hinh|display|lcd|led)\b', re.I)



def ascii_text(value):
    value = str(value or '').replace('đ', 'd').replace('Đ', 'D')
    return unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode().lower()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('metadata', type=Path)
    ap.add_argument('output', type=Path)
    args = ap.parse_args()
    counts = {'service': 0, 'mini_pc': 0, 'aio_pc': 0, 'overlap': 0}
    with args.output.open('w') as out:
        for row in map(json.loads, args.metadata.open()):
            text = ascii_text(' '.join([str(row.get('name') or ''), str(row.get('model') or ''),
                                        ' '.join(str(v) for v in (row.get('specs') or {}).values())]))
            service = bool(SERVICE.search(text))
            mini = bool(MINI.search(text))
            aio = bool(AIO.search(text))
            # A monitor-only listing is not an AIO candidate merely because its
            # marketing copy says LCD/LED. Computer evidence wins.
            aio_pc = aio and not (MONITOR.search(text) and not re.search(r'\b(cpu|core\s*i[3579]|ryzen|ram|memory|ssd|nvme|windows|macos|mac\s*os|os)\b', text, re.I))
            mini_pc = mini and not aio_pc
            if not (service or mini_pc or aio_pc):
                continue
            reasons = []
            if service: reasons.append('service keyword')
            if mini_pc: reasons.append('mini-pc keyword')
            if aio_pc: reasons.append('aio-computer keyword')
            if mini_pc and aio_pc: counts['overlap'] += 1
            for key, yes in [('service', service), ('mini_pc', mini_pc), ('aio_pc', aio_pc)]:
                if yes: counts[key] += 1
            out.write(json.dumps({'id': row['id'], 'category': row['category'], 'brand': row.get('brand'),
                                  'name': row['name'], 'model': row.get('model'), 'mpn': row.get('mpn'),
                                  'gtin': row.get('gtin'), 'specs': row.get('specs'),
                                  'source_line': row['source_line'], 'source_sha1': row['source_sha1'],
                                  'candidate_reasons': reasons}, ensure_ascii=False, separators=(',', ':')) + '\n')
    print(json.dumps(counts, ensure_ascii=False))


if __name__ == '__main__':
    main()
