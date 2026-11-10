#!/usr/bin/env python3
"""Validate catalogue audit decisions and merge them into review artifacts.

Usage: python3 scripts/catalogue_audit_merge.py AUDIT_DIR

AUDIT_DIR contains metadata.jsonl, manifest.json, shards/shard-NN.jsonl and
decisions/shard-NN.jsonl. An optional targeted second pass lives in
rescan-shards/shard-NN.jsonl (candidate rows), rescan-decisions/shard-NN.jsonl
and rescan-manifest.json (who decided each shard). A rescan decision that is
not `keep` replaces the first-pass decision; a rescan `keep` only confirms the
row's source category for the rescan's question and leaves the first pass in
place. Optional overrides.jsonl contains explicitly operator-resolved
decisions; each row must include resolution_reason. Overrides are applied last,
after the complete agent decision sets have been validated. The script does not
infer decisions from product data.
"""
import argparse
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import uuid
from collections import Counter, defaultdict

try:
    from catalogue_audit import CATEGORIES
except ImportError:  # supports importing this file as scripts.catalogue_audit_merge
    from scripts.catalogue_audit import CATEGORIES

DECISION_FIELDS = ('id', 'action', 'from_category', 'reason', 'confidence', 'review_required')
ACTIONS = {'keep', 'recategorize', 'remove'}
CONFIDENCES = {'high', 'medium', 'low'}
SHARD_RE = re.compile(r'^shard-(\d+)\.jsonl$')
OUTPUTS = ('decisions.jsonl', 'corrections.jsonl', 'corrections.csv', 'summary.json', 'report.md', 'samples.json')


class AuditError(ValueError):
    """Input audit data cannot be safely merged."""


def _jsonl(path):
    rows = []
    with path.open(encoding='utf-8') as stream:
        for number, line in enumerate(stream, 1):
            if not line.strip():
                raise AuditError(f'{path}: blank line {number}')
            try:
                row = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise AuditError(f'{path}: invalid JSON at line {number}: {exc}') from exc
            if not isinstance(row, dict):
                raise AuditError(f'{path}: line {number} must be a JSON object')
            row['_input_line'] = number
            rows.append(row)
    return rows


def _clean_record(row):
    return {key: value for key, value in row.items() if key != '_input_line'}


def _read_single_json(path):
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError) as exc:
        raise AuditError(f'Cannot read {path}: {exc}') from exc
    if not isinstance(data, dict):
        raise AuditError(f'{path} must contain a JSON object')
    return data


def _validate_id(value, where):
    if not isinstance(value, str):
        raise AuditError(f'{where}: id must be a UUID string')
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as exc:
        raise AuditError(f'{where}: invalid UUID {value!r}') from exc
    if str(parsed) != value or parsed.version != 4:
        raise AuditError(f'{where}: UUID must be canonical lowercase UUIDv4: {value!r}')


def _validate_decision(raw, expected, shard, where, override=False):
    row = dict(raw)
    row.pop('_input_line', None)
    required = set(DECISION_FIELDS) | ({'resolution_reason'} if override else set())
    missing = required - row.keys()
    if missing:
        raise AuditError(f'{where}: missing required fields: {", ".join(sorted(missing))}')
    _validate_id(row['id'], where)
    if row['id'] != expected['id']:
        raise AuditError(f'{where}: id {row["id"]} does not match owned metadata row {expected["id"]}')
    if not isinstance(row['action'], str) or row['action'] not in ACTIONS:
        raise AuditError(f'{where}: action must be keep, recategorize, or remove')
    if not isinstance(row['from_category'], str) or row['from_category'] not in CATEGORIES:
        raise AuditError(f'{where}: unknown from_category {row["from_category"]!r}')
    if row['from_category'] != expected['category']:
        raise AuditError(f'{where}: from_category does not match metadata category for {row["id"]}')
    if not isinstance(row['reason'], str) or not row['reason'].strip():
        raise AuditError(f'{where}: reason must be a non-empty string')
    if not isinstance(row['confidence'], str) or row['confidence'] not in CONFIDENCES:
        raise AuditError(f'{where}: confidence must be high, medium, or low')
    if not isinstance(row['review_required'], bool):
        raise AuditError(f'{where}: review_required must be a boolean')
    if row['action'] == 'recategorize':
        if not isinstance(row.get('to_category'), str) or row['to_category'] not in CATEGORIES:
            raise AuditError(f'{where}: recategorize requires a known to_category')
        if row['to_category'] == row['from_category']:
            raise AuditError(f'{where}: recategorize cannot be a no-op')
    elif 'to_category' in row and row['to_category'] is not None:
        raise AuditError(f'{where}: to_category is only allowed for recategorize decisions')
    if override and (not isinstance(row['resolution_reason'], str) or not row['resolution_reason'].strip()):
        raise AuditError(f'{where}: resolution_reason must be a non-empty string')
    row['shard'] = shard
    if override:
        row['resolution_reason'] = row['resolution_reason'].strip()
    return row


def _read_shards(audit_dir, manifest):
    shard_paths = sorted((audit_dir / 'shards').glob('shard-*.jsonl'))
    if not shard_paths:
        raise AuditError('No shards/shard-NN.jsonl files found')
    manifest_shards = manifest.get('shards')
    if not isinstance(manifest_shards, list):
        raise AuditError('manifest.json must contain a shards array')
    manifest_names = {entry.get('shard') for entry in manifest_shards if isinstance(entry, dict)}
    if len(manifest_names) != len(manifest_shards):
        raise AuditError('manifest.json has invalid or duplicate shard entries')
    expected_names = {p.stem for p in shard_paths}
    if expected_names != manifest_names:
        raise AuditError('Shard files do not match manifest shard entries')
    owners, shard_rows = {}, {}
    for path in shard_paths:
        match = SHARD_RE.match(path.name)
        if not match:
            raise AuditError(f'Invalid shard filename: {path.name}')
        shard = path.stem
        rows = _jsonl(path)
        shard_rows[shard] = rows
        for row in rows:
            _validate_id(row.get('id'), f'{path}:{row["_input_line"]}')
            if row['id'] in owners:
                raise AuditError(f'Duplicate shard ownership for id {row["id"]}')
            owners[row['id']] = shard
    for entry in manifest_shards:
        if entry.get('records') != len(shard_rows[entry['shard']]):
            raise AuditError(f'Manifest record count differs for {entry["shard"]}')
    return owners, shard_rows


def _read_rescan(audit_dir, metadata):
    """Validate the optional targeted rescan; return {id: decision} with provenance."""
    shard_dir, decision_dir = audit_dir / 'rescan-shards', audit_dir / 'rescan-decisions'
    if not shard_dir.exists() and not decision_dir.exists():
        return {}
    shard_paths = sorted(shard_dir.glob('shard-*.jsonl')) if shard_dir.is_dir() else []
    if not shard_paths:
        raise AuditError('rescan-decisions exists without rescan-shards/shard-NN.jsonl')
    manifest = _read_single_json(audit_dir / 'rescan-manifest.json')
    deciders = manifest.get('shards')
    if not isinstance(deciders, dict):
        raise AuditError('rescan-manifest.json must contain a shards object')
    owners = {}
    for path in shard_paths:
        if not SHARD_RE.match(path.name):
            raise AuditError(f'Invalid rescan shard filename: {path.name}')
        for row in _jsonl(path):
            where = f'{path}:{row["_input_line"]}'
            _validate_id(row.get('id'), where)
            if row['id'] not in metadata:
                raise AuditError(f'{where}: rescan candidate {row["id"]} is not in metadata')
            if row.get('category') != metadata[row['id']]['category']:
                raise AuditError(f'{where}: rescan candidate category differs from metadata')
            if row['id'] in owners:
                raise AuditError(f'Duplicate rescan ownership for id {row["id"]}')
            owners[row['id']] = path.stem
    decision_paths = {path.stem: path for path in decision_dir.glob('shard-*.jsonl')} if decision_dir.is_dir() else {}
    shards = {path.stem for path in shard_paths}
    if set(decision_paths) != shards:
        raise AuditError('Rescan decision files must exactly match the rescan shard files')
    for shard in shards:
        decider = deciders.get(shard)
        if not isinstance(decider, dict) or not all(isinstance(decider.get(k), str) and decider[k] for k in ('provider', 'model')):
            raise AuditError(f'rescan-manifest.json must name the provider and model for {shard}')
    decisions = {}
    for shard, path in sorted(decision_paths.items()):
        for raw in _jsonl(path):
            where = f'{path}:{raw["_input_line"]}'
            record_id = raw.get('id')
            if record_id not in owners:
                raise AuditError(f'{where}: rescan decision has extra or unknown id {record_id!r}')
            if record_id in decisions:
                raise AuditError(f'{where}: duplicate rescan decision for id {record_id}')
            if owners[record_id] != shard:
                raise AuditError(f'{where}: id {record_id} belongs to rescan {owners[record_id]}, not {shard}')
            decision = _validate_decision(raw, metadata[record_id], shard, where)
            decision['provenance'] = {**{k: deciders[shard][k] for k in ('provider', 'model', 'effort', 'note')
                                         if isinstance(deciders[shard].get(k), str)},
                                      'type': 'rescan_decision', 'shard': shard}
            decisions[record_id] = decision
    if len(decisions) != len(owners):
        raise AuditError(f'Missing rescan decisions for {len(owners) - len(decisions)} candidates')
    return decisions


def _serialize_jsonl(rows):
    return ''.join(json.dumps(row, ensure_ascii=False, separators=(',', ':')) + '\n' for row in rows).encode('utf-8')


def _atomic_write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f'.{path.name}.', suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
    except BaseException:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def merge(audit_dir):
    audit_dir = Path(audit_dir)
    metadata_path = audit_dir / 'metadata.jsonl'
    manifest_path = audit_dir / 'manifest.json'
    if not metadata_path.is_file() or not manifest_path.is_file():
        raise AuditError('Audit directory must contain metadata.jsonl and manifest.json')
    manifest = _read_single_json(manifest_path)
    metadata_rows = _jsonl(metadata_path)
    metadata = {}
    for row in metadata_rows:
        where = f'metadata.jsonl:{row["_input_line"]}'
        _validate_id(row.get('id'), where)
        if row['id'] in metadata:
            raise AuditError(f'Duplicate metadata id {row["id"]}')
        if not isinstance(row.get('category'), str) or row['category'] not in CATEGORIES:
            raise AuditError(f'{where}: unknown category')
        metadata[row['id']] = _clean_record(row)
    if manifest.get('records') != len(metadata):
        raise AuditError(f'Manifest says {manifest.get("records")} records; metadata has {len(metadata)}')

    owners, shard_rows = _read_shards(audit_dir, manifest)
    if set(owners) != set(metadata):
        missing = len(set(metadata) - set(owners))
        extra = len(set(owners) - set(metadata))
        raise AuditError(f'Shard membership does not cover metadata (missing={missing}, extra={extra})')
    for shard, rows in shard_rows.items():
        for row in rows:
            meta = metadata[row['id']]
            if _clean_record(row) != meta:
                raise AuditError(f'{shard}:{row["_input_line"]}: shard row differs from metadata for {row["id"]}')

    decisions_dir = audit_dir / 'decisions'
    decision_paths = {path.stem: path for path in decisions_dir.glob('shard-*.jsonl')} if decisions_dir.is_dir() else {}
    if set(decision_paths) != set(shard_rows):
        raise AuditError('Decision shard files must exactly match the input shard files')
    agent_decisions = {}
    for shard, path in decision_paths.items():
        for raw in _jsonl(path):
            where = f'{path}:{raw["_input_line"]}'
            decision_id = raw.get('id')
            if decision_id not in metadata:
                raise AuditError(f'{where}: decision has extra or unknown id {decision_id!r}')
            if decision_id in agent_decisions:
                raise AuditError(f'{where}: duplicate decision for id {decision_id}')
            expected_shard = owners[decision_id]
            if expected_shard != shard:
                raise AuditError(f'{where}: id {decision_id} belongs to {expected_shard}, not {shard}')
            agent_decisions[decision_id] = _validate_decision(raw, metadata[decision_id], shard, where)
    if len(agent_decisions) != len(metadata):
        absent = set(metadata) - set(agent_decisions)
        raise AuditError(f'Missing decisions for {len(absent)} metadata rows (first: {next(iter(absent), "")})')

    final_decisions = dict(agent_decisions)
    for decision in agent_decisions.values():
        decision['provenance'] = {'type': 'agent_decision'}
    rescan = _read_rescan(audit_dir, metadata)
    rescan_stats = Counter()
    for record_id, decision in rescan.items():
        first = agent_decisions[record_id]
        if decision['action'] == 'keep':
            rescan_stats['keep_left_first_pass'] += 1
            if first['action'] != 'keep':
                rescan_stats['keep_vs_first_pass_change'] += 1
            continue
        if first['action'] != 'keep' and (first['action'], first.get('to_category')) != (decision['action'], decision.get('to_category')):
            rescan_stats['replaced_first_pass_change'] += 1
        rescan_stats['applied'] += 1
        final_decisions[record_id] = decision
    overrides_path = audit_dir / 'overrides.jsonl'
    seen_overrides = set()
    if overrides_path.exists():
        for raw in _jsonl(overrides_path):
            where = f'{overrides_path}:{raw["_input_line"]}'
            record_id = raw.get('id')
            if record_id not in metadata:
                raise AuditError(f'{where}: override has unknown id {record_id!r}')
            if record_id in seen_overrides:
                raise AuditError(f'{where}: duplicate override for id {record_id}')
            seen_overrides.add(record_id)
            shard = owners[record_id]
            override = _validate_decision(raw, metadata[record_id], shard, where, override=True)
            override['provenance'] = {'type': 'operator_resolution', 'resolution_reason': override['resolution_reason']}
            final_decisions[record_id] = override

    merged_rows, corrections = [], []
    for record_id, meta in metadata.items():
        decision = final_decisions[record_id]
        result = dict(decision)
        result['metadata'] = meta
        merged_rows.append(result)
        if decision['action'] != 'keep':
            correction = {key: decision.get(key) for key in ('id', 'action', 'from_category', 'to_category')
                          if key != 'to_category' or decision['action'] == 'recategorize'}
            correction.update({key: meta.get(key) for key in ('name', 'source_sha1', 'source_line')})
            correction.update({key: decision[key] for key in ('reason', 'confidence', 'review_required')})
            correction['provenance'] = result['provenance']
            corrections.append(correction)

    action_counts = Counter(row['action'] for row in merged_rows)
    source_action = Counter((row['from_category'], row['action']) for row in merged_rows)
    transitions = Counter((row['from_category'], row['to_category']) for row in merged_rows if row['action'] == 'recategorize')
    final_categories = Counter()
    for row in merged_rows:
        if row['action'] != 'remove':
            final_categories[row.get('to_category') if row['action'] == 'recategorize' else row['from_category']] += 1
    confidence_counts = Counter(row['confidence'] for row in merged_rows)
    review_counts = Counter({str(flag).lower(): sum(row['review_required'] is flag for row in merged_rows) for flag in (True, False)})
    sha256 = manifest.get('source_sha256')
    if not isinstance(sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', sha256):
        raise AuditError('manifest.json must contain a lowercase source_sha256')
    decisions_bytes = _serialize_jsonl(merged_rows)
    corrections_bytes = _serialize_jsonl(corrections)
    csv_buffer = io.StringIO(newline='')
    csv_fields = ('id', 'action', 'from_category', 'to_category', 'name', 'reason', 'confidence', 'review_required', 'source_sha1', 'source_line')
    writer = csv.DictWriter(csv_buffer, fieldnames=csv_fields, lineterminator='\n')
    writer.writeheader()
    for row in corrections:
        writer.writerow({field: row.get(field, '') for field in csv_fields})
    csv_bytes = csv_buffer.getvalue().encode('utf-8')
    summary = {
        'records': len(metadata), 'coverage': {'metadata': len(metadata), 'decisions': len(merged_rows), 'complete': len(merged_rows) == len(metadata)},
        'actions': dict(sorted(action_counts.items())),
        'source_category_action': {f'{cat}/{action}': count for (cat, action), count in sorted(source_action.items())},
        'recategorization_transitions': {f'{src}->{dst}': count for (src, dst), count in sorted(transitions.items())},
        'final_category_totals': dict(sorted(final_categories.items())),
        'confidence': dict(sorted(confidence_counts.items())), 'review_required': dict(sorted(review_counts.items())),
        'unresolved_review_required': sum(row['review_required'] for row in merged_rows),
        'source_sha256': sha256, 'corrections_sha256': hashlib.sha256(corrections_bytes).hexdigest(),
        'overrides': sum(row['provenance']['type'] == 'operator_resolution' for row in merged_rows),
        'provenance': dict(sorted(Counter(row['provenance']['type'] for row in merged_rows).items())),
        'rescan': {'candidates': len(rescan), 'actions': dict(sorted(Counter(d['action'] for d in rescan.values()).items())),
                   **dict(sorted(rescan_stats.items()))},
    }
    removals = [row for row in merged_rows if row['action'] == 'remove'][:12]
    recategorized = [row for row in merged_rows if row['action'] == 'recategorize'][:12]
    flagged_keeps = [row for row in merged_rows if row['action'] == 'keep' and row['review_required']][:8]
    by_target = {target: [row for row in merged_rows if row['action'] == 'recategorize' and row['to_category'] == target][:12]
                 for target in ('mini_pc', 'aio_pc')}
    rescan_removals = [row for row in merged_rows if row['action'] == 'remove' and row['provenance']['type'] != 'agent_decision'][:12]
    examples = {'remove': removals, 'recategorize': recategorized, 'flagged_keeps': flagged_keeps,
                'to_mini_pc': by_target['mini_pc'], 'to_aio_pc': by_target['aio_pc'], 'rescan_or_operator_remove': rescan_removals}
    special = [row for row in merged_rows if 'GE GN610' in ' '.join(
        str(value) for value in row['metadata'].values()).upper()]
    samples = {'examples': examples, 'GE_GN610': special}
    artifacts = {
        'decisions.jsonl': decisions_bytes,
        'corrections.jsonl': corrections_bytes,
        'corrections.csv': csv_bytes,
        'summary.json': (json.dumps(summary, ensure_ascii=False, indent=2) + '\n').encode('utf-8'),
        'samples.json': (json.dumps(samples, ensure_ascii=False, indent=2) + '\n').encode('utf-8'),
    }
    paths = {name: str(audit_dir / name) for name in OUTPUTS}
    report_lines = ['# Kết quả gộp kiểm tra danh mục', '',
                    f'- Tổng sản phẩm: **{len(metadata)}**; quyết định: **{len(merged_rows)}** (độ phủ đầy đủ).',
                    f'- Giữ nguyên: **{action_counts["keep"]}**; đổi danh mục: **{action_counts["recategorize"]}**; loại bỏ: **{action_counts["remove"]}**.',
                    f'- Cần xem xét thêm: **{summary["unresolved_review_required"]}**. Đây chưa phải phê duyệt cuối cùng.',
                    f'- Quét lại (rescan): **{len(rescan)}** ứng viên, **{rescan_stats["applied"]}** quyết định thay lượt 1.',
                    f'- Ghi đè do người vận hành: **{summary["overrides"]}**.', '', '## Số lượng theo danh mục cuối cùng', '', '| Danh mục | Số lượng |', '|---|---:|']
    report_lines += [f'| {cat} | {count} |' for cat, count in sorted(final_categories.items())]
    report_lines += ['', '## Mẫu quyết định', '']
    for title, rows in (('Loại bỏ (tối đa 12)', removals), ('Đổi danh mục (tối đa 12)', recategorized),
                        ('Sang Mini PC (tối đa 12)', by_target['mini_pc']), ('Sang PC All-in-One (tối đa 12)', by_target['aio_pc']),
                        ('Loại bỏ từ rescan/người dùng (tối đa 12)', rescan_removals), ('Giữ nhưng cần xem xét (tối đa 8)', flagged_keeps)):
        report_lines += [f'### {title}', '']
        report_lines.extend([f'- `{row["id"]}` — {row["metadata"].get("name", "")} — {row["reason"]}' for row in rows] or ['- Không có.'])
        report_lines.append('')
    report_lines += ['## Tệp kết quả', ''] + [f'- `{paths[name]}`' for name in OUTPUTS]
    artifacts['report.md'] = ('\n'.join(report_lines) + '\n').encode('utf-8')
    # All reads, validation, and serialization finish before the first output write.
    for name in OUTPUTS:
        if name in artifacts:
            _atomic_write(audit_dir / name, artifacts[name])
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('audit_dir', type=Path)
    args = parser.parse_args(argv)
    try:
        print(json.dumps(merge(args.audit_dir), ensure_ascii=False, indent=2))
    except (AuditError, OSError) as exc:
        print(f'catalogue_audit_merge: {exc}', file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
