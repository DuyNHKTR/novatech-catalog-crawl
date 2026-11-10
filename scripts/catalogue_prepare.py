#!/usr/bin/env python3
"""Convert the source catalogue JSONL into an import-ready `.novacat` package.

    uv run --with pillow scripts/catalogue_prepare.py IN.jsonl OUT.novacat \
        [--workers N] [--limit N] [--skip N]
    uv run --with pillow scripts/catalogue_prepare.py --verify OUT.novacat

Every base64 image is decoded once and re-encoded as WebP (card 480, large
1000, zoom 1600 only when the original is larger than 1000 px), so the website
import does no image processing. Next to the package it writes
`OUT.novacat.report.json` (counts, skipped images/rows, spec label frequencies
per category).

Package layout (read by the /manage/ import, wordpress/plugins/novatech-commerce/includes/catalog/catalogue.php):

    b"NOVACAT 1\\n"
    per record: one JSON line + b"\\n", then `blob` bytes
        {"id","category","brand","name","model","mpn","gtin"?,"specs",
         "images":[{"card":[bytes,w,h],"large":[bytes,w,h],"zoom":[bytes,w,h]|null}],
         "blob":total bytes,"src":sha1 of the source line}
        blob = card, large, zoom (if any) of image 0, then image 1, ...; all WebP
    footer JSON {"format":"novacat","version":1,"package_id","records","images","files",
                 "end","offsets":[record start, ...],"categories","max_record_bytes","source"}
    uint64 little-endian = footer JSON length

A record never exceeds 16 MiB (trailing gallery images are dropped and reported).
"""

import argparse
import base64
import binascii
import hashlib
import io
import json
import os
import re
import signal
import struct
import sys
import time
import uuid
import warnings
from collections import Counter, defaultdict, deque
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from datetime import datetime, timezone

MAGIC = b"NOVACAT 1\n"
RECORD_CAP = 16 * 1024 * 1024
# (name, longest side, WebP quality), largest first. Zoom only for originals > 1000.
SIZES = (("zoom", 1600, 82), ("large", 1000, 80), ("card", 480, 78))
UUID4 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
TOP_LABELS = 60
MAX_PIXELS = 150_000_000  # larger images are skipped (a worker would need > 1 GB)
# EXIF orientation -> Image.Transpose value (applied by hand: ImageOps.exif_transpose copies the image)
TRANSPOSE = {2: 0, 3: 3, 4: 1, 5: 5, 6: 4, 7: 6, 8: 2}


def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


# ---------------------------------------------------------------- worker side

def validate(row):
    """Return an error string, or None when the row matches the schema."""
    if not isinstance(row, dict):
        return "row is not an object"
    if not isinstance(row.get("id"), str) or not UUID4.match(row["id"]):
        return "id is not a lowercase uuid4"
    for key in ("category", "brand", "name", "model"):
        if not isinstance(row.get(key), str) or not row[key]:
            return f"{key} missing or empty"
    if "mpn" not in row or not (row["mpn"] is None or isinstance(row["mpn"], str)):
        return "mpn must be string or null"
    if "gtin" in row and not isinstance(row["gtin"], str):
        return "gtin must be a string"
    specs = row.get("specs")
    if not isinstance(specs, dict) or not all(isinstance(v, str) and v for v in specs.values()):
        return "specs must be an object of non-empty strings"
    if not isinstance(row.get("images"), list):
        return "images must be a list"
    return None


def fit(im, longest):
    """Downscale (LANCZOS) so the longest side is `longest`; never upscale."""
    from PIL import Image

    w, h = im.size
    top = max(w, h)
    if top <= longest:
        return im
    return im.resize((max(1, round(w * longest / top)), max(1, round(h * longest / top))), Image.LANCZOS)


def convert_image(raw):
    """Decode one image and return ({name: (bytes, w, h)}, None) for card/large/zoom."""
    from PIL import Image

    im = Image.open(io.BytesIO(raw))
    top = max(im.size)
    want_zoom = top > 1000
    orientation = im.getexif().get(0x0112, 1)
    if im.format in ("JPEG", "MPO"):
        # Decode at reduced size when the JPEG allows it, keeping longest side >= target.
        target = min(top, 1600 if want_zoom else 1000)
        w, h = im.size
        im.draft(None, (-(-w * target // top), -(-h * target // top)))
    alpha = "A" in im.getbands() or (im.mode == "P" and "transparency" in im.info)
    mode = "RGBA" if alpha else "RGB"
    factor = max(im.size) // 1600
    if factor >= 2:
        # Huge image: box-reduce first (keeps the longest side >= 1600) so we never hold
        # a full-size RGBA copy.
        if im.mode not in ("RGB", "RGBA", "L", "LA"):
            im = im.convert(mode)
        im = im.reduce(factor)
    if orientation in TRANSPOSE:
        im = im.transpose(TRANSPOSE[orientation])
    im = im.convert(mode) if im.mode != mode else im

    out = {}
    for name, longest, quality in SIZES:
        if name == "zoom" and not want_zoom:
            continue
        im = fit(im, longest)  # each step starts from the previous (larger) one
        buf = io.BytesIO()
        im.save(buf, "WEBP", quality=quality, method=4)
        out[name] = (buf.getvalue(), im.size[0], im.size[1])
    return out


def build_json(row, images, blob_len, src):
    rec = {"id": row["id"], "category": row["category"], "brand": row["brand"],
           "name": row["name"], "model": row["model"], "mpn": row["mpn"]}
    if "gtin" in row:
        rec["gtin"] = row["gtin"]
    rec["specs"] = row["specs"]
    rec["images"] = images
    rec["blob"] = blob_len
    rec["src"] = src
    return dumps(rec)


def process_row(task):
    """Worker: one source line -> a result dict (record bytes, or an invalid reason)."""
    from PIL import Image, PngImagePlugin

    Image.MAX_IMAGE_PIXELS = MAX_PIXELS // 2  # Pillow raises above 2x this, warns above 1x
    # Some shop PNGs carry multi-MB metadata (iTXt/zTXt) chunks; the default 1 MB cap rejects the whole picture.
    PngImagePlugin.MAX_TEXT_CHUNK = PngImagePlugin.MAX_TEXT_MEMORY = 64 * 1024 * 1024
    warnings.simplefilter("ignore", Image.DecompressionBombWarning)
    index, line = task
    line = line.rstrip(b"\r\n") if line.endswith(b"\n") else line
    res = {"index": index, "in_bytes": len(line), "skipped": [], "truncated": 0}
    try:
        row = json.loads(line)
    except (ValueError, RecursionError) as e:
        res["invalid"] = f"bad JSON: {e}"[:200]
        return res
    err = validate(row)
    if err:
        res["invalid"] = err
        return res
    res.update(id=row["id"], category=row["category"], labels=list(row["specs"]))
    src = hashlib.sha1(line).hexdigest()

    images, blobs, blob_len, n_files = [], [], 0, 0
    for i, img in enumerate(row["images"]):
        try:
            if not isinstance(img, dict) or not isinstance(img.get("data"), str):
                raise ValueError("image entry has no base64 data")
            raw = base64.b64decode(img["data"], validate=True)
            files = convert_image(raw)
        except (binascii.Error, ValueError, OSError, Image.DecompressionBombError, SyntaxError, MemoryError,
                OverflowError, TypeError) as e:
            res["skipped"].append({"row": index, "id": row["id"], "image": i, "reason": f"{type(e).__name__}: {e}"[:200]})
            continue
        entry = {k: [len(v[0]), v[1], v[2]] for k, v in files.items()}
        entry = {"card": entry["card"], "large": entry["large"], "zoom": entry.get("zoom")}
        added = sum(len(v[0]) for v in files.values())
        trial = build_json(row, images + [entry], blob_len + added, src)
        if len(trial) + 1 + blob_len + added > RECORD_CAP:
            res["truncated"] = len(row["images"]) - i  # this image and all after it (skipped ones included)
            break
        images.append(entry)
        blob_len += added
        n_files += len(files)
        for name in ("card", "large", "zoom"):
            if name in files:
                blobs.append(files[name][0])
    try:
        head = build_json(row, images, blob_len, src)
    except UnicodeEncodeError as e:
        res["invalid"] = f"cannot encode as UTF-8: {e}"[:200]
        return res
    if len(head) + 1 + blob_len > RECORD_CAP:
        res["invalid"] = "record exceeds the 16 MiB cap even without images"
        return res
    res.update(record=head + b"\n" + b"".join(blobs), images=len(images), files=n_files,
               src_images=len(row["images"]))
    return res


# ------------------------------------------------------------------ main side

def worker_init():
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # only the main process reacts to Ctrl-C


def read_rows(path, skip, limit, bounds):
    """Yield (index, line) for the requested slice of the input."""
    n = 0
    with open(path, "rb") as f:
        for _ in range(skip):
            if not f.readline():
                return
        bounds["skip_bytes"] = f.tell()
        while limit is None or n < limit:
            line = f.readline()
            if not line:
                return
            if not line.strip():
                continue
            yield skip + n, line
            n += 1


def fmt_time(s):
    s = int(s)
    return f"{s // 3600}:{s % 3600 // 60:02d}:{s % 60:02d}"


def prepare(args):
    src_path, out_path = args.input, args.output
    workers = args.workers or max(1, (os.cpu_count() or 2) - 1)
    src_size = os.path.getsize(src_path)
    part = out_path + ".part"
    window = 4 * workers  # rows in flight: bounds memory, the reader never races ahead
    bounds = {"skip_bytes": 0}
    started = time.time()

    offsets, categories = [], Counter()
    seen_ids = set()
    labels = defaultdict(Counter)
    totals = Counter()
    skipped, invalid, truncated = [], [], []
    max_record = 0
    pos = len(MAGIC)
    last_log = started
    pool = ProcessPoolExecutor(workers, initializer=worker_init, max_tasks_per_child=200)

    def ordered_results():
        rows = read_rows(src_path, args.skip, args.limit, bounds)
        pending = deque()
        while True:
            while len(pending) < window and (task := next(rows, None)) is not None:
                pending.append(pool.submit(process_row, task))
            if not pending:
                return
            yield pending.popleft().result()  # raises BrokenProcessPool if a worker died

    try:
        with open(part, "wb", buffering=1 << 20) as out:
            out.write(MAGIC)
            for res in ordered_results():
                totals["rows"] += 1
                totals["in_bytes"] += res["in_bytes"]
                skipped += res["skipped"]
                if "invalid" in res:
                    invalid.append({"row": res["index"], "id": res.get("id"), "reason": res["invalid"]})
                else:
                    rec = res["record"]
                    if res["id"] in seen_ids:
                        invalid.append({"row": res["index"], "id": res["id"], "reason": "duplicate id"})
                        continue
                    seen_ids.add(res["id"])
                    offsets.append(pos)
                    out.write(rec)
                    pos += len(rec)
                    max_record = max(max_record, len(rec))
                    categories[res["category"]] += 1
                    labels[res["category"]].update(res["labels"])
                    totals["images"] += res["images"]
                    totals["files"] += res["files"]
                    totals["src_images"] += res["src_images"]
                    if res["truncated"]:
                        truncated.append({"row": res["index"], "id": res["id"], "dropped_images": res["truncated"]})
                now = time.time()
                if now - last_log >= 5:
                    last_log = now
                    elapsed = now - started
                    if args.limit:
                        frac = totals["rows"] / args.limit
                    else:
                        frac = totals["in_bytes"] / max(1, src_size - bounds["skip_bytes"])
                    eta = elapsed * (1 - frac) / frac if frac > 0 else 0
                    print(f"rows {totals['rows']}  images {totals['images']}  {pos / 1e6:.0f} MB  "
                          f"{totals['images'] / elapsed:.1f} img/s  {totals['rows'] / elapsed:.1f} rows/s  "
                          f"{frac * 100:.1f}%  ETA {fmt_time(eta)}", file=sys.stderr, flush=True)
            footer = dumps({
                "format": "novacat", "version": 1, "package_id": str(uuid.uuid4()),
                "created": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "records": len(offsets), "images": totals["images"], "files": totals["files"],
                "end": pos, "offsets": offsets, "categories": dict(sorted(categories.items())),
                "max_record_bytes": max_record,
                "source": {"name": os.path.basename(src_path), "bytes": src_size},
            })
            out.write(footer + struct.pack("<Q", len(footer)))
            total_bytes = pos + len(footer) + 8
        pool.shutdown()
        os.replace(part, out_path)
    except BaseException:
        # Don't wait for in-flight rows: drop queued work and kill the workers, then clean up.
        procs = list((getattr(pool, "_processes", None) or {}).values())
        pool.shutdown(wait=False, cancel_futures=True)
        for proc in procs:
            proc.kill()
        if os.path.lexists(part):
            os.remove(part)
        raise

    elapsed = time.time() - started
    report = {
        "package": os.path.basename(out_path), "elapsed_seconds": round(elapsed, 1), "workers": workers,
        "images_per_second": round(totals["images"] / elapsed, 2) if elapsed else 0,
        "counts": {"rows_read": totals["rows"], "records": len(offsets), "images": totals["images"],
                   "files": totals["files"], "source_images": totals["src_images"],
                   "skipped_images": len(skipped), "invalid_rows": len(invalid),
                   "truncated_records": len(truncated), "total_bytes": total_bytes,
                   "max_record_bytes": max_record, "categories": dict(sorted(categories.items()))},
        "skipped_images": skipped, "invalid_rows": invalid, "truncated_records": truncated,
        "spec_labels": {c: labels[c].most_common(TOP_LABELS) for c in sorted(labels)},
    }
    with open(out_path + ".report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=1)
    print(f"done: {len(offsets)} records, {totals['images']} images, {totals['files']} files, "
          f"{total_bytes / 1e6:.1f} MB in {fmt_time(elapsed)} ({report['images_per_second']} img/s); "
          f"skipped images {len(skipped)}, invalid rows {len(invalid)}, truncated {len(truncated)}", file=sys.stderr)


# --------------------------------------------------------------------- verify

def verify(path):
    problems = []

    def bad(msg):
        if len(problems) < 20:
            problems.append(msg)

    size = os.path.getsize(path)
    with open(path, "rb") as f:
        if f.read(len(MAGIC)) != MAGIC:
            sys.exit("bad magic")
        f.seek(size - 8)
        (flen,) = struct.unpack("<Q", f.read(8))
        f.seek(size - 8 - flen)
        footer = json.loads(f.read(flen))
        offsets, end = footer["offsets"], footer["end"]
        if end != size - 8 - flen:
            bad(f"footer end {end} != {size - 8 - flen}")
        if footer["format"] != "novacat" or footer["version"] != 1 or len(offsets) != footer["records"]:
            bad("footer fields inconsistent")
        if offsets and offsets[0] != len(MAGIC):
            bad("first offset is not 10")
        images = files = max_record = 0
        cats = Counter()
        for i, off in enumerate(offsets):
            nxt = offsets[i + 1] if i + 1 < len(offsets) else end
            if nxt <= off:
                bad(f"record {i}: offsets not increasing")
                continue
            f.seek(off)
            line = f.readline()
            try:
                rec = json.loads(line)
            except ValueError:
                bad(f"record {i}: bad JSON")
                continue
            span = nxt - off
            max_record = max(max_record, span)
            if span > RECORD_CAP:
                bad(f"record {i}: {span} bytes over the cap")
            if len(line) + rec["blob"] != span:
                bad(f"record {i}: json {len(line)} + blob {rec['blob']} != span {span}")
                continue
            if not UUID4.match(rec["id"]):
                bad(f"record {i}: bad id")
            cats[rec["category"]] += 1
            total = 0
            for n, img in enumerate(rec["images"]):
                images += 1
                for name in ("card", "large", "zoom"):
                    ent = img.get(name)
                    if ent is None:
                        if name != "zoom":
                            bad(f"record {i} image {n}: missing {name}")
                        continue
                    files += 1
                    head = f.read(12)
                    if head[:4] != b"RIFF" or head[8:12] != b"WEBP":
                        bad(f"record {i} image {n} {name}: not WebP")
                    elif struct.unpack("<I", head[4:8])[0] + 8 != ent[0]:
                        bad(f"record {i} image {n} {name}: RIFF size mismatch")
                    f.seek(ent[0] - 12, 1)
                    total += ent[0]
                    if max(ent[1], ent[2]) > 1600:
                        bad(f"record {i} image {n} {name}: dimension over 1600")
            if total != rec["blob"]:
                bad(f"record {i}: file lengths {total} != blob {rec['blob']}")
        if (images, files) != (footer["images"], footer["files"]):
            bad(f"footer counts {footer['images']}/{footer['files']} != actual {images}/{files}")
        if dict(cats) != footer["categories"]:
            bad("footer categories differ from records")
        if offsets and max_record != footer["max_record_bytes"]:
            bad("footer max_record_bytes differs")
    print(f"{path}: {size} bytes, {len(offsets)} records, {images} images, {files} files, "
          f"max record {max_record}, package_id {footer['package_id']}, {len(cats)} categories")
    if problems:
        print("PROBLEMS:\n  " + "\n  ".join(problems))
        sys.exit(1)
    print("OK")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("input", nargs="?")
    ap.add_argument("output", nargs="?")
    ap.add_argument("--workers", type=int, default=0, help="default: cpu_count - 1")
    ap.add_argument("--limit", type=int, help="process at most N rows")
    ap.add_argument("--skip", type=int, default=0, help="skip the first N rows")
    ap.add_argument("--verify", metavar="FILE", help="check an existing package and exit")
    args = ap.parse_args()
    if args.verify:
        return verify(args.verify)
    if not args.input or not args.output:
        ap.error("input and output are required")
    try:
        prepare(args)
    except KeyboardInterrupt:
        sys.exit("interrupted; partial output removed")
    except BrokenProcessPool:
        sys.exit("a worker process died (out of memory?); partial output removed")
    except OSError as e:
        sys.exit(f"I/O error: {e}; partial output removed")


if __name__ == "__main__":
    main()
