# Catalogue release dataset

Release tag: `v2026.11.10-catalogue`

The release payload is the completed product catalogue recovered from the October 2026 crawl:

- Compressed source: `catalogue.jsonl.zst`
- Compressed size: **28,199,363,733 bytes**
- Uncompressed dataset: `catalogue.jsonl`
- Uncompressed size: **41,489,800,125 bytes**
- Uncompressed SHA-256: `ee3b58d9e0b3b73f8352da82691fb7252ca74505f53afd2cc8fe10b74ea96b77`
- Products: **30,323**
- Embedded gallery images: **135,690**
- Release assets: `catalogue.jsonl.zst.part-000` through `catalogue.jsonl.zst.part-026`

The first 26 parts are 1 GiB (1,073,741,824 bytes) each. The final part is the 282,076,309-byte remainder. Reassemble in lexical order:

```sh
cat catalogue.jsonl.zst.part-* > catalogue.jsonl.zst
```

Then decompress and verify the uncompressed stream:

```sh
zstd -dc catalogue.jsonl.zst | sha256sum
```

The expected digest is the SHA-256 shown above.
