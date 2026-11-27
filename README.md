# xmip-core-archive-aws-s3

S3 archive target: one item is one object in a bucket, its TOML metadata a second object under `.meta`, each created only where none is and never written over. A technology of [xmip-core-archive](https://github.com/IlleNilsson/xmip-core-archive).

An item is its bytes at `<prefix>/<data_type>/<identifier>` and its metadata, the TOML sidecar, at `<prefix>/.meta/<data_type>/<identifier>`. What is archived is never written over (ADR-0040, amendment 2026-11-19): the archive capability's one rule, `archive::named`, creates the metadata first, then the bytes, and gives the receipt only then; this crate implements its `Named` over one bucket, each object created with `PUT` and `If-None-Match: *` (`aws_s3::Client::put_new`), which S3 answers `412` where an object is there already, and read with `GET`, which S3 answers `404` where none is. An identifier archived again with the same bytes answers the same receipt; with other bytes it is refused and the earlier receipt still restores; an archive stopped between the two objects is completed by its repeat. A restore reads the metadata first and refuses a location that has none.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
