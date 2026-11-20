#![forbid(unsafe_code)]

//! S3 archive: an [`ArchiveStore`] that keeps each retained item as one
//! object in a bucket, its metadata as a second object, and restores the
//! item by getting both back.
//!
//! What an archive laid out by name does — the layout, the metadata
//! sidecar, the checksum, the order of the two objects and what a repeat
//! finds — is the archive capability's one rule (`archive::named`,
//! ADR-0044); only the dialect is this crate's: [`Named`] over one bucket.
//!
//! A xmip-core-archive **technology** (repository-model.md): it depends on
//! the archive capability for the [`ArchiveStore`] trait, its item, receipt
//! and error types and the named rule, and on the aws-s3 transport
//! technology for the signed requests — Signature Version 4, path-style,
//! one connection a call. The bytes are at
//! `<prefix>/<data_type>/<identifier>` and the metadata, the TOML sidecar,
//! at `<prefix>/.meta/<data_type>/<identifier>`, as `archive::layout` says.
//!
//! An archive never deletes and never writes over (ADR-0040): this one
//! creates and gets, nothing else. Each object is created with `PUT` and
//! `If-None-Match: *`, which S3 answers 412 where an object is there
//! already and writes nothing (`aws_s3::Client::put_new`). The receipt is
//! `aws-s3://<bucket>/<key>`, the transport's origin for the bytes,
//! written by its `origin`, with the SHA-256 of the bytes as its checksum.

use std::time::Duration;

use archive::named::{self, Created, Named};
use archive::{ArchiveError, ArchiveItem, ArchiveReceipt, ArchiveStore, location};
use aws_s3::Client;

/// An archive that keeps items as objects under one prefix of one bucket.
pub struct S3Archive {
    endpoint: String,
    region: String,
    bucket: String,
    access_key: String,
    secret_key: String,
    prefix: String,
    timeout: Option<Duration>,
}

impl S3Archive {
    /// An archive writing to `bucket` at `endpoint` — `http://host:port` or
    /// `https://host:port` — in `region`, under no prefix. Any real endpoint
    /// wants [`Self::with_credentials`].
    #[must_use]
    pub fn new(
        endpoint: impl Into<String>,
        region: impl Into<String>,
        bucket: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            region: region.into(),
            bucket: bucket.into(),
            access_key: String::new(),
            secret_key: String::new(),
            prefix: String::new(),
            timeout: None,
        }
    }

    /// The access key and secret key every request is signed with.
    #[must_use]
    pub fn with_credentials(
        mut self,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        self.access_key = access_key.into();
        self.secret_key = secret_key.into();
        self
    }

    /// The prefix every key is laid out under, `retained` say.
    #[must_use]
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Give up on an endpoint that stops answering.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// The bucket, through one client for one archive or restore.
    fn bucket(&self) -> Result<Bucket<'_>, ArchiveError> {
        let client = Client::new(
            &self.endpoint,
            &self.region,
            &self.access_key,
            &self.secret_key,
        )
        .map_err(ArchiveError::caused_by)?;
        let client = match self.timeout {
            Some(timeout) => client.timing_out_after(timeout),
            None => client,
        };
        Ok(Bucket {
            client,
            bucket: &self.bucket,
        })
    }
}

impl ArchiveStore for S3Archive {
    fn archive(&self, item: ArchiveItem) -> Result<ArchiveReceipt, ArchiveError> {
        named::archive(&self.bucket()?, &self.prefix, item)
    }

    fn restore(&self, receipt: &ArchiveReceipt) -> Result<ArchiveItem, ArchiveError> {
        named::restore(&self.bucket()?, &self.prefix, receipt)
    }
}

/// One bucket, spoken to through one client: what the named rule creates
/// objects in and reads them from.
struct Bucket<'a> {
    client: Client,
    bucket: &'a str,
}

impl Named for Bucket<'_> {
    fn create(&self, key: &str, bytes: &[u8]) -> Result<Created, ArchiveError> {
        let stored = self
            .client
            .put_new(self.bucket, key, bytes)
            .map_err(ArchiveError::caused_by)?;
        Ok(if stored.is_some() {
            Created::Created
        } else {
            Created::Exists
        })
    }

    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, ArchiveError> {
        self.client
            .get(self.bucket, key)
            .map_err(ArchiveError::caused_by)
    }

    fn location(&self, key: &str) -> String {
        aws_s3::origin(self.bucket, key)
    }

    fn key(&self, location: &str) -> Result<String, ArchiveError> {
        match location::bucket_key("aws-s3", location)? {
            (bucket, key) if bucket == self.bucket => Ok(key.to_string()),
            (bucket, _) => Err(ArchiveError {
                message: format!(
                    "{location} is in the bucket {bucket}, not in this archive's, {}",
                    self.bucket
                ),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use archive::checksum;
    use archive::fixture::{item, secs};
    use archive::sidecar::Sidecar;
    use aws_s3::{Event, Session};
    use std::assert_matches;
    use std::collections::BTreeMap;
    use std::net::TcpListener;
    use std::thread::JoinHandle;

    type FarEnd = JoinHandle<(Session, Vec<Event>)>;

    /// A far end holding `objects` that answers `requests` signed requests,
    /// one connection each, and then hands back what it holds and what it
    /// saw.
    fn far_end_holding(requests: usize, objects: BTreeMap<String, Vec<u8>>) -> (String, FarEnd) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("address").to_string();
        let handle = std::thread::spawn(move || {
            let mut session = Session::new("eu-north-1", "AKID", "secret")
                .timing_out_after(secs(2))
                .with_objects(objects);
            let events = (0..requests)
                .map(|_| session.serve_one(&listener).expect("served"))
                .collect();
            (session, events)
        });
        (address, handle)
    }

    fn far_end(requests: usize) -> (String, FarEnd) {
        far_end_holding(requests, BTreeMap::new())
    }

    fn store(address: &str) -> S3Archive {
        S3Archive::new(format!("http://{address}"), "eu-north-1", "orders-archive")
            .with_credentials("AKID", "secret")
            .with_prefix("retained")
            .timing_out_after(secs(2))
    }

    /// The fixture item with other bytes.
    fn other_bytes(id: &str) -> ArchiveItem {
        ArchiveItem {
            bytes: b"{\"kept\":false}".to_vec(),
            ..item(id)
        }
    }

    const BYTES: &str = "orders-archive/retained/json/json#1";
    const META: &str = "orders-archive/retained/.meta/json/json#1";

    #[test]
    fn an_archived_item_is_its_metadata_then_its_bytes_and_the_receipt_names_the_bytes() {
        let (address, far_end) = far_end(2);
        let original = item("json#1");
        let receipt = store(&address).archive(original.clone()).expect("archive");
        assert_eq!(
            receipt.location,
            "aws-s3://orders-archive/retained/json/json#1"
        );
        assert_eq!(
            receipt.checksum.as_deref(),
            Some(checksum::sha256_hex(&original.bytes).as_str())
        );
        let (session, events) = far_end.join().expect("thread");
        let held = session.objects();
        assert_eq!(held.len(), 2);
        assert_eq!(held.get(BYTES), Some(&original.bytes));
        let meta = String::from_utf8(held.get(META).expect("metadata").clone()).expect("utf-8");
        let sidecar = Sidecar::parse(&meta).expect("the TOML sidecar");
        assert_eq!(
            (sidecar.data_type.as_str(), sidecar.identifier.as_str()),
            ("json", "json#1")
        );
        assert_eq!(sidecar.checksum, checksum::sha256_hex(&original.bytes));
        assert_eq!(sidecar.metadata, original.metadata);
        assert_matches!(
            &events[0],
            Event::Stored(arrived)
                if arrived.origin_uri == "aws-s3://orders-archive/retained/.meta/json/json#1"
        );
        assert_matches!(
            &events[1],
            Event::Stored(arrived)
                if arrived.origin_uri == "aws-s3://orders-archive/retained/json/json#1"
        );
    }

    #[test]
    fn the_objects_restore_the_item_through_the_same_session() {
        let (address, far_end) = far_end(4);
        let store = store(&address);
        let original = item("json#2");
        let receipt = store.archive(original.clone()).expect("archive");
        let restored = store.restore(&receipt).expect("restore");
        assert_eq!(restored, original, "both objects read back give the item");
        let (_, events) = far_end.join().expect("thread");
        assert_eq!(
            events[2..4],
            [
                Event::Retrieved("aws-s3://orders-archive/retained/.meta/json/json#2".to_string()),
                Event::Retrieved("aws-s3://orders-archive/retained/json/json#2".to_string())
            ],
            "the metadata first"
        );
    }

    #[test]
    fn an_identifier_archived_again_with_the_same_bytes_answers_the_same_receipt() {
        // Two creates; two refused creates, each with its get.
        let (address, far_end) = far_end(6);
        let store = store(&address);
        let first = store.archive(item("json#1")).expect("archive");
        let again = store.archive(item("json#1")).expect("archived again");
        assert_eq!(again, first);
        let (session, events) = far_end.join().expect("thread");
        assert_eq!(session.objects().len(), 2, "nothing more is written");
        assert_eq!(events[2], Event::Refused("PreconditionFailed".to_string()));
        assert_eq!(events[4], Event::Refused("PreconditionFailed".to_string()));
    }

    #[test]
    fn an_identifier_archived_again_with_other_bytes_is_refused_and_the_earlier_receipt_restores() {
        // Two creates; a refused create and its get; a restore's two gets.
        let (address, far_end) = far_end(6);
        let store = store(&address);
        let receipt = store.archive(item("json#1")).expect("archive");
        let refused = store
            .archive(other_bytes("json#1"))
            .expect_err("other bytes");
        assert!(
            refused
                .message
                .contains("is archived already with other bytes"),
            "{refused}"
        );
        assert_eq!(store.restore(&receipt).expect("restore"), item("json#1"));
        let (session, _) = far_end.join().expect("thread");
        assert_eq!(session.objects().get(BYTES), Some(&item("json#1").bytes));
    }

    #[test]
    fn order_and_order_meta_are_two_items() {
        let (address, far_end) = far_end(8);
        let store = store(&address);
        let order = item("order");
        let order_meta = ArchiveItem {
            bytes: b"not order's metadata".to_vec(),
            ..item("order.meta")
        };
        let first = store.archive(order.clone()).expect("order");
        let second = store.archive(order_meta.clone()).expect("order.meta");
        assert_eq!(store.restore(&first).expect("order"), order);
        assert_eq!(store.restore(&second).expect("order.meta"), order_meta);
        let (session, _) = far_end.join().expect("thread");
        assert_eq!(session.objects().len(), 4);
    }

    /// What a far end holds after `item` was archived into it and the
    /// archive stopped between the metadata and the bytes.
    fn interrupted(item: ArchiveItem) -> BTreeMap<String, Vec<u8>> {
        let (address, far_end) = far_end(2);
        store(&address).archive(item).expect("archive");
        let (session, _) = far_end.join().expect("thread");
        let mut held = session.objects().clone();
        held.remove(BYTES).expect("the bytes");
        held
    }

    #[test]
    fn an_interrupted_pair_is_completed_by_its_repeat_and_refuses_another_item() {
        let claim = interrupted(item("json#1"));
        assert_eq!(claim.keys().collect::<Vec<_>>(), [META], "the claim alone");
        // Other bytes: a refused create and its get. The repeat: a refused
        // create, its get, the bytes created. Other bytes again; a restore.
        let (address, far_end) = far_end_holding(9, claim);
        let store = store(&address);
        let refused = store.archive(other_bytes("json#1")).expect_err("claimed");
        assert!(
            refused
                .message
                .contains("is archived already with other bytes"),
            "{refused}"
        );
        let receipt = store.archive(item("json#1")).expect("completed");
        assert_eq!(store.restore(&receipt).expect("restore"), item("json#1"));
        let refused = store.archive(other_bytes("json#1")).expect_err("taken");
        assert!(
            refused
                .message
                .contains("is archived already with other bytes"),
            "{refused}"
        );
        let (session, _) = far_end.join().expect("thread");
        assert_eq!(session.objects().get(BYTES), Some(&item("json#1").bytes));
    }

    #[test]
    fn bytes_without_metadata_are_refused_on_restore() {
        let bytes = item("json#1").bytes;
        let held = BTreeMap::from([(BYTES.to_string(), bytes.clone())]);
        let (address, far_end) = far_end_holding(1, held);
        let receipt = ArchiveReceipt {
            location: "aws-s3://orders-archive/retained/json/json#1".to_string(),
            checksum: Some(checksum::sha256_hex(&bytes)),
        };
        let refused = store(&address).restore(&receipt).expect_err("no metadata");
        assert!(
            refused
                .message
                .contains("is not an item this archive completed"),
            "{refused}"
        );
        let (_, events) = far_end.join().expect("thread");
        assert_eq!(events, [Event::Refused("NoSuchKey".to_string())]);
    }

    #[test]
    fn a_foreign_object_at_the_key_is_refused() {
        let held = BTreeMap::from([(BYTES.to_string(), b"someone else's".to_vec())]);
        // The metadata created; the bytes' create refused, and their get.
        let (address, far_end) = far_end_holding(3, held);
        let refused = store(&address)
            .archive(item("json#1"))
            .expect_err("foreign bytes");
        assert!(
            refused
                .message
                .contains("holds bytes this archive did not write there"),
            "{refused}"
        );
        let (session, _) = far_end.join().expect("thread");
        assert_eq!(
            session.objects().get(BYTES),
            Some(&b"someone else's".to_vec()),
            "left as it was"
        );
    }

    #[test]
    fn a_data_type_beginning_with_a_dot_is_refused() {
        // Nothing listens: the refusal is made before any request.
        let hidden = ArchiveItem {
            data_type: ".meta".to_string(),
            ..item("json#1")
        };
        let refused = store("127.0.0.1:1")
            .archive(hidden)
            .expect_err("a dot first");
        assert!(refused.message.contains("begins with a dot"), "{refused}");
    }

    #[test]
    fn a_missing_item_a_changed_checksum_and_a_foreign_receipt_are_refused() {
        // Two creates; a restore's two gets; a get finding no metadata.
        let (address, far_end) = far_end(5);
        let store = store(&address);
        let mut receipt = store.archive(item("json#3")).expect("archive");
        receipt.checksum = Some("0".repeat(64));
        let changed = store.restore(&receipt).expect_err("checksum");
        assert!(changed.message.contains("checksum mismatch"), "{changed}");
        receipt.location = "aws-s3://orders-archive/retained/json/none".to_string();
        receipt.checksum = None;
        let missing = store.restore(&receipt).expect_err("missing");
        assert!(
            missing
                .message
                .contains("is not an item this archive completed"),
            "{missing}"
        );
        far_end.join().expect("thread");
        for location in [
            "google-cloud-storage://bucket/key",
            "aws-s3://bucket",
            "aws-s3://orders-archive/elsewhere/n",
            "aws-s3://another-bucket/retained/json/json#3",
        ] {
            receipt.location = location.to_string();
            assert!(store.restore(&receipt).is_err(), "{location}");
        }
    }
}
