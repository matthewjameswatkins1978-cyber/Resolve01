//! Authenticated HTTPS adapter for the canonical Tethers Guard Protocol v1.
//!
//! This crate exposes only guard admission and outcome delivery. It does not
//! make policy decisions or execute providers; those remain Tethers authority.

use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use resolve_core::{
    GuardId, MonotonicInstant, ScopeKey, ScopeSet, TethersActionRef, TethersOutcome,
    TethersPreparationDigest,
};
use resolve_store::{
    GuardAdmission, GuardAdmissionRejection, OutcomeRecording, ResolveService, StoreError,
};
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};
use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use subtle::ConstantTimeEq;
use tokio::task;

pub const PROTOCOL_VERSION: &str = "resolve.tethers-guard/1";
pub const ADMISSION_PATH: &str = "/internal/tethers/v1/guard/admit";
pub const OUTCOME_PATH: &str = "/internal/tethers/v1/outcome";
pub const MAX_BODY_BYTES: usize = 16 * 1024;

/// Supplies both Resolve's monotonic clock and persisted Unix-second time.
/// The host owns clock policy; the bridge never invents time values.
pub trait ResolveClock: Send + Sync + 'static {
    fn monotonic_now(&self) -> MonotonicInstant;
    fn unix_seconds(&self) -> i64;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BridgeConfigError {
    EmptyBridgeKey,
    InvalidBridgeKey,
}

impl fmt::Display for BridgeConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyBridgeKey => {
                formatter.write_str("Resolve Tethers bridge key must not be empty")
            }
            Self::InvalidBridgeKey => {
                formatter.write_str("Resolve Tethers bridge key is not a valid HTTP header value")
            }
        }
    }
}

impl std::error::Error for BridgeConfigError {}

#[derive(Clone)]
pub struct BridgeState {
    service: Arc<Mutex<ResolveService>>,
    key: Arc<[u8]>,
    clock: Arc<dyn ResolveClock>,
}

impl fmt::Debug for BridgeState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BridgeState")
            .field("service", &"<ResolveService>")
            .field("key", &"<redacted>")
            .field("clock", &"<ResolveClock>")
            .finish()
    }
}

impl BridgeState {
    pub fn new(
        service: ResolveService,
        key: impl AsRef<[u8]>,
        clock: Arc<dyn ResolveClock>,
    ) -> Result<Self, BridgeConfigError> {
        if key.as_ref().is_empty() {
            return Err(BridgeConfigError::EmptyBridgeKey);
        }
        let header_key = HeaderValue::from_bytes(key.as_ref())
            .map_err(|_| BridgeConfigError::InvalidBridgeKey)?;
        if header_key.to_str().is_err() {
            return Err(BridgeConfigError::InvalidBridgeKey);
        }
        Ok(Self {
            service: Arc::new(Mutex::new(service)),
            key: Arc::from(key.as_ref()),
            clock,
        })
    }
}

/// Build the protocol router. Applications can mount this router only behind
/// their HTTPS listener; this crate deliberately provides no plaintext serve.
pub fn router(state: BridgeState) -> Router {
    Router::new()
        .route(ADMISSION_PATH, post(admit))
        .route(OUTCOME_PATH, post(record_outcome))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
}

/// Serve the bridge on a TLS listener using explicit PEM certificate/key files.
pub async fn serve_https(
    address: SocketAddr,
    state: BridgeState,
    certificate_pem: impl AsRef<std::path::Path>,
    private_key_pem: impl AsRef<std::path::Path>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let tls =
        axum_server::tls_rustls::RustlsConfig::from_pem_file(certificate_pem, private_key_pem)
            .await?;
    axum_server::bind_rustls(address, tls)
        .serve(router(state).into_make_service())
        .await?;
    Ok(())
}

async fn admit(State(state): State<BridgeState>, headers: HeaderMap, body: Bytes) -> Response {
    if !authorized(&headers, &state.key) || !json_content_type(&headers) {
        return closed_failure();
    }
    let request: AdmissionRequest = match parse_request(&body) {
        Ok(request) => request,
        Err(()) => return closed_failure(),
    };
    if !valid_digest(&request.guard_ref)
        || !valid_digest(&request.preparation_digest)
        || !valid_action_id(&request.action_id)
        || request.scope_keys.len() > 64
        || request.scope_keys.iter().any(|key| !valid_digest(key))
        || request.scope_keys.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return closed_failure();
    }
    let digest = digest(&request.canonical_value);
    if request.scope_keys.is_empty() {
        return Json(AdmissionResponse {
            protocol_version: PROTOCOL_VERSION,
            request_digest: &digest,
            decision: "REJECTED",
            reason_code: "scope_keys_mismatch",
        })
        .into_response();
    }
    let guard_id = match GuardId::try_new(request.guard_ref.clone()) {
        Ok(value) => value,
        Err(_) => return closed_failure(),
    };
    let action_ref = match TethersActionRef::try_new(request.action_id.clone()) {
        Ok(value) => value,
        Err(_) => return closed_failure(),
    };
    let preparation_digest = match TethersPreparationDigest::try_new(request.preparation_digest) {
        Ok(value) => value,
        Err(_) => return closed_failure(),
    };
    let scope_keys = match request
        .scope_keys
        .into_iter()
        .map(ScopeKey::try_new)
        .collect::<Result<Vec<_>, _>>()
        .and_then(ScopeSet::try_new)
    {
        Ok(value) => value,
        Err(_) => return closed_failure(),
    };
    let result = run_store(state.clone(), move |service| {
        service.admit_guard_with_preparation(
            &guard_id,
            scope_keys,
            action_ref,
            preparation_digest,
            state.clock.monotonic_now(),
        )
    })
    .await;
    let (decision, reason) = match result {
        Ok(GuardAdmission::Admitted | GuardAdmission::AlreadyAdmitted) => ("ADMITTED", "admitted"),
        Err(StoreError::NotFound {
            entity: "guard", ..
        }) => ("REJECTED", "guard_not_found"),
        Err(StoreError::GuardAdmissionRejected(reason)) => match reason {
            GuardAdmissionRejection::GuardRevoked => ("REJECTED", "guard_revoked"),
            GuardAdmissionRejection::GuardAlreadyBound => ("REJECTED", "guard_already_bound"),
            GuardAdmissionRejection::GuardExpired => ("REJECTED", "guard_expired"),
            GuardAdmissionRejection::TaskNotActive => ("REJECTED", "task_not_active"),
            GuardAdmissionRejection::OwnershipChanged => ("REJECTED", "ownership_changed"),
            GuardAdmissionRejection::FenceChanged => ("REJECTED", "fence_changed"),
            GuardAdmissionRejection::ActionMismatch => ("REJECTED", "action_mismatch"),
            GuardAdmissionRejection::PreparationMismatch => ("REJECTED", "preparation_mismatch"),
            GuardAdmissionRejection::ScopeKeysMismatch => ("REJECTED", "scope_keys_mismatch"),
            GuardAdmissionRejection::InternalIntegrity => ("INDETERMINATE", "internal_integrity"),
        },
        Err(StoreError::ScopeLocked { .. }) => ("REJECTED", "scope_keys_mismatch"),
        Err(StoreError::OutstandingAction { .. }) => ("REJECTED", "action_mismatch"),
        Err(StoreError::GuardInvalid { .. }) => ("INDETERMINATE", "internal_integrity"),
        Err(StoreError::InvalidPersistedData(_) | StoreError::InvalidMutation(_)) => {
            ("INDETERMINATE", "internal_integrity")
        }
        Err(_) => ("INDETERMINATE", "state_unavailable"),
    };
    let response = AdmissionResponse {
        protocol_version: PROTOCOL_VERSION,
        request_digest: &digest,
        decision,
        reason_code: reason,
    };
    Json(response).into_response()
}

async fn record_outcome(
    State(state): State<BridgeState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !authorized(&headers, &state.key) || !json_content_type(&headers) {
        return closed_failure();
    }
    let request: OutcomeRequest = match parse_request(&body) {
        Ok(request) => request,
        Err(()) => return closed_failure(),
    };
    if !valid_action_id(&request.action_id) || !valid_digest(&request.preparation_digest) {
        return closed_failure();
    }
    let delivery_digest = digest(&request.canonical_value);
    let action_ref = match TethersActionRef::try_new(request.action_id) {
        Ok(value) => value,
        Err(_) => return closed_failure(),
    };
    let preparation_digest = match TethersPreparationDigest::try_new(request.preparation_digest) {
        Ok(value) => value,
        Err(_) => return closed_failure(),
    };
    let outcome = match request.outcome.as_str() {
        "SUCCEEDED" => TethersOutcome::Succeeded { action_ref },
        "FAILED" => TethersOutcome::Failed { action_ref },
        "UNCERTAIN" => TethersOutcome::Uncertain { action_ref },
        _ => return closed_failure(),
    };
    let result = run_store(state.clone(), move |service| {
        service.record_outcome_with_preparation(
            outcome,
            preparation_digest,
            state.clock.unix_seconds(),
        )
    })
    .await;
    let result = match result {
        Ok(OutcomeRecording::Recorded) => "RECORDED",
        Ok(OutcomeRecording::AlreadyRecorded) => "ALREADY_RECORDED",
        Err(StoreError::OutcomeConflict { .. }) => "CONFLICT",
        Err(_) => return closed_failure(),
    };
    Json(OutcomeResponse {
        protocol_version: PROTOCOL_VERSION,
        delivery_digest: &delivery_digest,
        result,
    })
    .into_response()
}

async fn run_store<T: Send + 'static>(
    state: BridgeState,
    operation: impl FnOnce(&mut ResolveService) -> Result<T, StoreError> + Send + 'static,
) -> Result<T, StoreError> {
    task::spawn_blocking(move || {
        let mut service = state.service.lock().map_err(|_| {
            StoreError::InvalidPersistedData("Resolve service lock poisoned".to_owned())
        })?;
        operation(&mut service)
    })
    .await
    .unwrap_or_else(|_| {
        Err(StoreError::InvalidPersistedData(
            "Resolve service worker failed".to_owned(),
        ))
    })
}

fn authorized(headers: &HeaderMap, expected: &[u8]) -> bool {
    let mut values = headers.get_all("X-Resolve-Tethers-Key").iter();
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }
    let Ok(actual) = value.to_str() else {
        return false;
    };
    actual.len() == expected.len() && bool::from(actual.as_bytes().ct_eq(expected))
}

fn json_content_type(headers: &HeaderMap) -> bool {
    let mut values = headers.get_all(header::CONTENT_TYPE).iter();
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }
    value
        .to_str()
        .ok()
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
}

fn closed_failure() -> Response {
    (StatusCode::BAD_REQUEST, Body::empty()).into_response()
}

fn digest(value: &Value) -> String {
    let canonical =
        serde_json_canonicalizer::to_vec(value).expect("JSON values serialize to canonical JSON");
    format!("sha256:{:x}", Sha256::digest(canonical))
}

fn valid_digest(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_action_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'/' | b'-')
        })
}

fn parse_request<T: HasCanonicalValue>(body: &[u8]) -> Result<T, ()> {
    if body.len() > MAX_BODY_BYTES {
        return Err(());
    }
    let text = std::str::from_utf8(body).map_err(|_| ())?;
    let value = UniqueValueSeed
        .deserialize(&mut serde_json::Deserializer::from_str(text))
        .map_err(|_| ())?;
    let mut object = value.as_object().cloned().ok_or(())?;
    let protocol = object
        .remove("protocol_version")
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or(())?;
    if protocol != PROTOCOL_VERSION {
        return Err(());
    }
    let mut with_protocol = Map::new();
    with_protocol.insert("protocol_version".to_owned(), Value::String(protocol));
    with_protocol.extend(object);
    let canonical_value = Value::Object(with_protocol);
    let mut request = serde_json::from_value::<T>(canonical_value.clone()).map_err(|_| ())?;
    request.set_canonical_value(canonical_value);
    Ok(request)
}

trait HasCanonicalValue: for<'de> Deserialize<'de> {
    fn set_canonical_value(&mut self, value: Value);
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AdmissionRequest {
    #[serde(rename = "protocol_version")]
    _protocol_version: String,
    guard_ref: String,
    action_id: String,
    preparation_digest: String,
    scope_keys: Vec<String>,
    #[serde(skip)]
    canonical_value: Value,
}

impl HasCanonicalValue for AdmissionRequest {
    fn set_canonical_value(&mut self, value: Value) {
        self.canonical_value = value;
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OutcomeRequest {
    #[serde(rename = "protocol_version")]
    _protocol_version: String,
    action_id: String,
    preparation_digest: String,
    outcome: String,
    #[serde(skip)]
    canonical_value: Value,
}

impl HasCanonicalValue for OutcomeRequest {
    fn set_canonical_value(&mut self, value: Value) {
        self.canonical_value = value;
    }
}

#[derive(Serialize)]
struct AdmissionResponse<'a> {
    protocol_version: &'static str,
    request_digest: &'a str,
    decision: &'a str,
    reason_code: &'a str,
}

#[derive(Serialize)]
struct OutcomeResponse<'a> {
    protocol_version: &'static str,
    delivery_digest: &'a str,
    result: &'a str,
}

struct UniqueValueSeed;

impl<'de> DeserializeSeed<'de> for UniqueValueSeed {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(UniqueValueVisitor)
    }
}

struct UniqueValueVisitor;

impl<'de> Visitor<'de> for UniqueValueVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JSON without duplicate object keys")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Value, E> {
        Ok(Value::Number(Number::from(value)))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Value, E> {
        Ok(Value::Number(Number::from(value)))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Value, E>
    where
        E: de::Error,
    {
        Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("invalid number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Value, E> {
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Value, E> {
        Ok(Value::String(value))
    }

    fn visit_none<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(UniqueValueSeed)? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = Map::new();
        while let Some((key, value)) = map.next_entry_seed(StringSeed, UniqueValueSeed)? {
            if values.insert(key, value).is_some() {
                return Err(de::Error::custom("duplicate object key"));
            }
        }
        Ok(Value::Object(values))
    }
}

struct StringSeed;

impl<'de> DeserializeSeed<'de> for StringSeed {
    type Value = String;

    fn deserialize<D>(self, deserializer: D) -> Result<String, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use resolve_core::MonotonicInstant;
    use resolve_store::SqliteStore;
    use tower::ServiceExt;

    struct TestClock;

    impl ResolveClock for TestClock {
        fn monotonic_now(&self) -> MonotonicInstant {
            MonotonicInstant::from_ticks(10)
        }

        fn unix_seconds(&self) -> i64 {
            10
        }
    }

    fn state() -> BridgeState {
        let store = SqliteStore::open_in_memory_for_tests().expect("test store opens");
        BridgeState::new(
            ResolveService::new(store),
            b"test-bridge-key",
            Arc::new(TestClock),
        )
        .expect("bridge key is configured")
    }

    #[test]
    fn bridge_rejects_empty_keys_and_redacts_configured_key_from_debug() {
        let store = SqliteStore::open_in_memory_for_tests().expect("test store opens");
        assert_eq!(
            BridgeState::new(ResolveService::new(store), b"", Arc::new(TestClock))
                .expect_err("empty key must disable bridge construction"),
            BridgeConfigError::EmptyBridgeKey
        );
        let store = SqliteStore::open_in_memory_for_tests().expect("test store opens");
        assert_eq!(
            BridgeState::new(ResolveService::new(store), b"bad\nkey", Arc::new(TestClock))
                .expect_err("invalid HTTP key must disable bridge construction"),
            BridgeConfigError::InvalidBridgeKey
        );
        assert!(!format!("{:?}", state()).contains("test-bridge-key"));
    }

    #[test]
    fn accepted_tethers_request_digests_match_the_shared_fixtures() {
        let admission = serde_json::json!({
            "protocol_version": PROTOCOL_VERSION,
            "guard_ref": format!("sha256:{}", "0".repeat(64)),
            "action_id": "action-fixture-1",
            "preparation_digest": format!("sha256:{}", "1".repeat(64)),
            "scope_keys": [
                format!("sha256:{}", "2".repeat(64)),
                format!("sha256:{}", "3".repeat(64)),
            ],
        });
        assert_eq!(
            digest(&admission),
            "sha256:a643f675c945f85b213398f85a97400bec3332178705c67fbebcfeb898c08ce2"
        );
        let outcome = serde_json::json!({
            "protocol_version": PROTOCOL_VERSION,
            "action_id": "action-fixture-1",
            "preparation_digest": format!("sha256:{}", "1".repeat(64)),
            "outcome": "SUCCEEDED",
        });
        assert_eq!(
            digest(&outcome),
            "sha256:d39c779993ff590d913cadd1db4dcaf7233d652e97866cb518bb4d8c5ed8ff4f"
        );
    }

    #[test]
    fn request_parser_rejects_duplicate_unknown_and_wrong_protocol_fields() {
        let duplicate = br#"{"protocol_version":"resolve.tethers-guard/1","protocol_version":"resolve.tethers-guard/1"}"#;
        assert!(parse_request::<OutcomeRequest>(duplicate).is_err());

        let unknown = br#"{"protocol_version":"resolve.tethers-guard/1","action_id":"a","preparation_digest":"sha256:1111111111111111111111111111111111111111111111111111111111111111","outcome":"SUCCEEDED","extra":true}"#;
        assert!(parse_request::<OutcomeRequest>(unknown).is_err());

        let wrong_version = br#"{"protocol_version":"resolve.tethers-guard/2","action_id":"a","preparation_digest":"sha256:1111111111111111111111111111111111111111111111111111111111111111","outcome":"SUCCEEDED"}"#;
        assert!(parse_request::<OutcomeRequest>(wrong_version).is_err());
    }

    #[tokio::test]
    async fn authenticated_admission_uses_exact_path_and_closed_decision_shape() {
        let app = router(state());
        let body = serde_json::json!({
            "protocol_version": PROTOCOL_VERSION,
            "guard_ref": format!("sha256:{}", "0".repeat(64)),
            "action_id": "action-fixture-1",
            "preparation_digest": format!("sha256:{}", "1".repeat(64)),
            "scope_keys": [format!("sha256:{}", "2".repeat(64))],
        })
        .to_string();
        let response = app
            .clone()
            .oneshot(
                Request::post(ADMISSION_PATH)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("X-Resolve-Tethers-Key", "test-bridge-key")
                    .body(Body::from(body.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["decision"], "REJECTED");
        assert_eq!(json["reason_code"], "guard_not_found");
        assert_eq!(
            json["request_digest"],
            digest(&serde_json::from_str::<Value>(&body).unwrap())
        );

        let unauthenticated = app
            .oneshot(
                Request::post(ADMISSION_PATH)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(unauthenticated.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn body_limit_and_unregistered_paths_fail_closed() {
        let app = router(state());
        let too_large = app
            .clone()
            .oneshot(
                Request::post(ADMISSION_PATH)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("X-Resolve-Tethers-Key", "test-bridge-key")
                    .body(Body::from(vec![b' '; MAX_BODY_BYTES + 1]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(too_large.status(), StatusCode::OK);

        let other_path = app
            .oneshot(
                Request::post("/internal/tethers/v1/other")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("X-Resolve-Tethers-Key", "test-bridge-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(other_path.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn empty_scope_set_is_a_closed_admission_rejection() {
        let response = router(state())
            .oneshot(
                Request::post(ADMISSION_PATH)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("X-Resolve-Tethers-Key", "test-bridge-key")
                    .body(Body::from(
                        serde_json::json!({
                            "protocol_version": PROTOCOL_VERSION,
                            "guard_ref": format!("sha256:{}", "0".repeat(64)),
                            "action_id": "action-fixture-1",
                            "preparation_digest": format!("sha256:{}", "1".repeat(64)),
                            "scope_keys": [],
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["decision"], "REJECTED");
        assert_eq!(value["reason_code"], "scope_keys_mismatch");
    }
}
