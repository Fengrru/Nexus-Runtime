use rmp_serde::Serializer;
use serde::Serialize;
use serde::de::DeserializeOwned;

pub fn serialize_deterministic<T: Serialize>(value: &T) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    let mut buf = Vec::new();
    value.serialize(&mut Serializer::new(&mut buf).with_struct_map())?;
    Ok(buf)
}

pub fn deserialize_deterministic<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, rmp_serde::decode::Error> {
    rmp_serde::from_slice(bytes)
}

pub fn compute_hash(bytes: &[u8]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(bytes);
    hasher.finalize().to_hex().to_string()
}

pub fn compute_sha256_hash(bytes: &[u8]) -> String {
    use sha2::{Sha256, Digest};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

pub struct UsdCents(pub u64);

impl UsdCents {
    pub fn from_float(dollars: f64) -> Self {
        Self((dollars * 100.0).round() as u64)
    }

    pub fn to_float(&self) -> f64 {
        self.0 as f64 / 100.0
    }

    pub fn add(&self, other: UsdCents) -> Self {
        Self(self.0.saturating_add(other.0))
    }

    pub fn subtract(&self, other: UsdCents) -> Option<Self> {
        self.0.checked_sub(other.0).map(Self)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

pub fn zstd_compress(data: &[u8]) -> Vec<u8> {
    zstd::encode_all(data, 3).unwrap_or_else(|_| data.to_vec())
}

pub fn zstd_decompress(data: &[u8]) -> Result<Vec<u8>, String> {
    zstd::decode_all(data).map_err(|e| format!("zstd decompression failed: {}", e))
}

pub fn canonicalize_json(value: &serde_json::Value) -> String {
    let sorted = sort_json_keys(value);
    serde_json::to_string(&sorted).unwrap_or_default()
}

fn sort_json_keys(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut sorted: Vec<(String, serde_json::Value)> = map
                .iter()
                .map(|(k, v)| (k.clone(), sort_json_keys(v)))
                .collect();
            sorted.sort_by(|a, b| a.0.cmp(&b.0));
            let mut new_map = serde_json::Map::new();
            for (k, v) in sorted {
                new_map.insert(k, v);
            }
            serde_json::Value::Object(new_map)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(sort_json_keys).collect())
        }
        other => other.clone(),
    }
}

pub fn to_messagepack_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    serialize_deterministic(value).map_err(|e| format!("serialization error: {}", e))
}

pub fn from_messagepack_bytes<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    deserialize_deterministic(bytes).map_err(|e| format!("deserialization error: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;

    #[test]
    fn test_deterministic_serialization() {
        let cv1 = {
            let mut cv = CausalVector::new();
            cv.increment(SessionId::from_bytes([1u8; 16]));
            cv.increment(SessionId::from_bytes([1u8; 16]));
            cv
        };
        let cv2 = cv1.clone();

        let bytes1 = serialize_deterministic(&cv1).unwrap();
        let bytes2 = serialize_deterministic(&cv2).unwrap();
        assert_eq!(bytes1, bytes2, "Deterministic serialization must produce identical bytes");
    }

    #[test]
    fn test_hash_consistency() {
        let data = b"hello deterministic world";
        let h1 = compute_hash(data);
        let h2 = compute_hash(data);
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64);
    }

    #[test]
    fn test_usd_cents_no_float_drift() {
        let a = UsdCents::from_float(1.23);
        let b = UsdCents::from_float(3.45);
        let sum = a.add(b);
        assert_eq!(sum.0, 468);
    }
}
