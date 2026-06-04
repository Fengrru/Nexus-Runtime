# Serialization & Determinism

## Format

- **Codec:** MessagePack via `rmp-serde`
- **Mode:** StructMap (structs serialized as maps, not arrays)
- **Integer Encoding:** BigEndian
- **Float:** Prohibited (use `u64` fixed-point for currency)

## Allowed Types

| Type | Usage |
|------|-------|
| `BTreeMap<K, V>` | Maps with deterministic iteration |
| `BTreeSet<T>` | Unique collections with deterministic iteration |
| `Vec<T>` | Ordered sequences |
| `u64` | Counters, timestamps, currency (cents) |
| `String` | UTF-8 text |

## Forbidden Types

| Type | Reason | Replacement |
|------|--------|-------------|
| `HashMap` | Non-deterministic iteration | `BTreeMap` |
| `HashSet` | Non-deterministic iteration | `BTreeSet` |
| `f32`/`f64` | NaN/inf platform variance | `u64` fixed-point |
| `SystemTime` | Non-deterministic across machines | `u64` epoch millis |
| `Instant` | Non-serializable | `u64` monotonic counter |

## Enforcement

- `#![deny(clippy::disallowed_types)]` in every crate
- `.clippy.toml` disallows `HashMap`/`HashSet`
- Golden fixtures in `fixtures/` verify byte-identical serialization
