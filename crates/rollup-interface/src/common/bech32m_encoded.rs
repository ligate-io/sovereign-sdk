//! Bech32m-encoded byte wrappers for chain-meaningful identifier types
//! (tx hash, block hash, state root). Borsh layout is identical to the
//! underlying byte array; only the textual surface (Display, JSON
//! Serialize, FromStr) differs from a raw byte array.
//!
//! Defined as three concrete newtypes ([`LtxHash`], [`LblkHash`],
//! [`LsrHash`]) rather than a single generic over an HRP marker
//! because the `UniversalWallet` derive macro reads
//! `#[sov_wallet(display(bech32m(prefix = "func()")))]` as a string
//! literal at expansion time, which a type-level HRP marker can't
//! provide. Concrete-per-HRP plays nice with the existing derive and
//! avoids a manual `UniversalWallet` impl per type.

use core::fmt::{Debug, Display};
use core::str::FromStr;

use borsh::{BorshDeserialize, BorshSerialize};
use sov_universal_wallet::UniversalWallet;

// ---- HRP prefix functions for the wallet derive --------------------------
//
// The `UniversalWallet` derive's `display(bech32m(prefix = "..."))`
// attribute treats the prefix as a path to a `&'static str`-returning
// function. We expose one per HRP.

/// HRP for transaction hashes (`ltx1...`).
pub fn ltx_hrp() -> &'static str {
    "ltx"
}

/// HRP for block hashes (`lblk1...`).
pub fn lblk_hrp() -> &'static str {
    "lblk"
}

/// HRP for state roots (`lsr1...`).
pub fn lsr_hrp() -> &'static str {
    "lsr"
}

// ---- Concrete types -------------------------------------------------------
//
// Each type is `pub struct $Name(pub [u8; N])` with the full set of
// derives including `UniversalWallet`. The `#[sov_wallet]` attribute
// drives the wallet-schema's bech32m display generation; the manual
// `Display`, `FromStr`, etc. impls below cover the wire-level surface
// (REST API JSON, logs, debug formatting).

macro_rules! bech32m_hash_type {
    (
        $(#[$outer:meta])*
        $name:ident, $bytes:literal, $hrp_fn:ident
    ) => {
        $(#[$outer])*
        #[derive(
            Copy,
            Clone,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            UniversalWallet,
        )]
        pub struct $name(
            #[sov_wallet(display(bech32m(prefix = stringify!($hrp_fn))))]
            pub [u8; $bytes],
        );

        impl $name {
            /// Wrap raw bytes.
            pub const fn new(bytes: [u8; $bytes]) -> Self {
                Self(bytes)
            }
        }

        impl AsRef<[u8]> for $name {
            fn as_ref(&self) -> &[u8] {
                &self.0
            }
        }

        impl From<[u8; $bytes]> for $name {
            fn from(bytes: [u8; $bytes]) -> Self {
                Self(bytes)
            }
        }

        impl From<$name> for [u8; $bytes] {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                let hrp = bech32::Hrp::parse($hrp_fn()).map_err(|_| core::fmt::Error)?;
                let encoded = bech32::encode::<bech32::Bech32m>(hrp, &self.0)
                    .map_err(|_| core::fmt::Error)?;
                f.write_str(&encoded)
            }
        }

        impl Debug for $name {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                Display::fmt(self, f)
            }
        }

        impl FromStr for $name {
            type Err = anyhow::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                // Backward-compat: also accept `0x...` hex form so old
                // clients / scripts / curl-pasted strings keep working.
                let bytes: Vec<u8> = if let Some(stripped) =
                    s.strip_prefix("0x").or_else(|| s.strip_prefix("0X"))
                {
                    hex::decode(stripped)?
                } else {
                    let (hrp, data) = bech32::decode(s)?;
                    let expected = $hrp_fn();
                    if hrp.as_str() != expected {
                        anyhow::bail!(
                            "expected HRP '{}', got '{}'",
                            expected,
                            hrp.as_str()
                        );
                    }
                    data
                };
                let arr: [u8; $bytes] = bytes.try_into().map_err(|_| {
                    anyhow::anyhow!("decoded bytes have wrong length for {}", stringify!($name))
                })?;
                Ok(Self(arr))
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                if serializer.is_human_readable() {
                    serializer.serialize_str(&self.to_string())
                } else {
                    use serde::ser::SerializeSeq;
                    let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
                    for b in &self.0 {
                        seq.serialize_element(b)?;
                    }
                    seq.end()
                }
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(
                deserializer: D,
            ) -> Result<Self, D::Error> {
                if deserializer.is_human_readable() {
                    let s: String = serde::Deserialize::deserialize(deserializer)?;
                    Self::from_str(&s).map_err(serde::de::Error::custom)
                } else {
                    let bytes: Vec<u8> = serde::Deserialize::deserialize(deserializer)?;
                    let arr: [u8; $bytes] = bytes
                        .try_into()
                        .map_err(|_| serde::de::Error::custom("invalid byte length"))?;
                    Ok(Self(arr))
                }
            }
        }

        impl BorshSerialize for $name {
            fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
                self.0.serialize(writer)
            }
        }

        impl BorshDeserialize for $name {
            fn deserialize_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
                <[u8; $bytes] as BorshDeserialize>::deserialize_reader(reader).map(Self)
            }
        }

        impl schemars::JsonSchema for $name {
            fn schema_name() -> String {
                stringify!($name).to_string()
            }

            fn json_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
                serde_json::from_value(serde_json::json!({
                    "type": "string",
                    "pattern": format!(
                        "^{}1[023456789acdefghjklmnpqrstuvwxyz]+$",
                        $hrp_fn()
                    ),
                    "description": format!(
                        "Bech32m-encoded {}-byte value with HRP '{}'.",
                        $bytes,
                        $hrp_fn()
                    ),
                }))
                .expect("static JSON schema is valid")
            }
        }

        #[cfg(feature = "arbitrary")]
        impl<'a> arbitrary::Arbitrary<'a> for $name {
            fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                Ok(Self(<[u8; $bytes] as arbitrary::Arbitrary>::arbitrary(u)?))
            }
        }

        #[cfg(feature = "arbitrary")]
        impl proptest::arbitrary::Arbitrary for $name {
            type Parameters = ();
            type Strategy = proptest::strategy::BoxedStrategy<Self>;

            fn arbitrary_with(_: ()) -> Self::Strategy {
                use proptest::prelude::*;
                any::<[u8; $bytes]>().prop_map(Self).boxed()
            }
        }
    };
}

bech32m_hash_type! {
    /// Transaction hash. Bech32m-encoded with HRP `ltx` for human-facing
    /// display (`ltx1...`); borsh layout is the raw 32-byte array.
    LtxHash, 32, ltx_hrp
}

bech32m_hash_type! {
    /// Block hash. Bech32m-encoded with HRP `lblk` (`lblk1...`).
    LblkHash, 32, lblk_hrp
}

bech32m_hash_type! {
    /// State root. Bech32m-encoded with HRP `lsr` (`lsr1...`); 64 bytes
    /// underneath because chain state roots are 64-byte hashes.
    LsrHash, 64, lsr_hrp
}

// ---- Cross-conversions with `HexString<[u8; N]>` -------------------------
//
// Internal SDK code paths construct hashes from bytes / digest output
// and threaded `HexHash` through. After the alias swap (`TxHash =
// LtxHash`), those paths still produce `HexString<[u8; 32]>` values
// that need to flow into the new types. These conversion impls
// preserve the underlying bytes; the only difference is which Display /
// Serialize they go through.

impl From<crate::common::HexString<[u8; 32]>> for LtxHash {
    fn from(value: crate::common::HexString<[u8; 32]>) -> Self {
        Self(value.0)
    }
}

impl From<LtxHash> for crate::common::HexString<[u8; 32]> {
    fn from(value: LtxHash) -> Self {
        crate::common::HexString::new(value.0)
    }
}

#[allow(deprecated)]
impl From<digest::generic_array::GenericArray<u8, digest::typenum::U32>> for LtxHash {
    #[allow(deprecated)]
    fn from(value: digest::generic_array::GenericArray<u8, digest::typenum::U32>) -> Self {
        Self(value.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ltx_round_trip() {
        let bytes = [0xab; 32];
        let h = LtxHash::new(bytes);
        let s = h.to_string();
        assert!(s.starts_with("ltx1"), "got {s}");
        let parsed: LtxHash = s.parse().unwrap();
        assert_eq!(parsed, h);
    }

    #[test]
    fn lblk_round_trip() {
        let bytes = [0xcd; 32];
        let h = LblkHash::new(bytes);
        let s = h.to_string();
        assert!(s.starts_with("lblk1"), "got {s}");
        let parsed: LblkHash = s.parse().unwrap();
        assert_eq!(parsed, h);
    }

    #[test]
    fn lsr_round_trip() {
        let bytes = [0xef; 64];
        let h = LsrHash::new(bytes);
        let s = h.to_string();
        assert!(s.starts_with("lsr1"), "got {s}");
        let parsed: LsrHash = s.parse().unwrap();
        assert_eq!(parsed, h);
    }

    #[test]
    fn ltx_accepts_hex_backcompat_input() {
        let bytes = [0x42; 32];
        let hex_form = format!("0x{}", hex::encode(bytes));
        let parsed: LtxHash = hex_form.parse().unwrap();
        assert_eq!(parsed.0, bytes);
    }

    #[test]
    fn ltx_rejects_wrong_hrp() {
        let lblk_form = LblkHash::new([0x99; 32]).to_string();
        let result: Result<LtxHash, _> = lblk_form.parse();
        assert!(result.is_err());
    }

    #[test]
    fn ltx_borsh_layout_matches_underlying_bytes() {
        let bytes = [0x77; 32];
        let h = LtxHash::new(bytes);
        let serialised = borsh::to_vec(&h).unwrap();
        assert_eq!(serialised, bytes.to_vec());
    }

    #[test]
    fn ltx_json_round_trip() {
        let bytes = [0x21; 32];
        let h = LtxHash::new(bytes);
        let json = serde_json::to_string(&h).unwrap();
        assert!(json.starts_with("\"ltx1"));
        let back: LtxHash = serde_json::from_str(&json).unwrap();
        assert_eq!(back, h);
    }

    #[test]
    fn hexhash_to_ltx_round_trip() {
        let bytes = [0x11; 32];
        let hx = crate::common::HexString::new(bytes);
        let lx: LtxHash = hx.clone().into();
        assert_eq!(lx.0, bytes);
        let back: crate::common::HexString<[u8; 32]> = lx.into();
        assert_eq!(back.0, bytes);
    }
}
