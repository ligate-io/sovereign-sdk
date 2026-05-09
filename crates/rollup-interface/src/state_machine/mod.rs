//! Defines types, traits, and helpers that are used by the core state-machine of the rollup.
//! Items in this module must be fully deterministic, since they are expected to be executed inside of zkVMs.
pub mod crypto;
pub mod da;
pub mod stf;
pub mod zk;

use borsh::{BorshDeserialize, BorshSerialize};
pub use bytes::{Buf, BufMut, Bytes, BytesMut};
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sov_universal_wallet::schema::UniversalWallet;

use crate::common::{LbaHash, LblkHash, LbzHash, LschHash, LsrHash, LtxHash, SlotNumber};

pub mod optimistic;
pub mod storage;

/// A rollup transaction hash. Bech32m-encoded with HRP `ltx`
/// (`ltx1...`); borsh layout is the raw 32-byte array.
pub type TxHash = LtxHash;

/// A DA block hash (slot hash on the rollup side). Bech32m with HRP
/// `lblk` (`lblk1...`); 32 bytes.
pub type BlockHash = LblkHash;

/// A rollup state root. Bech32m with HRP `lsr` (`lsr1...`); 32 bytes,
/// matching `<S::Storage as Storage>::Root`.
pub type StateRootHash = LsrHash;

/// A sequencer batch hash. Bech32m with HRP `lba` (`lba1...`).
pub type BatchHash = LbaHash;

/// Runtime / wallet schema commitment hash (`Runtime::CHAIN_HASH`).
/// Bech32m with HRP `lsch` (`lsch1...`).
pub type ChainHash = LschHash;

/// DA-layer blob hash. Bech32m with HRP `lbz` (`lbz1...`).
pub type BlobHash = LbzHash;

/// Defines types and traits distinguishing between "native" (full node) and "zk" execution.
///
/// This module uses a combination of a sealed marker trait, unit structs, and an enum to
/// emulate the behavior of a const-generic enum.
pub mod execution_mode {
    use borsh::{BorshDeserialize, BorshSerialize};
    use serde::{Deserialize, Serialize};

    /// Execution modes for the rollup.
    #[derive(
        Debug,
        Clone,
        Copy,
        PartialEq,
        Eq,
        Hash,
        Serialize,
        Deserialize,
        BorshDeserialize,
        BorshSerialize,
        schemars::JsonSchema,
    )]
    #[serde(rename_all = "snake_case")]
    pub enum RuntimeExecutionMode {
        /// Execution inside of a [`Zkvm`](super::zk::Zkvm).
        Zk,
        /// Execution on a full node.
        Native,
        /// Execution on a full node with the ability to generate proofs.
        /// This adds some overhead on top of the [`RuntimeExecutionMode::Native`] mode.
        WitnessGeneration,
    }
    /// Marker trait for execution modes.
    pub trait ExecutionMode:
        super::sealed::Sealed
        + Send
        + Sync
        + 'static
        + Default
        + Serialize
        + serde::de::DeserializeOwned
    {
        /// An enum variant equivalent to the implementing type.
        const EXECUTION_MODE: RuntimeExecutionMode;
    }
    /// A unit struct marking that execution occurs inside of a [`Zkvm`](super::zk::Zkvm).
    #[derive(
        Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, schemars::JsonSchema,
    )]
    #[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
    pub struct Zk;
    impl ExecutionMode for Zk {
        const EXECUTION_MODE: RuntimeExecutionMode = RuntimeExecutionMode::Zk;
    }
    /// A unit struct marking that execution occurs on a full node.
    #[derive(
        Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, schemars::JsonSchema,
    )]
    #[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
    pub struct Native;
    impl ExecutionMode for Native {
        const EXECUTION_MODE: RuntimeExecutionMode = RuntimeExecutionMode::Native;
    }
    /// A unit struct marking that execution generates a witness, adding additional overhead on top of [`Native`] execution.
    #[derive(
        Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, schemars::JsonSchema,
    )]
    #[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
    pub struct WitnessGeneration;
    impl ExecutionMode for WitnessGeneration {
        const EXECUTION_MODE: RuntimeExecutionMode = RuntimeExecutionMode::WitnessGeneration;
    }
}

mod sealed {
    use super::execution_mode::{Native, WitnessGeneration, Zk};
    pub trait Sealed {}

    impl Sealed for Zk {}
    impl Sealed for Native {}
    impl Sealed for WitnessGeneration {}
}

/// A marker trait for general addresses.
pub trait BasicAddress:
    Ord
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Unpin
    + Sync
    + Clone
    + Copy
    + core::hash::Hash
    + AsRef<[u8]>
    + for<'a> TryFrom<&'a [u8], Error = anyhow::Error>
    + core::str::FromStr<
        Err: core::fmt::Debug + Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    > + Serialize
    + DeserializeOwned
    + BorshDeserialize
    + BorshSerialize
    + UniversalWallet
    + MaybeArbitrary
    + JsonSchema
    + 'static
{
}

/// Implement the `arbitrary::Arbitrary` trait when the `arbitrary` feature is enabled.
#[cfg(feature = "arbitrary")]
pub trait MaybeArbitrary: for<'a> arbitrary::Arbitrary<'a> {}
#[cfg(feature = "arbitrary")]
impl<T: for<'a> arbitrary::Arbitrary<'a>> MaybeArbitrary for T {}

/// Implement the `arbitrary::Arbitrary` trait when the `arbitrary` feature is enabled.
#[cfg(not(feature = "arbitrary"))]
pub trait MaybeArbitrary {}
#[cfg(not(feature = "arbitrary"))]
impl<T> MaybeArbitrary for T {}

/// A tracker that returns the maximum provable height of the rollup.
pub trait ProvableHeightTracker: Send + Sync + 'static {
    /// Returns the maximum provable height of the rollup.
    fn max_provable_slot_number(&self) -> SlotNumber;
}
