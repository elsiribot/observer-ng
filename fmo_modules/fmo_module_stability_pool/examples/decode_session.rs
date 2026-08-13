//! Decodes a real, production `SessionOutcome` blob and prints the amounts of
//! every `multi_sig_stability_pool` input/output it contains, using the same
//! decoder registry the observer's dispatch engine builds. This is a manual
//! validation aid, not part of the test suite:
//!
//! ```text
//! cargo run -p fmo_module_stability_pool --example decode_session -- \
//!     <config.hex> <session.hex>
//! ```
//!
//! where `config.hex` is the hex of a federation's consensus-encoded
//! `ClientConfig` (the `federations.config` bytea) and `session.hex` is the hex
//! of a `sessions.data` blob for that federation.

use std::sync::Arc;

use fedimint_core::config::ClientConfig;
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::epoch::ConsensusItem;
use fedimint_core::hex;
use fedimint_core::session_outcome::SessionOutcome;
use fmo_core::registry::{instance_to_kind, ModuleRegistry};
use fmo_module_stability_pool::spec::{StabilityPoolInput, StabilityPoolOutput};
use fmo_module_stability_pool::StabilityPoolObserver;

fn read_hex(path: &str) -> Vec<u8> {
    let text = std::fs::read_to_string(path).expect("read file");
    hex::decode(text.trim()).expect("valid hex")
}

fn main() {
    let mut args = std::env::args().skip(1);
    let config_path = args.next().expect("usage: <config.hex> <session.hex>");
    let session_path = args.next().expect("usage: <config.hex> <session.hex>");

    let config = ClientConfig::consensus_decode_whole(&read_hex(&config_path), &Default::default())
        .expect("decode ClientConfig");

    // Exactly the registry dispatch uses: the real stability-pool decoder for
    // its instance, raw fallback for every other module kind.
    let registry = ModuleRegistry::new(vec![Arc::new(StabilityPoolObserver)]);
    let decoders = registry.decoders(&config);

    let session = SessionOutcome::consensus_decode_whole(&read_hex(&session_path), &decoders)
        .expect("decode SessionOutcome");

    let mut inputs = 0usize;
    let mut outputs = 0usize;
    let mut cis = 0usize;
    for (item_index, accepted) in session.items.iter().enumerate() {
        match &accepted.item {
            ConsensusItem::Transaction(tx) => {
                let txid = tx.tx_hash();
                for (i, input) in tx.inputs.iter().enumerate() {
                    if instance_to_kind(&config, input.module_instance_id())
                        != "multi_sig_stability_pool"
                    {
                        continue;
                    }
                    let sp = input
                        .as_any()
                        .downcast_ref::<StabilityPoolInput>()
                        .expect("stability pool input decodes to typed value");
                    inputs += 1;
                    println!("tx {txid} input[{i}]: {sp:?}");
                }
                for (o, output) in tx.outputs.iter().enumerate() {
                    if instance_to_kind(&config, output.module_instance_id())
                        != "multi_sig_stability_pool"
                    {
                        continue;
                    }
                    let sp = output
                        .as_any()
                        .downcast_ref::<StabilityPoolOutput>()
                        .expect("stability pool output decodes to typed value");
                    outputs += 1;
                    let amount = match sp {
                        StabilityPoolOutput::V0(v0) => format!("{v0}"),
                        StabilityPoolOutput::V1(v1) => format!("{v1}"),
                        StabilityPoolOutput::Default { variant, .. } => {
                            format!("unknown variant {variant}")
                        }
                    };
                    println!(
                        "tx {txid} output[{o}] (json={}): {amount}",
                        serde_json::to_string(sp).unwrap_or_default()
                    );
                }
            }
            ConsensusItem::Module(mci) => {
                if instance_to_kind(&config, mci.module_instance_id()) != "multi_sig_stability_pool"
                {
                    continue;
                }
                cis += 1;
                let bytes = mci.consensus_encode_to_vec();
                println!(
                    "item[{item_index}] SP consensus item ({} bytes)",
                    bytes.len()
                );
            }
            ConsensusItem::Default { .. } => {}
        }
    }

    println!(
        "\nDecoded {inputs} SP inputs, {outputs} SP outputs, {cis} SP consensus items \
         across {} session items.",
        session.items.len()
    );
}
