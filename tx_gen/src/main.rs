use std::{collections::VecDeque, sync::Arc, time::Instant};

use solana_sdk::{compute_budget::ComputeBudgetInstruction, hash::Hash, message::Message, pubkey::Pubkey, signature::{Keypair, Signer}, system_instruction, timing::timestamp, transaction::Transaction};
use rand::distributions::{Distribution, Uniform};
use rayon::prelude::*;
use spl_instruction_padding::instruction::wrap_instruction;

/// Keypairs split into source and destination
/// used for transfer transactions
struct KeypairChunks<'a> {
    source: Vec<Vec<&'a Keypair>>,
    dest: Vec<VecDeque<&'a Keypair>>,
}


impl<'a> KeypairChunks<'a> {
    /// Split input slice of keypairs into two sets of chunks of given size
    fn new(keypairs: &'a [Keypair], chunk_size: usize) -> Self {
        // Use `chunk_size` as the number of conflict groups per chunk so that each destination key is unique
        Self::new_with_conflict_groups(keypairs, chunk_size, chunk_size)
    }

    /// Split input slice of keypairs into two sets of chunks of given size. Each chunk
    /// has a set of source keys and a set of destination keys. There will be
    /// `num_conflict_groups_per_chunk` unique destination keys per chunk, so that the
    /// destination keys may conflict with each other.
    fn new_with_conflict_groups(
        keypairs: &'a [Keypair],
        chunk_size: usize,
        num_conflict_groups_per_chunk: usize,
    ) -> Self {
        let mut source_keypair_chunks: Vec<Vec<&Keypair>> = Vec::new();
        let mut dest_keypair_chunks: Vec<VecDeque<&Keypair>> = Vec::new();
        for chunk in keypairs.chunks_exact(2 * chunk_size) {
            source_keypair_chunks.push(chunk[..chunk_size].iter().collect());
            dest_keypair_chunks.push(
                std::iter::repeat(&chunk[chunk_size..chunk_size + num_conflict_groups_per_chunk])
                    .flatten()
                    .take(chunk_size)
                    .collect(),
            );
        }
        KeypairChunks {
            source: source_keypair_chunks,
            dest: dest_keypair_chunks,
        }
    }
}

#[derive(Eq, PartialEq, Debug)]
pub struct InstructionPaddingConfig {
    pub program_id: Pubkey,
    pub data_size: u32,
}

#[derive(Debug, PartialEq)]
pub enum ComputeUnitPrice {
    Fixed(u64),
    Random,
}

#[derive(Debug, PartialEq, Default, Eq, Clone)]
pub(crate) struct TimestampedTransaction {
    transaction: Transaction,
    timestamp: Option<u64>,
    compute_unit_price: Option<u64>,
}

struct TransactionChunkGenerator<'a, 'b, T: ?Sized> {
    client: Arc<T>,
    account_chunks: KeypairChunks<'a>,
    nonce_chunks: Option<KeypairChunks<'b>>,
    chunk_index: usize,
    reclaim_lamports_back_to_source_account: bool,
    compute_unit_price: Option<ComputeUnitPrice>,
    instruction_padding_config: Option<InstructionPaddingConfig>,
    skip_tx_account_data_size: bool,
}


impl<'a, 'b, T> TransactionChunkGenerator<'a, 'b, T>
where
    T: 'static /* + TpsClient */ + Send + Sync + ?Sized,
{
    fn new(
        client: Arc<T>,
        gen_keypairs: &'a [Keypair],
        nonce_keypairs: Option<&'b Vec<Keypair>>,
        chunk_size: usize,
        compute_unit_price: Option<ComputeUnitPrice>,
        instruction_padding_config: Option<InstructionPaddingConfig>,
        num_conflict_groups: Option<usize>,
        skip_tx_account_data_size: bool,
    ) -> Self {
        let account_chunks = if let Some(num_conflict_groups) = num_conflict_groups {
            KeypairChunks::new_with_conflict_groups(gen_keypairs, chunk_size, num_conflict_groups)
        } else {
            KeypairChunks::new(gen_keypairs, chunk_size)
        };
        let nonce_chunks =
            nonce_keypairs.map(|nonce_keypairs| KeypairChunks::new(nonce_keypairs, chunk_size));

        TransactionChunkGenerator {
            client,
            account_chunks,
            nonce_chunks,
            chunk_index: 0,
            reclaim_lamports_back_to_source_account: false,
            compute_unit_price,
            instruction_padding_config,
            skip_tx_account_data_size,
        }
    }

    /// generate transactions to transfer lamports from source to destination accounts
    /// if durable nonce is used, blockhash is None
    fn generate(&mut self, blockhash: Option<&Hash>) -> Vec<TimestampedTransaction> {
        let tx_count = self.account_chunks.source.len();
        println!(
            "Signing transactions... {} (reclaim={}, blockhash={:?})",
            tx_count, self.reclaim_lamports_back_to_source_account, blockhash
        );
        let signing_start = Instant::now();

        let source_chunk = &self.account_chunks.source[self.chunk_index];
        let dest_chunk = &self.account_chunks.dest[self.chunk_index];
        let transactions = if let Some(nonce_chunks) = &self.nonce_chunks {
            unimplemented!("Nonced transactions are not supported yet");
            // let source_nonce_chunk = &nonce_chunks.source[self.chunk_index];
            // let dest_nonce_chunk: &VecDeque<&Keypair> = &nonce_chunks.dest[self.chunk_index];
            // generate_nonced_system_txs(
            //     self.client.clone(),
            //     source_chunk,
            //     dest_chunk,
            //     source_nonce_chunk,
            //     dest_nonce_chunk,
            //     self.reclaim_lamports_back_to_source_account,
            //     self.skip_tx_account_data_size,
            //     &self.instruction_padding_config,
            // )
        } else {
            assert!(blockhash.is_some());
            generate_system_txs(
                source_chunk,
                dest_chunk,
                self.reclaim_lamports_back_to_source_account,
                blockhash.unwrap(),
                &self.instruction_padding_config,
                &self.compute_unit_price,
                self.skip_tx_account_data_size,
            )
        };

        let duration = signing_start.elapsed();
        let ns = duration.as_secs() * 1_000_000_000 + u64::from(duration.subsec_nanos());
        let bsps = (tx_count) as f64 / ns as f64;
        let nsps = ns as f64 / (tx_count) as f64;
        println!(
            "Done. {:.2} thousand signatures per second, {:.2} us per signature, {} ms total time, {:?}",
            bsps * 1_000_000_f64,
            nsps / 1_000_f64,
            duration.as_millis(),
            blockhash,
        );
        // datapoint_info!(
        //     "bench-tps-generate_txs",
        //     ("duration", duration.as_micros() as i64, i64)
        // );

        transactions
    }
}

// Add prioritization fee to transfer transactions, if `compute_unit_price` is set.
// If `Random` the compute-unit-price is determined by generating a random number in the range
// 0..MAX_RANDOM_COMPUTE_UNIT_PRICE then multiplying by COMPUTE_UNIT_PRICE_MULTIPLIER.
// If `Fixed` the compute-unit-price is the value of the `compute-unit-price` parameter.
// It also sets transaction's compute-unit to TRANSFER_TRANSACTION_COMPUTE_UNIT. Therefore the
// max additional cost is:
// `TRANSFER_TRANSACTION_COMPUTE_UNIT * MAX_COMPUTE_UNIT_PRICE * COMPUTE_UNIT_PRICE_MULTIPLIER / 1_000_000`
const MAX_RANDOM_COMPUTE_UNIT_PRICE: u64 = 50;
const COMPUTE_UNIT_PRICE_MULTIPLIER: u64 = 1_000;
const TRANSFER_TRANSACTION_COMPUTE_UNIT: u32 = 600; // 1 transfer is plus 3 compute_budget ixs
const PADDED_TRANSFER_COMPUTE_UNIT: u32 = 3_000; // padding program execution requires consumes this amount

fn generate_system_txs(
    source: &[&Keypair],
    dest: &VecDeque<&Keypair>,
    reclaim: bool,
    blockhash: &Hash,
    instruction_padding_config: &Option<InstructionPaddingConfig>,
    compute_unit_price: &Option<ComputeUnitPrice>,
    skip_tx_account_data_size: bool,
) -> Vec<TimestampedTransaction> {
    let pairs: Vec<_> = if !reclaim {
        source.iter().zip(dest.iter()).collect()
    } else {
        dest.iter().zip(source.iter()).collect()
    };

    if let Some(compute_unit_price) = compute_unit_price {
        let compute_unit_prices = match compute_unit_price {
            ComputeUnitPrice::Random => {
                let mut rng = rand::thread_rng();
                let range = Uniform::from(0..MAX_RANDOM_COMPUTE_UNIT_PRICE);
                (0..pairs.len())
                    .map(|_| {
                        range
                            .sample(&mut rng)
                            .saturating_mul(COMPUTE_UNIT_PRICE_MULTIPLIER)
                    })
                    .collect()
            }
            ComputeUnitPrice::Fixed(compute_unit_price) => vec![*compute_unit_price; pairs.len()],
        };

        let pairs_with_compute_unit_prices: Vec<_> =
            pairs.iter().zip(compute_unit_prices.iter()).collect();

        pairs_with_compute_unit_prices
            .par_iter()
            .map(|((from, to), compute_unit_price)| {
                let compute_unit_price = Some(**compute_unit_price);
                TimestampedTransaction {
                    transaction: transfer_with_compute_unit_price_and_padding(
                        from,
                        &to.pubkey(),
                        1,
                        *blockhash,
                        instruction_padding_config,
                        compute_unit_price,
                        skip_tx_account_data_size,
                    ),
                    timestamp: Some(timestamp()),
                    compute_unit_price,
                }
            })
            .collect()
    } else {
        pairs
            .par_iter()
            .map(|(from, to)| TimestampedTransaction {
                transaction: transfer_with_compute_unit_price_and_padding(
                    from,
                    &to.pubkey(),
                    1,
                    *blockhash,
                    instruction_padding_config,
                    None,
                    skip_tx_account_data_size,
                ),
                timestamp: Some(timestamp()),
                compute_unit_price: None,
            })
            .collect()
    }
}

// In case of plain transfer transaction, set loaded account data size to 30K.
// It is large enough yet smaller than 32K page size, so it'd cost 0 extra CU.
const TRANSFER_TRANSACTION_LOADED_ACCOUNTS_DATA_SIZE: u32 = 30 * 1024;
// In case of padding program usage, we need to take into account program size
const PADDING_PROGRAM_ACCOUNT_DATA_SIZE: u32 = 28 * 1024;

fn get_transaction_loaded_accounts_data_size(enable_padding: bool) -> u32 {
    if enable_padding {
        TRANSFER_TRANSACTION_LOADED_ACCOUNTS_DATA_SIZE + PADDING_PROGRAM_ACCOUNT_DATA_SIZE
    } else {
        TRANSFER_TRANSACTION_LOADED_ACCOUNTS_DATA_SIZE
    }
}

fn transfer_with_compute_unit_price_and_padding(
    from_keypair: &Keypair,
    to: &Pubkey,
    lamports: u64,
    recent_blockhash: Hash,
    instruction_padding_config: &Option<InstructionPaddingConfig>,
    compute_unit_price: Option<u64>,
    skip_tx_account_data_size: bool,
) -> Transaction {
    let from_pubkey = from_keypair.pubkey();
    let transfer_instruction = system_instruction::transfer(&from_pubkey, to, lamports);
    let instruction = if let Some(instruction_padding_config) = instruction_padding_config {
        wrap_instruction(
            instruction_padding_config.program_id,
            transfer_instruction,
            vec![],
            instruction_padding_config.data_size,
        )
        .expect("Could not create padded instruction")
    } else {
        transfer_instruction
    };
    let mut instructions = vec![];
    if !skip_tx_account_data_size {
        instructions.push(
            ComputeBudgetInstruction::set_loaded_accounts_data_size_limit(
                get_transaction_loaded_accounts_data_size(instruction_padding_config.is_some()),
            ),
        )
    }
    instructions.push(instruction);
    if instruction_padding_config.is_some() {
        // By default, CU budget is DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT which is much larger than needed
        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(
            PADDED_TRANSFER_COMPUTE_UNIT,
        ));
    }

    if let Some(compute_unit_price) = compute_unit_price {
        instructions.extend_from_slice(&[
            ComputeBudgetInstruction::set_compute_unit_limit(TRANSFER_TRANSACTION_COMPUTE_UNIT),
            ComputeBudgetInstruction::set_compute_unit_price(compute_unit_price),
        ])
    }
    let message = Message::new(&instructions, Some(&from_pubkey));
    Transaction::new(&[from_keypair], message, recent_blockhash)
}

pub fn main() {

}