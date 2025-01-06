use std::{
    collections::VecDeque, net::IpAddr, sync::{atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering}, Arc, RwLock}, thread::{sleep, Builder, JoinHandle}, time::{Duration, Instant}
};

use chrono::{DateTime, Utc};
use crossbeam_channel::{unbounded, Sender};
use rand::distributions::{Distribution, Uniform};
use rayon::prelude::*;
use solana_bench_tps::{cli::{ComputeUnitPrice, Config, InstructionPaddingConfig}, send_batch::{get_latest_blockhash, withdraw_durable_nonce_accounts}};
use solana_core::validator::ValidatorConfig;
use solana_faucet::faucet::run_local_faucet;
use solana_local_cluster::{
    cluster::Cluster,
    local_cluster::{ClusterConfig, LocalCluster},
    validator_configs::make_identical_validator_configs,
};
use solana_rpc::rpc::JsonRpcConfig;
use solana_sdk::{
    account::{Account, AccountSharedData}, clock::{DEFAULT_S_PER_SLOT, MAX_PROCESSING_AGE}, commitment_config::CommitmentConfig, compute_budget::ComputeBudgetInstruction, hash::Hash, message::Message, pubkey::Pubkey, rent::Rent, signature::{Keypair, Signature, Signer}, signer::keypair::keypair_from_seed, system_instruction, timing::timestamp, transaction::Transaction
};
use solana_streamer::socket::SocketAddrSpace;
use solana_tps_client::TpsClient;
use spl_instruction_padding::instruction::wrap_instruction;
use thiserror::Error;

use {
    rand::{Rng, SeedableRng},
    rand_chacha::ChaChaRng,
};

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
    T: 'static + TpsClient + Send + Sync + ?Sized,
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
        // datapoint_println!(
        //     "bench-tps-generate_txs",
        //     ("duration", duration.as_micros() as i64, i64)
        // );

        transactions
    }

    fn advance(&mut self) {
        // Rotate destination keypairs so that the next round of transactions will have different
        // transaction signatures even when blockhash is reused.
        self.account_chunks.dest[self.chunk_index].rotate_left(1);
        if let Some(nonce_chunks) = &mut self.nonce_chunks {
            nonce_chunks.dest[self.chunk_index].rotate_left(1);
        }
        // Move on to next chunk
        self.chunk_index = (self.chunk_index + 1) % self.account_chunks.source.len();

        // Switch directions after transferring for each "chunk"
        if self.chunk_index == 0 {
            self.reclaim_lamports_back_to_source_account =
                !self.reclaim_lamports_back_to_source_account;
        }
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

pub struct GenKeys {
    generator: ChaChaRng,
}

impl GenKeys {
    pub fn new(seed: [u8; 32]) -> GenKeys {
        let generator = ChaChaRng::from_seed(seed);
        GenKeys { generator }
    }

    fn gen_seed(&mut self) -> [u8; 32] {
        let mut seed = [0u8; 32];
        self.generator.fill(&mut seed);
        seed
    }

    fn gen_n_seeds(&mut self, n: u64) -> Vec<[u8; 32]> {
        (0..n).map(|_| self.gen_seed()).collect()
    }

    pub fn gen_keypair(&mut self) -> Keypair {
        let mut seed = [0u8; Keypair::SECRET_KEY_LENGTH];
        self.generator.fill(&mut seed[..]);
        keypair_from_seed(&seed).unwrap()
    }

    pub fn gen_n_keypairs(&mut self, n: u64) -> Vec<Keypair> {
        self.gen_n_seeds(n)
            .into_par_iter()
            .map(|seed| {
                let mut keypair_seed = [0u8; Keypair::SECRET_KEY_LENGTH];
                ChaChaRng::from_seed(seed).fill(&mut keypair_seed[..]);
                keypair_from_seed(&keypair_seed).unwrap()
            })
            .collect()
    }
}

const MAX_SPENDS_PER_TX: u64 = 4;

pub fn generate_keypairs(seed_keypair: &Keypair, count: u64) -> (Vec<Keypair>, u64) {
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&seed_keypair.to_bytes()[..32]);
    let mut rnd = GenKeys::new(seed);

    let mut total_keys = 0;
    let mut extra = 0; // This variable tracks the number of keypairs needing extra transaction fees funded
    let mut delta = 1;
    while total_keys < count {
        extra += delta;
        delta *= MAX_SPENDS_PER_TX;
        total_keys += delta;
    }
    (rnd.gen_n_keypairs(total_keys), extra)
}

const NUM_LAMPORTS_PER_ACCOUNT_DEFAULT: u64 = solana_sdk::native_token::LAMPORTS_PER_SOL;

#[derive(Eq, PartialEq, Debug)]
pub enum ExternalClientType {
    // Submits transactions to an Rpc node using an RpcClient
    RpcClient,
    // Submits transactions directly to leaders using a TpuClient, broadcasting to upcoming leaders
    // via TpuClient default configuration
    TpuClient,
}

fn program_account(program_data: &[u8]) -> AccountSharedData {
    AccountSharedData::from(Account {
        lamports: Rent::default().minimum_balance(program_data.len()).min(1),
        data: program_data.to_vec(),
        owner: solana_sdk::bpf_loader::id(),
        executable: true,
        rent_epoch: 0,
    })
}

// Data to establish communication between sender thread and
// LogTransactionService.
#[derive(Clone)]
struct TransactionInfoBatch {
    pub signatures: Vec<Signature>,
    pub sent_at: DateTime<Utc>,
    pub compute_unit_prices: Vec<Option<u64>>,
}

type SignatureBatchSender = Sender<TransactionInfoBatch>;

pub type SharedTransactions = Arc<RwLock<VecDeque<Vec<TimestampedTransaction>>>>;

pub struct LogTransactionService {
    thread_handler: JoinHandle<()>,
}

fn data_file_provided(block_data_file: Option<&str>, transaction_data_file: Option<&str>) -> bool {
    block_data_file.is_some() || transaction_data_file.is_some()
}

// pub fn create_log_transactions_service_and_sender<Client>(
//     client: &Arc<Client>,
//     block_data_file: Option<&str>,
//     transaction_data_file: Option<&str>,
// ) -> (Option<LogTransactionService>, Option<SignatureBatchSender>)
// where
//     Client: 'static + TpsClient + Send + Sync + ?Sized,
// {
//     if data_file_provided(block_data_file, transaction_data_file) {
//         let (sender, receiver) = unbounded();
//         let log_tx_service =
//             LogTransactionService::new(client, receiver, block_data_file, transaction_data_file);
//         (Some(log_tx_service), Some(sender))
//     } else {
//         (None, None)
//     }
// }


fn create_sender_threads<T>(
    client: &Arc<T>,
    shared_txs: &SharedTransactions,
    thread_batch_sleep_ms: usize,
    total_tx_sent_count: &Arc<AtomicUsize>,
    threads: usize,
    exit_signal: Arc<AtomicBool>,
    shared_tx_active_thread_count: &Arc<AtomicIsize>,
    signatures_sender: Option<SignatureBatchSender>,
) -> Vec<JoinHandle<()>>
where
    T: 'static + TpsClient + Send + Sync + ?Sized,
{
    (0..threads)
        .map(|_| {
            let exit_signal = exit_signal.clone();
            let shared_txs = shared_txs.clone();
            let shared_tx_active_thread_count = shared_tx_active_thread_count.clone();
            let total_tx_sent_count = total_tx_sent_count.clone();
            let client = client.clone();
            let signatures_sender = signatures_sender.clone();
            Builder::new()
                .name("solana-client-sender".to_string())
                .spawn(move || {
                    do_tx_transfers(
                        &exit_signal,
                        &shared_txs,
                        &shared_tx_active_thread_count,
                        &total_tx_sent_count,
                        thread_batch_sleep_ms,
                        &client,
                        signatures_sender,
                    );
                })
                .unwrap()
        })
        .collect()
}

// The point at which transactions become "too old", in seconds.
const MAX_TX_QUEUE_AGE: u64 = (MAX_PROCESSING_AGE as f64 * DEFAULT_S_PER_SLOT) as u64;

fn do_tx_transfers<T: TpsClient + ?Sized>(
    exit_signal: &AtomicBool,
    shared_txs: &SharedTransactions,
    shared_tx_thread_count: &Arc<AtomicIsize>,
    total_tx_sent_count: &Arc<AtomicUsize>,
    thread_batch_sleep_ms: usize,
    client: &Arc<T>,
    signatures_sender: Option<SignatureBatchSender>,
) {
    let mut last_sent_time = timestamp();
    'thread_loop: loop {
        if thread_batch_sleep_ms > 0 {
            sleep(Duration::from_millis(thread_batch_sleep_ms as u64));
        }
        let txs = {
            let mut shared_txs_wl = shared_txs.write().expect("write lock in do_tx_transfers");
            shared_txs_wl.pop_front()
        };
        if let Some(txs) = txs {
            shared_tx_thread_count.fetch_add(1, Ordering::Relaxed);
            let num_txs = txs.len();
            println!("Transferring 1 unit {} times...", num_txs);
            let transfer_start = Instant::now();
            let mut old_transactions = false;
            let mut min_timestamp = u64::MAX;
            let mut transactions = Vec::<_>::with_capacity(num_txs);
            let mut signatures = Vec::<_>::with_capacity(num_txs);
            let mut compute_unit_prices = Vec::<_>::with_capacity(num_txs);
            for tx in txs {
                let now = timestamp();
                // Transactions without durable nonce that are too old will be rejected by the cluster Don't bother
                // sending them.
                if let Some(tx_timestamp) = tx.timestamp {
                    if tx_timestamp < min_timestamp {
                        min_timestamp = tx_timestamp;
                    }
                    if now > tx_timestamp && now - tx_timestamp > 1000 * MAX_TX_QUEUE_AGE {
                        old_transactions = true;
                        continue;
                    }
                }
                signatures.push(tx.transaction.signatures[0]);
                transactions.push(tx.transaction);
                compute_unit_prices.push(tx.compute_unit_price);
            }

            // if min_timestamp != u64::MAX {
            //     datapoint_info!(
            //         "bench-tps-do_tx_transfers",
            //         ("oldest-blockhash-age", timestamp() - min_timestamp, i64),
            //     );
            // }

            if let Some(signatures_sender) = &signatures_sender {
                if let Err(error) = signatures_sender.send(TransactionInfoBatch {
                    signatures,
                    sent_at: Utc::now(),
                    compute_unit_prices,
                }) {
                    println!("Receiver has been dropped with error `{error}`, stop sending transactions.");
                    break 'thread_loop;
                }
            }

            if let Err(error) = client.send_batch(transactions) {
                println!("send_batch_sync in do_tx_transfers failed: {}", error);
            }

            // datapoint_info!(
            //     "bench-tps-do_tx_transfers",
            //     (
            //         "time-elapsed-since-last-send",
            //         timestamp() - last_sent_time,
            //         i64
            //     ),
            // );

            last_sent_time = timestamp();

            if old_transactions {
                let mut shared_txs_wl = shared_txs.write().expect("write lock in do_tx_transfers");
                shared_txs_wl.clear();
            }
            shared_tx_thread_count.fetch_add(-1, Ordering::Relaxed);
            total_tx_sent_count.fetch_add(num_txs, Ordering::Relaxed);
            println!(
                "Tx send done. {} ms {} tps",
                transfer_start.elapsed().as_millis(),
                num_txs as f32 / transfer_start.elapsed().as_secs_f32(),
            );
            // datapoint_info!(
            //     "bench-tps-do_tx_transfers",
            //     ("duration", transfer_start.elapsed().as_micros() as i64, i64),
            //     ("count", num_txs, i64)
            // );
        }
        if exit_signal.load(Ordering::Relaxed) {
            break;
        }
    }
}

fn wait_for_target_slots_per_epoch<T>(target_slots_per_epoch: u64, client: &Arc<T>)
where
    T: 'static + TpsClient + Send + Sync + ?Sized,
{
    if target_slots_per_epoch != 0 {
        println!(
            "Waiting until epochs are {} slots long..",
            target_slots_per_epoch
        );
        loop {
            if let Ok(epoch_info) = client.get_epoch_info() {
                if epoch_info.slots_in_epoch >= target_slots_per_epoch {
                    println!("Done epoch_info: {:?}", epoch_info);
                    break;
                }
                println!(
                    "Waiting for epoch: {} now: {}",
                    target_slots_per_epoch, epoch_info.slots_in_epoch
                );
            }
            sleep(Duration::from_secs(3));
        }
    }
}


fn generate_txs<T: 'static + TpsClient + Send + Sync + ?Sized>(
    shared_txs: &SharedTransactions,
    blockhash: &Arc<RwLock<Hash>>,
    chunk_generator: &mut TransactionChunkGenerator<'_, '_, T>,
    threads: usize,
    use_durable_nonce: bool,
) {
    let transactions = if use_durable_nonce {
        chunk_generator.generate(None)
    } else {
        let blockhash = blockhash.read().map(|x| *x).ok();
        chunk_generator.generate(blockhash.as_ref())
    };

    let sz = transactions.len() / threads;
    let chunks: Vec<_> = transactions.chunks(sz).collect();
    {
        let mut shared_txs_wl = shared_txs.write().unwrap();
        for chunk in chunks {
            shared_txs_wl.push_back(chunk.to_vec());
        }
    }
}

fn generate_chunked_transfers<T: 'static + TpsClient + Send + Sync + ?Sized>(
    recent_blockhash: Arc<RwLock<Hash>>,
    shared_txs: &SharedTransactions,
    shared_tx_active_thread_count: Arc<AtomicIsize>,
    mut chunk_generator: TransactionChunkGenerator<'_, '_, T>,
    threads: usize,
    duration: Duration,
    sustained: bool,
    use_durable_nonce: bool,
) {
    // generate and send transactions for the specified duration
    let start = Instant::now();
    let mut last_generate_txs_time = Instant::now();

    while start.elapsed() < duration {
        generate_txs(
            shared_txs,
            &recent_blockhash,
            &mut chunk_generator,
            threads,
            use_durable_nonce,
        );

        // datapoint_info!(
        //     "blockhash_stats",
        //     (
        //         "time_elapsed_since_last_generate_txs",
        //         last_generate_txs_time.elapsed().as_millis(),
        //         i64
        //     )
        // );

        last_generate_txs_time = Instant::now();

        // In sustained mode, overlap the transfers with generation. This has higher average
        // performance but lower peak performance in tested environments.
        if sustained {
            // Ensure that we don't generate more transactions than we can handle.
            while shared_txs.read().unwrap().len() > 2 * threads {
                sleep(Duration::from_millis(1));
            }
        } else {
            while !shared_txs.read().unwrap().is_empty()
                || shared_tx_active_thread_count.load(Ordering::Relaxed) > 0
            {
                sleep(Duration::from_millis(1));
            }
        }
        chunk_generator.advance();
    }
}

pub fn do_bench_tps<T>(
    client: Arc<T>,
    config: Config,
    gen_keypairs: Vec<Keypair>,
    nonce_keypairs: Option<Vec<Keypair>>,
)
where
    T: 'static + TpsClient + Send + Sync + ?Sized,
{
    let Config {
        id,
        threads,
        thread_batch_sleep_ms,
        duration,
        tx_count,
        sustained,
        target_slots_per_epoch,
        compute_unit_price,
        skip_tx_account_data_size,
        use_durable_nonce,
        instruction_padding_config,
        num_conflict_groups,
        block_data_file,
        transaction_data_file,
        ..
    } = config;

    assert!(gen_keypairs.len() >= 2 * tx_count);
    let chunk_generator = TransactionChunkGenerator::new(
        client.clone(),
        &gen_keypairs,
        nonce_keypairs.as_ref(),
        tx_count,
        compute_unit_price,
        instruction_padding_config,
        num_conflict_groups,
        skip_tx_account_data_size,
    );

    let first_tx_count = loop {
        match client.get_transaction_count() {
            Ok(count) => break count,
            Err(err) => {
                println!("Couldn't get transaction count: {:?}", err);
                sleep(Duration::from_secs(1));
            }
        }
    };
    println!("Initial transaction count {}", first_tx_count);

    let exit_signal = Arc::new(AtomicBool::new(false));

    // Setup a thread per validator to sample every period
    // collect the max transaction rate and total tx count seen
    let maxes = Arc::new(RwLock::new(Vec::new()));
    let sample_period = 1; // in seconds

    let shared_txs: SharedTransactions = Arc::new(RwLock::new(VecDeque::new()));

    let blockhash = Arc::new(RwLock::new(get_latest_blockhash(client.as_ref())));
    let shared_tx_active_thread_count = Arc::new(AtomicIsize::new(0));
    let total_tx_sent_count = Arc::new(AtomicUsize::new(0));

    // if we use durable nonce, we don't need blockhash thread
    let blockhash_thread = if !use_durable_nonce {
        unimplemented!("!usable_durable_nonce not implemented")
        // let exit_signal = exit_signal.clone();
        // let blockhash = blockhash.clone();
        // let client = client.clone();
        // let id = id.pubkey();
        // Some(
        //     Builder::new()
        //         .name("solana-blockhash-poller".to_string())
        //         .spawn(move || {
        //             poll_blockhash(&exit_signal, &blockhash, &client, &id);
        //         })
        //         .unwrap(),
        // )
    } else {
        None
    };

    let (log_transaction_service, signatures_sender) = create_log_transactions_service_and_sender(
        &client,
        block_data_file.as_deref(),
        transaction_data_file.as_deref(),
    );

    let sender_threads = create_sender_threads(
        &client,
        &shared_txs,
        thread_batch_sleep_ms,
        &total_tx_sent_count,
        threads,
        exit_signal.clone(),
        &shared_tx_active_thread_count,
        signatures_sender,
    );

    wait_for_target_slots_per_epoch(target_slots_per_epoch, &client);

    let start = Instant::now();

    generate_chunked_transfers(
        blockhash,
        &shared_txs,
        shared_tx_active_thread_count,
        chunk_generator,
        threads,
        duration,
        sustained,
        use_durable_nonce,
    );

    // Stop the sampling threads so it will collect the stats
    exit_signal.store(true, Ordering::Relaxed);

    // println!("Waiting for sampler threads...");
    // if let Err(err) = sample_thread.join() {
    //     println!("  join() failed with: {:?}", err);
    // }

    // join the tx send threads
    println!("Waiting for transmit threads...");
    for t in sender_threads {
        if let Err(err) = t.join() {
            println!("  join() failed with: {:?}", err);
        }
    }

    if let Some(blockhash_thread) = blockhash_thread {
        println!("Waiting for blockhash thread...");
        if let Err(err) = blockhash_thread.join() {
            println!("  join() failed with: {:?}", err);
        }
    }

    // if let Some(log_transaction_service) = log_transaction_service {
    //     println!("Waiting for log_transaction_service thread...");
    //     if let Err(err) = log_transaction_service.join() {
    //         println!("  join() failed with: {:?}", err);
    //     }
    // }

    if let Some(nonce_keypairs) = nonce_keypairs {
        withdraw_durable_nonce_accounts(client.clone(), &gen_keypairs, &nonce_keypairs);
    }

    let balance = client.get_balance(&id.pubkey()).unwrap_or(0);
    // metrics_submit_lamport_balance(balance);

    // compute_and_report_stats(
    //     &maxes,
    //     sample_period,
    //     &start.elapsed(),
    //     total_tx_sent_count.load(Ordering::Relaxed),
    // );

    // let r_maxes = maxes.read().unwrap();
    // r_maxes.first().unwrap().1.txs
}

fn test_bench_tps_local_cluster(config: solana_bench_tps::cli::Config) {
    let native_instruction_processors = vec![];
    let additional_accounts = vec![(
        spl_instruction_padding::ID,
        program_account(include_bytes!(
            "../../agave/bench-tps/tests/fixtures/spl_instruction_padding.so"
        )),
    )];

    let faucet_keypair = Keypair::new();
    let faucet_pubkey = faucet_keypair.pubkey();
    let faucet_addr = run_local_faucet(faucet_keypair, None);

    const NUM_NODES: usize = 1;
    let cluster = LocalCluster::new(
        &mut ClusterConfig {
            node_stakes: vec![999_990; NUM_NODES],
            cluster_lamports: 200_000_000,
            validator_configs: make_identical_validator_configs(
                &ValidatorConfig {
                    rpc_config: JsonRpcConfig {
                        faucet_addr: Some(faucet_addr),
                        ..JsonRpcConfig::default_for_test()
                    },
                    ..ValidatorConfig::default_for_test()
                },
                NUM_NODES,
            ),
            native_instruction_processors,
            additional_accounts,
            ..ClusterConfig::default()
        },
        SocketAddrSpace::Unspecified,
    );

    cluster.transfer(&cluster.funding_keypair, &faucet_pubkey, 100_000_000);

    let client = Arc::new(cluster.build_tpu_quic_client().unwrap_or_else(|err| {
        panic!("Could not create TpuClient with Quic Cache {err:?}");
    }));

    let lamports_per_account = 100;

    let keypair_count = config.tx_count * config.keypair_multiplier;
    let keypairs = solana_bench_tps::bench::generate_and_fund_keypairs(
        client.clone(),
        &config.id,
        keypair_count,
        lamports_per_account,
        false,
        false,
    )
    .unwrap();

    let _total = do_bench_tps(client, config, keypairs, None);

    #[cfg(not(debug_assertions))]
    assert!(_total > 100);
}

pub fn main() {
    test_bench_tps_local_cluster(solana_bench_tps::cli::Config {
        tx_count: 10,
        duration: Duration::from_secs(10),
        use_durable_nonce: true,
        ..solana_bench_tps::cli::Config::default()
    });
}
