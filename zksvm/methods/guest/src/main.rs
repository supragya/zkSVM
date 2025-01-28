use risc0_zkvm::guest::env;

/// This module contains the hardcoded programs that are used in the zkVM.
/// The programs are stored as byte arrays. VERY SPECIFIC TO THIS EXECUTION ENVIRONMENT
pub(crate) mod hardcoded_programs;
pub(crate) mod hardcoded_keypairs;

pub(crate) mod mock_bank;
pub(crate) mod test_environment;

fn main() {
    for test_entry in test_environment::program_medley() {
        let env = test_environment::SvmTestEnvironment::create(test_entry);
        env.execute();
    }
}
