use risc0_zkvm::guest::env;

/// This module contains the hardcoded programs that are used in the zkVM.
/// The programs are stored as byte arrays. VERY SPECIFIC TO THIS EXECUTION ENVIRONMENT
mod hardcoded_programs;

fn main() {
    // TODO: Implement your guest code here

    // read the input
    // let input: u32 = env::read();

    // TODO: do something with the input

    // write public output to the journal
    // env::commit(&input);
}
