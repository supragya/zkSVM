use tx_gen::test_environment::{program_medley, SvmTestEnvironment};

fn main() {
    for test_entry in program_medley() {
        let env = SvmTestEnvironment::create(test_entry);
        env.execute();
    }
}