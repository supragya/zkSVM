
#[derive(Clone)]
pub struct TransactionRecorder {
    // Typically, shared by all users of PohRecorder
    // But in our case needs to be single threaded
    pub record_sender: Sender<Record>,
    pub is_exited: Arc<AtomicBool>,
}
