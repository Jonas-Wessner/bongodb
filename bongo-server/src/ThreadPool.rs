use std::thread;
use std::sync::mpsc::{channel, Receiver};
use std::sync::{mpsc, Arc, Mutex};

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Job>,
}

type Job = Box<dyn FnOnce() + Send + 'static>;


impl ThreadPool {
    /// Creates a new `ThreadPool`.
    ///
    /// The argument size specifies the number of threads in the pool
    ///
    /// # Panics
    ///
    /// The function `new` panics if `size` is less than 1.
    ///
    ///
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);

        let (sender, receiver) = channel();

        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        };

        ThreadPool { workers, sender }
    }

    /// Executes a `function` on a thread inside the `ThreadPool`
    pub fn execute<F>(&self, function: F) -> ()
        where F: FnOnce() + Send + 'static {
        let job = Box::new(function);
        // Rust documentation says that send can only fail if the other end of the channel was hung up,
        // which we can guarantee is not the case. This is because the Sender object will be released when all
        // Workers in the pool are joint, however this is not happening as long as the ThreadPool object exists
        self.sender.send(job).unwrap();
    }
}

struct Worker {
    id: usize,
    thread: thread::JoinHandle<()>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<Receiver<Job>>>) -> Worker {
        let thread = thread::spawn(move || loop {
            let job = receiver
                .lock()
                // safe because error is only returned in case another thread which holds a lock to the mutex panics
                // In this case we already have a panic in the program and therefore cannot really recover anyways here.
                // Therefore we unwrap and panic in worst case
                .unwrap()
                .recv()
                // safe because we can guarantee that the sender will not hang up the other end of the channel
                .unwrap();

            println!("Thread {} is executing a job", id);

            job();
        });
        Worker { id, thread }
    }
}