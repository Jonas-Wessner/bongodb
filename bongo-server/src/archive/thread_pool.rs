use std::thread;
use std::sync::mpsc::{channel, Receiver};
use std::sync::{mpsc, Arc, Mutex};


/// A pool of `size` threads that can easily used to execute any tasks
///
pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<WorkerMessage>,
}

type Job = Box<dyn FnOnce() + Send + 'static>;

enum WorkerMessage {
    NewJob(Job),
    Terminate,
}


impl ThreadPool {
    /// Creates a new `ThreadPool`.
    ///
    /// The argument size specifies the number of threads in the pool
    ///
    /// # Panics
    /// The function `new` panics if `size` is less than 1.
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
    ///
    /// Exactly one thread inside the pool will pick up and execute the task.
    /// If all threads are busy the task will be picked up and executed as soon as a thread in the pool is free.
    ///
    /// This function never blocks.
    pub fn execute<F>(&self, function: F) -> ()
        where F: FnOnce() + Send + 'static {
        let job = WorkerMessage::NewJob(Box::new(function));
        // Rust documentation says that send can only fail if the other end of the channel was hung up,
        // which we can guarantee is not the case. This is because the Sender object will be released when all
        // Workers in the pool are joint, however this is not happening as long as the ThreadPool object exists
        self.sender.send(job).unwrap();
    }
}

/// Implements a graceful shutdown on object destruction
///
/// Lets all so far requested jobs finish execution before shutdown.
/// Joins all threads after they have finished their tasks.
///
impl Drop for ThreadPool {
    fn drop(&mut self) {
        // sending as many Terminate messages as workers exists.
        // Eventually all workers will receive exactly one terminate and stop executing its endless loop.
        for _ in &self.workers {
            self.sender.send(WorkerMessage::Terminate).unwrap();
        }

        // Waiting for each thread to join.
        // join blocks until the specified worker is done with its task.
        // This happens as soon as all WorkerMessage::NewJob messages have been processed and
        // a worker receives the WorkerMessage::Terminate message
        for worker in &mut self.workers {
            println!("Shutting down worker {}", worker.id);
            if let Some(thread) = worker.thread.take() {
                // Only returns error if worker thread panics.
                // In this case recovery is not sensible and we call therefore unwrap
                // assuming joining worked or panicking the main thread
                thread.join().unwrap()
            }
        }
    }
}

struct Worker {
    id: usize,
    // handle to the `JoinHandle` of the thread or None if the thread has been joined.
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    /// Creates a new `Worker`
    ///
    /// The worker runs a loop checking for new jobs and executing them.
    /// When a worker receives the WorkerMessage::Terminate it exits the loop and finished execution
    ///
    fn new(id: usize, receiver: Arc<Mutex<Receiver<WorkerMessage>>>) -> Worker {
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

            match job {
                WorkerMessage::NewJob(func) => {
                    println!("Thread {} is executing a job", id);
                    func()
                }
                WorkerMessage::Terminate => {
                    println!("Thread {} received terminate message", id);
                    break;
                }
            }
        });
        Worker { id, thread: Some(thread) }
    }
}