use tokio::net::{TcpListener, tcp::ReadHalf};
use tokio::io::{BufReader, AsyncWriteExt};
use async_trait::async_trait;
use std::sync::{Arc};


///
/// A `Webserver` handling tcp connections in an asynchronous multithreaded manner using the tokio library
///
pub struct Webserver<Request>
    where Request: Send {
    address: String,
    // as the size of RequestParser and Fn(Request) is unknown at compile time they have to be
    // stored on the heap using Box
    request_parser: Box<dyn RequestParser<Request> + Send + Sync>,
    handle_request: Box<dyn (Fn(Request) -> String) + Send + Sync>,
}

// safe to implement, because Webserver only has read access to its fields and therefore no mutable
// shared data exists
unsafe impl<Request: Send> Send for Webserver<Request> {}
unsafe impl<Request: Send> Sync for Webserver<Request> {}


///
/// Structs that implement `RequestParser<T>` can be used to parse requests of type `T`
///
#[async_trait]
pub trait RequestParser<Request>
    // currently requests only require to be `Send`
    where Request: Send {
    async fn parse(&self, reader: &mut BufReader<ReadHalf>) -> Option<Request>;
}

impl<Request: 'static + Send> Webserver<Request> {
    ///
    /// Creates a new instance of `Webserver`
    ///
    /// * `address` - An address consisting of HOSTNAME:PORT that the server connects on.
    /// * `request_parser` - A parser that is used to parse individual `Request`s from the TCP-stream.
    /// * `handle_request` - A callback function or closure that is called every time a `Request`
    /// has been parsed from the TCP-stream. This function gets passed the parsed request as an argument.
    /// The returned string will be transmitted via the TCP-stream back to the client of this connection.
    ///
    pub fn new<F, P>(address: &str, request_parser: P, handle_request: F) -> Webserver<Request>
        where F: 'static + (Fn(Request) -> String) + Send + Sync,
              P: 'static + RequestParser<Request> + Send + Sync{
        Self {
            address: String::from(address),
            request_parser: Box::new(request_parser),
            handle_request: Box::new(handle_request),
        }
    }

    ///
    /// Starts the `Webserver` with the attributes supplied in it`s constructor before
    ///
    pub async fn start(self) -> String {
        let listener;

        match TcpListener::bind(&self.address).await {
            Ok(contained_listener) => {
                println!("BongoServer started on {}", &self.address);
                listener = contained_listener;
            }
            Err(_) => { return String::from("Failed to bind to address `") + &self.address + "`"; }
        }

        let caller = Arc::new(self);

        loop {
            Self::handle_connection(Arc::clone(&caller), &listener).await;
        }
    }

    ///
    /// Handles connections in an asynchronous manner
    ///
    /// Note:
    /// We have shared ownership to the instance of `Self` via an `Arc<Self>`.
    /// This is needed because Self is passed into in arbitrary number of `future`s,
    /// which are possibly executed on different threads.
    /// However we do not need mutable access to the instance of `Self`,
    /// so we do not need to use a `Mutex` or other locking mechanisms
    ///
    async fn handle_connection(self: Arc<Self>, listener: &TcpListener) -> () {
        let (mut socket, _addr) = listener.accept().await.unwrap();

        tokio::spawn(async move {
            println!("A connection has been opened.");

            let (read_half, mut write_half) = socket.split();
            let mut reader = BufReader::new(read_half);

            loop {
                // TODO: read first 4 bytes from stream (header), then read that amount of bytes from stream
                //  parse the resulting bytes to the parser, which then parses the Request object from
                //  the read bytes.
                match self.request_parser.parse(&mut reader).await {
                    Some(request) => {
                        // TODO: before writing all bytes of the response write the 4-byte header to stream first.
                        write_half.write_all((self.handle_request)(request).as_bytes()).await.unwrap();
                        write_half.flush().await.unwrap();
                    }
                    None => {
                        println!("A connection has been canceled.");
                        break;
                    }
                }
            }
        });
    }
}

