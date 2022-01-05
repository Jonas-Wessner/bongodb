use tokio::net::{TcpListener};
use tokio::io::{BufReader, AsyncWriteExt, AsyncReadExt};
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
pub trait RequestParser<Request>
// currently requests only require to be `Send`
    where Request: Send {
    fn parse(&self, bytes: &[u8]) -> Option<Request>;
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
              P: 'static + RequestParser<Request> + Send + Sync {
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
            let mut size = [1; 4];

            loop {
                match reader.read_exact(&mut size).await {
                    Ok(_) => {
                        let size = i32::from_be_bytes(size);

                        let mut buffer = Vec::with_capacity(size as usize);
                        unsafe { buffer.set_len(size as usize); } // extend size of vector over the allocated space#

                        match reader.read_exact(&mut buffer).await {
                            Ok(_) => {
                                let response: String;
                                match self.request_parser.parse(&buffer) {
                                    Some(request) => {
                                        response = (self.handle_request)(request);
                                    }
                                    None => {
                                        response = "Request format could not be parsed, request is ignored.".to_string();
                                    }
                                }
                                let size = &(response.len() as u32).to_be_bytes();
                                write_half.write_all(&[size, response.as_bytes()].concat()).await.unwrap();
                                write_half.flush().await.unwrap();
                            }
                            Err(_) => {
                                println!("Reading request with size of {} bytes not successful. Therefore connection closed.", size);
                                break;
                            }
                        }
                    }
                    Err(_) => {
                        println!("Reading 32-bit request header not successful. Therefore connection closed.");
                        break;
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::webserver::{Webserver, RequestParser};
    use std::io::prelude::*;
    use std::net::TcpStream;
    use std::{thread, time};

    pub struct ExampleRequestParser {}

    impl RequestParser<String> for ExampleRequestParser {
        fn parse(&self, bytes: &[u8]) -> Option<String> {
            Some(String::from_utf8(bytes.to_vec()).unwrap())
        }
    }

    #[test]
    fn server_connect_receive_send() {
        println!("started");
        let output = tokio_test::block_on(
            Webserver::new(
                "localhost:8080", // connect to localhost
                ExampleRequestParser {}, // parse a string from request
                |request| -> String { // just echo the request
                    assert_eq!(request, "Hello World!".to_string()); // TODO: remove this
                    request
                },
            ).start()
        );
        println!("{}", output);
    }


    #[test]
    fn client_connect_receive_send() {
        // wait until server is up
        thread::sleep(time::Duration::from_secs_f32(0.5));

        let mut stream = TcpStream::connect("localhost:8080").unwrap();

        let request = "Hello World!";
        let size = &(request.len() as u32).to_be_bytes();

        stream.write(&[size, request.as_bytes()].concat()).unwrap();

        let mut size: [u8; 4] = [0; 4];
        stream.read_exact(&mut size).unwrap();
        let size = i32::from_be_bytes(size) as usize;

        assert_eq!("Hello World!".len(), size);

        let mut response_buffer = Vec::with_capacity(size);
        unsafe { response_buffer.set_len(size); } // resize buffer over allocated memory

        stream.read_exact(&mut response_buffer).unwrap();

        assert_eq!("Hello World!".to_string(), String::from_utf8(response_buffer).unwrap());
    }
}

