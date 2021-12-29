use tokio::net::{TcpListener, tcp::ReadHalf};
use tokio::io::{self, BufReader, AsyncBufReadExt, AsyncWriteExt};
use async_trait::async_trait;
use std::sync::{Arc};

pub struct Webserver<Request>
    where Request: Send {
    address: String,
    protocol: Box<dyn RequestParser<Request> + Send + Sync>,
    handle_request: Box<dyn (Fn(Request) -> String) + Send + Sync>,
}

// safe to implement, because it only has read access to its fields
unsafe impl<Request: Send> Send for Webserver<Request> {}

unsafe impl<Request: Send> Sync for Webserver<Request> {}

pub struct BongoRequest {
    pub sql: String,
}

#[async_trait]
pub trait RequestParser<Request>
    where Request: Send {
    async fn parse(&self, reader: &mut BufReader<ReadHalf>) -> Option<Request>;
}

pub struct BongoRequestParser {
    buffer_allocator_size: usize,
}


impl BongoRequestParser {
    pub fn new(buffer_allocator_size: usize) -> BongoRequestParser {
        Self {
            buffer_allocator_size
        }
    }
}

#[async_trait]
impl RequestParser<BongoRequest> for BongoRequestParser {
    async fn parse(&self, reader: &mut BufReader<ReadHalf>) -> Option<BongoRequest> {
        let mut buffer = Vec::with_capacity(self.buffer_allocator_size);

        let delimiters_front = "{\"sql\":\"".as_bytes();

        match reader.read_until_multiple(delimiters_front, &mut buffer).await {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    return None;
                }

                buffer.clear(); /* discard delimiters */
            }
            Err(_) => { return None; }
        }

        match reader.read_until(b'"', &mut buffer).await {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    return None;
                }

                buffer.pop(); // return delimiter again
            }
            Err(_) => {}
        }

        match reader.read_until(b'}', &mut buffer).await {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    return None;
                }

                // remove all trailing characters until closing curly brace
                buffer = buffer[..buffer.len() - bytes_read].to_vec();
            }
            Err(_) => {}
        }

        let request = BongoRequest {
            sql: String::from_utf8(buffer).unwrap() // safe because our format does only contain ACII, wich requires only one UTF8 byte
        };

        Some(request)
    }
}

#[async_trait]
trait ReadUntilMultiple {
    async fn read_until_multiple<'a>(&mut self, delimiters: &[u8], buffer: &'a mut Vec<u8>) -> io::Result<usize>;
}


#[async_trait]
impl ReadUntilMultiple for BufReader<ReadHalf<'_>> {
    async fn read_until_multiple<'a>(&mut self, delimiters: &[u8], buffer: &'a mut Vec<u8>) -> io::Result<usize> {
        let mut bytes_read = 0;
        for delim in delimiters {
            // return underlying error if not successful
            bytes_read += self.read_until(*delim, buffer).await?;
        }
        return Result::Ok(bytes_read);
    }
}


impl<Request: 'static + Send> Webserver<Request> {
    pub fn new<F, P>(address: &str, protocol: P, handle_request: F) -> Webserver<Request>
        where F: 'static + (Fn(Request) -> String) + Send + Sync,
              P: 'static + RequestParser<Request> + Send + Sync,
              Request: Send {
        Self {
            address: String::from(address),
            protocol: Box::new(protocol),
            handle_request: Box::new(handle_request),
        }
    }

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

    async fn handle_connection(self: Arc<Self>, listener: &TcpListener) -> () {
        let (mut socket, _addr) = listener.accept().await.unwrap();

        tokio::spawn(async move {
            println!("A connection has been opened.");

            let (read_half, mut write_half) = socket.split();
            let mut reader = BufReader::new(read_half);

            loop {
                match self.protocol.parse(&mut reader).await {
                    Some(request) => {
                        write_half.write_all((self.handle_request)(request).as_bytes()).await.unwrap();
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
