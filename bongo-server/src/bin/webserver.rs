use tokio::net::{TcpListener, TcpStream, tcp::ReadHalf};
use tokio::io::{self, AsyncWriteExt, BufReader, AsyncBufReadExt};
use std::result;
use std::io::Error;
use async_trait::async_trait;
use std::sync::Arc;

pub struct Webserver<'a, 'b, T> {
    address: String,
    protocol: Box<dyn Protocol<T> + 'a>,
    handle_request: Box<dyn Fn(T) -> String + 'b>,
}

// safe to implement, because it only has read access to its fields
unsafe impl<T> Send for Webserver<'_, '_, T> {}
unsafe impl<T> Sync for Webserver<'_, '_, T> {}

pub trait Protocol<T> {
    fn parse(&self, reader: &mut BufReader<ReadHalf>) -> Option<T>;
}

pub struct ClientToServerProto {
    buffer: Vec<u8>,
}

pub struct Request {
    sql: String,
}

impl ClientToServerProto {
    pub fn new(buffer_allocator_size: usize) -> ClientToServerProto {
        Self {
            buffer: Vec::with_capacity(buffer_allocator_size)
        }
    }
}


trait ReadUntilMultiple {
    fn read_until_multiple<'a>(&'a mut self, delimiters: &[u8], buffer: &'a mut Vec<u8>) -> result::Result<usize, &str>;
}

#[async_trait]
impl ReadUntilMultiple for BufReader<ReadHalf<'_>> {
    async fn read_until_multiple<'a>(&'a mut self, delimiters: &[u8], buffer: &'a mut Vec<u8>) -> result::Result<usize, &str> {
        let mut bytes_read = 0;
        for delim in delimiters {
            // return underlying error if not successful
            bytes_read = self.read_until(*delim, buffer).await?;
        }
        return Result::Ok(bytes_read);
    }
}

#[async_trait]
impl Protocol<Request> for ClientToServerProto {
    async fn parse(&mut self, reader: &mut BufReader<ReadHalf>) -> Option<Request> {
        let delimiters = "{\"sql\":\"".as_bytes();

        match reader.read_until_multiple(delimiters, &mut self.buffer) {
            Ok(_) => { self.buffer.clear(); /* discard delimiters */ }
            Err(_) => { return None; }
        }

        let mut bytes_read: usize = 0;

        match reader.read_until(b'}', &mut self.buffer).await {
            Ok(bytes) => {
                if bytes_read == 0 {
                    return None;
                }
                self.buffer.pop(); /* discard ending delimiter */
            }
            Err(_) => { return None; }
        }

        let request = Request {
            sql: String::from_utf8(self.buffer).unwrap() // safe because our format does only contain ACII, wich requires only one UTF8 byte
        };

        self.buffer.clear();

        Some(request)
    }
}


impl<'a, 'b, T> Webserver<'a, 'b, T> {
    pub fn new<F, P>(address: &str, protocol: P, handle_request: F) -> Webserver<T>
        where F: 'a + Fn(T) -> String,
              P: 'b + Protocol<T> {
        Self {
            address: String::from(address),
            protocol: Box::new(protocol),
            handle_request: Box::new(handle_request),
        }
    }

    pub async fn start(self) -> io::Result<()> {
        println!("BongoServer started on {}", self.address);

        let listener = TcpListener::bind("localhost:8080").await?;

        let caller = Arc::new(self);

        loop {
            Self::handle_connection(Arc::clone(&caller), &listener);
        }
    }

    async fn handle_connection(self: Arc<Self>, listener: &TcpListener) -> () {
        let (mut socket, _addr) = listener.accept().await.unwrap();

        tokio::spawn(async move {
            let (read_half, mut write_half) = socket.split();

            let mut reader = BufReader::new(read_half);

            loop {
                match self.protocol.parse(&mut reader) {
                    Some(request) => {
                        (self.handle_request)(request);
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
