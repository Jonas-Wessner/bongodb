use tokio::net::{TcpListener, tcp::ReadHalf};
use tokio::io::{self, BufReader, BufWriter, AsyncBufReadExt, AsyncWriteExt};
use async_trait::async_trait;
use std::sync::{Arc};

pub struct Webserver<T>
where T: Send{
    address: String,
    protocol: Box<dyn RequestParser<T> + Send + Sync>,
    handle_request: Box<dyn (Fn(T) -> String) +  Send + Sync>,
}

// safe to implement, because it only has read access to its fields
unsafe impl<T: Send> Send for Webserver<T> {}
unsafe impl<T: Send> Sync for Webserver<T> {}

pub struct Request {
    pub sql: String,
}

#[async_trait]
pub trait RequestParser<T>
where T: Send {
    async fn parse(&self, reader: &mut BufReader<ReadHalf>) -> Option<T>;
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
impl RequestParser<Request> for BongoRequestParser {
    async fn parse(&self, reader: &mut BufReader<ReadHalf>) -> Option<Request> {
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

        let request = Request {
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


impl<T: 'static + Send> Webserver<T> {
    pub fn new<F, P>(address: &str, protocol: P, handle_request: F) -> Webserver<T>
        where F: 'static + (Fn(T) -> String) + Send + Sync ,
              P: 'static + RequestParser<T> + Send + Sync,
              T: Send {
        Self {
            address: String::from(address),
            protocol: Box::new(protocol),
            handle_request: Box::new(handle_request),
        }
    }

    pub async fn start(self) -> io::Result<()> {
        println!("BongoServer started on {}", self.address);

        let listener = TcpListener::bind(&self.address).await?;

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
