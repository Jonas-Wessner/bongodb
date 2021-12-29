use async_trait::async_trait;
use tokio::io::{self, BufReader, AsyncBufReadExt};
use tokio::net::tcp::ReadHalf;

use crate::bongo_server::webserver::RequestParser;

pub struct BongoRequest {
    pub sql: String,
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