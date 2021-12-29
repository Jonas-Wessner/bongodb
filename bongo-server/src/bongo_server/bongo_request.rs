use async_trait::async_trait;
use tokio::io::{self, BufReader, AsyncBufReadExt};
use tokio::net::tcp::ReadHalf;

use crate::bongo_server::webserver::RequestParser;

///
/// A Request from a bongo client to a `BongoServer`
///
pub struct BongoRequest {
    pub sql: String,
}

///
/// A Parser that can parse requests of type `BongoRequest`.
///
pub struct BongoRequestParser {
    buffer_allocator_size: usize,
}

impl BongoRequestParser {
    ///
    /// Creates a new instance of `BongoRequestParser`
    ///
    /// * `buffer_allocator_size` - The *initial* size of the underlying `u8` buffer that is used.
    /// Therefore if one entire request transmitted via TCP is less or equal than buffer_allocator_size
    /// bytes long, no reallocation is needed. If the buffer is too small new space will be allocated.
    pub fn new(buffer_allocator_size: usize) -> BongoRequestParser {
        Self {
            buffer_allocator_size
        }
    }
}

#[async_trait]
impl RequestParser<BongoRequest> for BongoRequestParser {
    ///
    /// Parses a `BongoRequest` from the BufReader asynchronously.
    ///
    /// Requests are expected to be ASCII encoded and have the following format:
    ///
    /// { "sql": "THE FIRST REQUEST BODY" }{ "sql": "ANOTHER REQUEST BODY" }
    ///
    /// The `BongoRequest.sql` attribute will be set to the request body
    /// Any characters (usually whitespaces) in between the required separators will be ignored.
    ///
    /// Returns `Ok(BongoRequest)` if a request could be successfully parsed.
    /// Returns `None` if the TCP-connection has been canceled.
    ///
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

///
/// An interface for reading from a stream until multiple delimiters have been read
///
#[async_trait]
trait ReadUntilMultiple {
    ///
    /// Reads from `self` until the first delimiter has been read, then continuing the same way with
    /// the other delimiters until all delimiters have been read.
    /// All read characters are removed from the reader an placed in `buffer`.
    /// This function is equivalent to calling tokio::io::AsyncBufReadExt::read_until on `self`
    /// for each delimiter with `buffer`.
    ///
    /// Returns `Ok(bytes_read)` where `bytes_read` is the number of bytes read from `self` and
    /// therefore also the number of bytes written to `buffer`.
    /// Returns `Err(Error)` if an error occurred.
    ///
    /// * `delimiters` - An array of delimiters that shall be read. Delimiters may be separated by
    /// an arbitrary amount of arbitrary characters in the stream that is read from.
    /// * `buffer` - A buffer that all read bytes (non-delimiters and delimiters) are written to.
    ///
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