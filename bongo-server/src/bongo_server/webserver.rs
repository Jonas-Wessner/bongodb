use tokio::net::{TcpListener, tcp::ReadHalf};
use tokio::io::{BufReader, AsyncWriteExt};
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


#[async_trait]
pub trait RequestParser<Request>
    where Request: Send {
    async fn parse(&self, reader: &mut BufReader<ReadHalf>) -> Option<Request>;
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
