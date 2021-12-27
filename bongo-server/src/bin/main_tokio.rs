use tokio::net::TcpListener;
use tokio::io::{AsyncWriteExt, BufReader, AsyncBufReadExt};
use tokio::net::tcp::WriteHalf;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("localhost:8080").await.unwrap();

    loop {
        let (mut socket, _addr) = listener.accept().await.unwrap();

        tokio::spawn(async move {
            let (read_half, mut write_half) = socket.split();

            let mut reader = BufReader::new(read_half);

            let mut buffer = Vec::with_capacity(256);

            loop {
                let bytes_read = reader.read_until(b'}', &mut buffer).await.unwrap();

                if bytes_read == 0 {
                    break;
                }

                handle_request(&mut write_half, &buffer).await;

                buffer.clear();
            }
        });
    }
}

async fn handle_request(write_half: &mut WriteHalf<'_>, buffer: &[u8]) -> () {
    write_half.write_all(&buffer).await.unwrap();
}