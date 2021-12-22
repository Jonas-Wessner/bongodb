use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write, BufWriter};

fn main() {
    // TODO: do not use unwrap here
    let listener = TcpListener::bind("127.0.0.1:8080").unwrap();

    for stream in listener.incoming() {
        // TODO: do not use unwrap here
        let stream = stream.unwrap();
        handle_connection(stream);
    }
}

fn handle_connection(mut stream: TcpStream) {
    let mut buffer = [0; 1024];
    stream.read(&mut buffer).unwrap();
    println!(
        "Request: {}",
        String::from_utf8_lossy(&buffer[..])
    );

    // bufwriter only useful when having multiple write calls
    let mut writer = BufWriter::new(stream);

    let response = "HTTP/1.1 200 OK\r\n\r\n";
    writer.write(response.as_bytes()).unwrap();
    writer.flush().unwrap();
}