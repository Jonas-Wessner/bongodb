use bongo_server::BongoServer;

#[tokio::main]
async fn main() {
    match BongoServer::start_new("localhost:8080").await {
        error_message => {
            // BongoServer::start_new only returns in error case
            panic!("Some unrecoverable error occurred with the message `{}`", error_message);
        }
    }
}