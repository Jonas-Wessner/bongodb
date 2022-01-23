use bongo_server::BongoServer;

#[tokio::main]
async fn main() {
    match BongoServer::start_new("localhost:8080", "bongo_data", true, false).await {
        error => {
            // BongoServer::start_new only returns in error case
            panic!(
                "Some unrecoverable error occurred:\n`{:?}`",
                error.unwrap_err()
            );
        }
    }
}
