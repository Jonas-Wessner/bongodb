test-all:
	cargo +nightly test --manifest-path bongo-server/Cargo.toml
	cargo +nightly test --manifest-path bongo-lib/Cargo.toml
	cargo +nightly test --manifest-path bongo-core/Cargo.toml
	cargo +nightly test --manifest-path webserver/Cargo.toml
	cargo +nightly test --manifest-path examples-and-tests/Cargo.toml