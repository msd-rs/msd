

.PHONY: all build release test clean run

all: build

build:
	cargo build --bin msd

release:
	cargo build --release --bin msd
	maturin build -r -m bindings/python/Cargo.toml

test:
	cargo test

clean:
	cargo clean

run: build
	cargo run

python:
	maturin build -r -m bindings/python/Cargo.toml