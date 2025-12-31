

.PHONY: all build release test clean run

all: build

build:
	cargo build

release:
	cargo build --release
	maturin build -r -m bindings/python/Cargo.toml

test:
	cargo test

clean:
	cargo clean

run: build
	cargo run

python:
	maturin build -r -m bindings/python/Cargo.toml