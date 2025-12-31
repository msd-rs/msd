

all: msd python

msd: 
	cargo build --release --bin msd

python:
	maturin build -r -m bindings/python/Cargo.toml