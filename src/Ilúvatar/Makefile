NPROCS:=$(shell grep -c ^processor /proc/cpuinfo)

default: debug

clean:
	@echo "Cleaning build dir"
	@rm -rf target/*
	@echo "Cleaning using cargo"
	@cargo clean
check:
	@echo "Checking"
	@cargo check
release:
	@echo "Building release"
	@cargo build --release -j $(NPROCS)
tiny:
# https://github.com/johnthagen/min-sized-rust
	@echo "Building tiny version"
	@rustup toolchain install nightly
	@rustup component add rust-src --toolchain nightly
	@cargo +nightly build -Z build-std=std,panic_abort --target x86_64-unknown-linux-gnu -Z build-std-features=panic_immediate_abort --profile tiny
	@upx --best --lzma ./target/x86_64-unknown-linux-gnu/tiny/ilúvatar_*
debug:
	@echo "Building debug"
	@cargo build -j $(NPROCS)
spans:
	@echo "Building full_spans"
	@cargo build --features full_spans --release -j $(NPROCS)
spansd:
	@echo "Building debug full_spans"
	@cargo build --features full_spans -j $(NPROCS)
flame:
	@echo "TODO: implement running flame"
test:
	@echo "Running unit tests"
	@cargo test
