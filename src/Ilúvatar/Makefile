NPROCS:=$(shell grep -c ^processor /proc/cpuinfo)
RUST_FLAGS=-Dwarnings
BUILD_FLAGS=--all-targets --target $(TARGET_PLAT)
# Jetson platform
# TARGET_PLAT=aarch64-unknown-linux-gnu
TARGET_PLAT?=x86_64-unknown-linux-gnu

$(info Platform: $(TARGET_PLAT))

default: debug

clean:
	@echo "Cleaning build dir"
	@rm -rf target/*
	@echo "Cleaning using cargo"
	@cargo clean
check:
	@echo "Checking"
	@RUSTFLAGS=$(RUST_FLAGS) cargo check $(BUILD_FLAGS) $(cargo_args)
release:
	@echo "Building release"
	@RUSTFLAGS=$(RUST_FLAGS) cargo build --target $(TARGET_PLAT) --release -j $(NPROCS) --lib --bins $(cargo_args)
tiny:
# https://github.com/johnthagen/min-sized-rust
	@echo "Building tiny version"
	@rustup toolchain install nightly
	@rustup component add rust-src --toolchain nightly
	@cargo +nightly build -Z build-std=std,panic_abort --target $(TARGET_PLAT) -Z build-std-features=panic_immediate_abort --profile tiny
	@upx --best --lzma ./target/$(TARGET_PLAT)/tiny/iluvatar_*
debug:
	@echo "Building debug"
	@RUSTFLAGS=$(RUST_FLAGS) cargo build -j $(NPROCS) $(BUILD_FLAGS) $(cargo_args)
spans:
	@echo "Building full_spans"
	@RUSTFLAGS=$(RUST_FLAGS) cargo build --features full_spans --target $(TARGET_PLAT) --release -j $(NPROCS) --lib --bins $(cargo_args)
spansd:
	@echo "Building debug full_spans"
	@RUSTFLAGS=$(RUST_FLAGS) cargo build --features full_spans -j $(NPROCS) $(BUILD_FLAGS) $(cargo_args)
flame:
	@echo "TODO: implement running flame"
test:
	@echo "Running unit tests"
	@cargo test $(cargo_args)
format:
	@cargo fmt --all
format-check:
	@cargo fmt --all -- --check
clippy:
	@RUSTFLAGS=$(RUST_FLAGS) cargo clippy --workspace --examples --benches --no-deps -- -Dclippy::suspicious -Dclippy::correctness -Dclippy::perf -Aclippy::single_match -Aclippy::new_without_default -Aclippy::too_many_arguments -Aclippy::type-complexity -Dclippy::from_over_into -Aclippy::redundant-field-names -Dwarnings
