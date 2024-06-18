NPROCS:=$(shell grep -c ^processor /proc/cpuinfo)
CROSS_FLAGS=CROSS_CONTAINER_UID=0 CROSS_CONTAINER_GID=0 CROSS_ROOTLESS_CONTAINER_ENGINE=false CROSS_CONTAINER_USER_NAMESPACE=none CROSS_CONTAINER_OPTS=--privileged
RUST_FLAGS=-Dwarnings
# Jetson platform
# TARGET_PLAT=aarch64-unknown-linux-gnu
TARGET_PLAT?=x86_64-unknown-linux-gnu
ifeq ($(TARGET_PLAT),)
TARGET := 
else
TARGET :=--target $(TARGET_PLAT)
endif
RUST_C?=cross
DEBUG_FLAGS=--all-targets $(TARGET) -j $(NPROCS)
RELEASE_FLAGS=--lib --bins $(TARGET) -j $(NPROCS) --release
CARGO_ARGS?=""

default: debug

clean:
	@echo "Cleaning build dir"
	@rm -rf target/*
	@echo "Cleaning using cargo"
	@$(RUST_C) clean
check:
	@echo "Checking"
	@RUSTFLAGS=$(RUST_FLAGS) $(RUST_C) check $(DEBUG_FLAGS) $(CARGO_ARGS)
release:
	@echo "Building release"
	@RUSTFLAGS=$(RUST_FLAGS) $(RUST_C) build $(RELEASE_FLAGS) $(CARGO_ARGS)
tiny:
# https://github.com/johnthagen/min-sized-rust
	@echo "Building tiny version"
	@rustup toolchain install nightly
	@rustup component add rust-src --toolchain nightly
	@$(RUST_C) +nightly build -Z build-std=std,panic_abort $(RELEASE_FLAGS) -Z build-std-features=panic_immediate_abort --profile tiny
	@upx --best --lzma ./target/$(TARGET_PLAT)/tiny/iluvatar_*
debug:
	@echo "Building debug"
	@RUSTFLAGS=$(RUST_FLAGS) $(RUST_C) build $(DEBUG_FLAGS) $(CARGO_ARGS)
spans:
	@echo "Building full_spans"
	@RUSTFLAGS=$(RUST_FLAGS) $(RUST_C) build --features full_spans $(RELEASE_FLAGS) $(CARGO_ARGS)
spansd:
	@echo "Building debug full_spans"
	@RUSTFLAGS=$(RUST_FLAGS) $(RUST_C) build --features full_spans $(DEBUG_FLAGS) $(CARGO_ARGS)
flame:
	@echo "TODO: implement running flame"
test:
	@echo "Running unit tests"
# TODO: this is probably broken, Docker-in-Docker etc.
# https://github.com/cross-rs/cross?tab=readme-ov-file#docker-in-docker
	@RUSTFLAGS=$(RUST_FLAGS) cargo test $(CARGO_ARGS)
format:
	@cargo fmt --all
format-check:
	@cargo fmt --all -- --check
clippy:
	@RUSTFLAGS=$(RUST_FLAGS) $(RUST_C) clippy --workspace --examples --benches --no-deps -- -Dclippy::suspicious -Dclippy::correctness -Dclippy::perf -Aclippy::single_match -Aclippy::new_without_default -Aclippy::too_many_arguments -Aclippy::type-complexity -Dclippy::from_over_into -Aclippy::redundant-field-names -Dwarnings
