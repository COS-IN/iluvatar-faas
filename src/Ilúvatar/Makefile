ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
NPROCS:=$(shell grep -c ^processor /proc/cpuinfo)
CROSS_FLAGS=CROSS_CONTAINER_UID=0 CROSS_CONTAINER_GID=0 CROSS_ROOTLESS_CONTAINER_ENGINE=false CROSS_CONTAINER_USER_NAMESPACE=none CROSS_CONTAINER_OPTS=--privileged
TARGET_CPU?=generic
RUST_FLAGS=-Dwarnings -Ctarget-cpu=$(TARGET_CPU)
# Jetson platform
# TARGET_PLAT=aarch64-unknown-linux-gnu
TARGET_PLAT?=x86_64-unknown-linux-gnu
ifeq ($(TARGET_PLAT),)
TARGET := 
else
TARGET :=--target $(TARGET_PLAT)
endif
RUST_C?=cross
SHARED_FLAGS=$(TARGET) --jobs $(NPROCS)
BIN_FLAGS=--lib --bins $(SHARED_FLAGS)
CARGO_ARGS?=""

default: debug

clean:
	@echo "Cleaning build dir"
	@rm -rf target/*
	@echo "Cleaning using cargo"
	@RUSTFLAGS="$(RUST_FLAGS)" $(RUST_C) clean
check:
	@echo "Checking"
	@RUSTFLAGS="$(RUST_FLAGS)" $(RUST_C) check --all-features --all-targets $(SHARED_FLAGS) $(CARGO_ARGS)
release:
	@echo "Building release"
	@RUSTFLAGS="$(RUST_FLAGS)" $(RUST_C) build $(BIN_FLAGS) --profile=release $(CARGO_ARGS)
relwdebug:
	@echo "Building release w/debug symbols"
	@RUSTFLAGS="$(RUST_FLAGS)" $(RUST_C) build $(BIN_FLAGS) --profile=relwdebug $(CARGO_ARGS)

tiny:
# https://github.com/johnthagen/min-sized-rust
#
	@echo "Building tiny version"
	@rustup toolchain install nightly
	@rustup component add rust-src --toolchain nightly
	@RUSTFLAGS="$(RUST_FLAGS) -Zlocation-detail=none -Zfmt-debug=none" $(RUST_C) \
		+nightly build $(SHARED_FLAGS) --profile tiny \
		-Z build-std=std,panic_abort -Z build-std-features="optimize_for_size" -Z build-std-features=panic_immediate_abort
	@upx --best --lzma ./target/$(TARGET_PLAT)/tiny/iluvatar_*
debug:
	@echo "Building debug"
	@RUSTFLAGS="$(RUST_FLAGS)" $(RUST_C) build --profile=dev $(BIN_FLAGS) $(CARGO_ARGS)
spans:
	@echo "Building full_spans"
	@RUSTFLAGS="$(RUST_FLAGS)" $(RUST_C) build --features full_spans $(BIN_FLAGS) --profile=spans $(CARGO_ARGS)
spansd:
	@echo "Building debug full_spans"
	@RUSTFLAGS="$(RUST_FLAGS)" $(RUST_C) build --features full_spans $(BIN_FLAGS) --profile=spansd $(CARGO_ARGS)
flame:
	@echo "TODO: implement running flame"
test:
	@echo "Running unit tests"
	@-docker rm --force iluv-test
	@docker build -t iluv-test -f Test.Dockerfile .
	@docker run -d --name iluv-test --tmpfs /var/lib/docker:rw -v ~/.cargo/registry:/root/.cargo/registry/ \
             -v "$(ROOT_DIR)/../load/functions/python3:/funcs" -v "$(ROOT_DIR):/app" \
             --privileged docker.io/library/iluv-test
	@sleep 3
#   cache common images used in testing
# 	@docker exec iluv-test sh -c "ctr --address /var/run/docker/containerd/containerd.sock image pull docker.io/alfuerst/hello-iluvatar-action:latest && \
#                                  ctr --address /var/run/docker/containerd/containerd.sock image pull docker.io/alfuerst/json_dumps_loads-iluvatar-action:latest && \
#                                  ctr --address /var/run/docker/containerd/containerd.sock image pull docker.io/alfuerst/iluvatar-action-base:latest && \
#                                  docker pull docker.io/alfuerst/hello-iluvatar-action:latest && \
#                                  docker pull docker.io/alfuerst/json_dumps_loads-iluvatar-action:latest && \
#                                  docker pull docker.io/alfuerst/iluvatar-action-base:latest"
	@docker exec -e PY_FUNCS=/funcs -e "RUSTFLAGS=$(RUST_FLAGS)" --workdir /app iluv-test /root/.cargo/bin/cargo test \
                --jobs $(NPROCS) $(CARGO_ARGS)
	@-docker rm --force iluv-test
format:
	@cargo fmt --all
format-check:
	@cargo fmt --all -- --check
clippy:
	@# Clippy doesn't run on test code. They do some hacky stuff it doesn't approve of
	@RUSTFLAGS="$(RUST_FLAGS)" $(RUST_C) clippy --workspace --examples --benches --no-deps -- -Dwarnings
