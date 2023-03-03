VERSION=0.1.0
NAME=ilúvatar
NPROCS:=$(shell grep -c ^processor /proc/cpuinfo)

default: debug

clean:
	@echo "Cleaning build dir"
	@rm -rf target/*
	@echo "Cleaning using cargo"
	@cargo clean
check:
	@echo "Checking $(NAME)"
	@cargo check
release:
	@echo "Building release: $(VERSION)"
	@cargo build --release -j $(NPROCS)
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
