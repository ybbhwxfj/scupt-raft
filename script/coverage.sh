#!/bin/bash

export RUSTFLAGS="-Cinstrument-coverage"
export LLVM_PROFILE_FILE="scupt-raft-%p-%m.profraw"
rustup default nightly
rustup component add llvm-tools-preview
rustup update
cargo build --verbose
date
cargo test -- --nocapture
date