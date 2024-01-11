#!/bin/bash
export RUSTFLAGS="-Cinstrument-coverage"
export LLVM_PROFILE_FILE="scupt-raft-%p-%m.profraw"
