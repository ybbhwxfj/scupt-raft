FROM ubuntu:latest

RUN apt-get update && \
    apt-get install --no-install-recommends -y \
    ca-certificates curl file \
    build-essential \
    autoconf automake autotools-dev libtool xutils-dev \
    nginx

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain stable -y

ENV PATH /root/.cargo/bin/:$PATH

RUN rustup default nightly
RUN rustup component add llvm-tools-preview
RUN rustup update
RUN cargo install grcov

WORKDIR /scupt-raft
