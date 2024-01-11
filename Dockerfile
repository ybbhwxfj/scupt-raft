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

# invalid the cache
# COPY . .

ENV RUSTFLAGS="-Cinstrument-coverage"
ENV LLVM_PROFILE_FILE="scupt-raft-%p-%m.profraw"

COPY Cargo.toml ./
COPY src ./src
COPY tests ./test
COPY data ./data
COPY script ./script

RUN cargo build --verbose
RUN date
RUN cargo test --verbose -- --nocapture
RUN date

RUN grcov . -s . --binary-path ./target/debug/ \
    -t html --branch --ignore-not-existing -o ./target/debug/coverage/
RUN cp -r ./target/debug/coverage /var/www/

RUN echo "\
server {\n\
       listen 8000;\n\
       listen [::]:8000;\n\
\n\
       server_name coverage.com;\n\
\n\
       root /var/www/coverage;\n\
       index index.html;\n\
\n\
}" > /etc/nginx/sites-available/coverage

RUN ln -sf /etc/nginx/sites-available/coverage /etc/nginx/sites-enabled/default

EXPOSE 8000

CMD ["nginx", "-g", "daemon off;"]