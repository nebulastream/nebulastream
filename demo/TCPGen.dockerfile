FROM rust:slim-bookworm as builder
WORKDIR /app
COPY tcp_generator/Cargo.toml tcp_generator/Cargo.lock ./tcp_generator/
COPY tcp_generator/src ./tcp_generator/src
WORKDIR /app/tcp_generator
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /app/tcp_generator/target/release/tcp_generator /usr/local/bin/tcp_generator
ENTRYPOINT ["tcp_generator"]
