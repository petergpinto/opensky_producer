from rust:1.77-buster as builder

COPY . . 
RUN cargo install --path .

FROM debian:bookworm-slim
RUN apt update && apt-get install -y libssl-dev openssl ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/opensky_producer /usr/local/bin/opensky_producer
CMD ["opensky_producer"]