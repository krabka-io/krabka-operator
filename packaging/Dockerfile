FROM rust:1.97.1-bookworm AS build
RUN apt-get update && apt-get install --yes --no-install-recommends clang cmake ninja-build \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --locked --release -p krabka-operator --bin krabka-operator

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/krabka-operator /usr/bin/krabka-operator
ENTRYPOINT ["/usr/bin/krabka-operator"]
