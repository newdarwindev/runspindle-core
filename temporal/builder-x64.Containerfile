FROM docker.io/library/rust@sha256:6ae102bdbf528294bc79ad6e1fae682f6f7c2a6e6621506ba959f9685b308a55
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler=3.21.12-3+deb12u1 libprotobuf-dev=3.21.12-3+deb12u1 && rm -rf /var/lib/apt/lists/*
RUN rustup component add clippy --toolchain 1.94.1-x86_64-unknown-linux-gnu
