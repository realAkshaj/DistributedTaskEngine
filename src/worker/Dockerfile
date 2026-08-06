# syntax=docker/dockerfile:1
FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential cmake pkg-config \
      protobuf-compiler protobuf-compiler-grpc \
      libprotobuf-dev libgrpc++-dev \
      nlohmann-json3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY src/Contracts /src/Contracts
COPY src/worker    /src/worker

WORKDIR /src/worker
RUN cmake -B build -DCMAKE_BUILD_TYPE=Release \
 && cmake --build build -j"$(nproc)"

FROM ubuntu:22.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      libgrpc++-dev libprotobuf-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 10001 dte

COPY --from=builder /src/worker/build/dte_worker /usr/local/bin/dte_worker

USER dte
ENTRYPOINT ["/usr/local/bin/dte_worker"]
