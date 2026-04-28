# syntax=docker/dockerfile:1
# Lightweight runtime base image for running NebulaStream binaries.
# Contains only the runtime dependencies: libc++, grpc_health_probe, and basic utilities.
# This image is pre-built and pushed to the registry so that downstream images
# (worker, CLI, REPL, test containers) can skip network-heavy apt/wget steps at build time.
FROM ubuntu:24.04

ARG LLVM_TOOLCHAIN_VERSION=19
ARG GRPC_HEALTH_PROBE_VERSION=v0.4.40

RUN apt update -y && apt install curl wget gpg -y \
    && curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y \
    && apt install -y libc++1-${LLVM_TOOLCHAIN_VERSION} libc++abi1-${LLVM_TOOLCHAIN_VERSION} \
    && apt clean && rm -rf /var/lib/apt/lists/*

# Add ODBC runtime dependencies for the ODBCSource plugin (MSSQL via msodbcsql18 plus unixodbc).
RUN apt update -y && apt install -y curl gpg \
    && curl -sSL -O https://packages.microsoft.com/config/ubuntu/$(grep VERSION_ID /etc/os-release | cut -d '"' -f 2)/packages-microsoft-prod.deb \
    && dpkg -i packages-microsoft-prod.deb \
    && rm packages-microsoft-prod.deb \
    && apt update -y \
    && ACCEPT_EULA=Y apt install -y msodbcsql18 mssql-tools18 unixodbc \
    && apt clean && rm -rf /var/lib/apt/lists/*

RUN wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-$(dpkg --print-architecture) \
    && chmod +x /bin/grpc_health_probe
