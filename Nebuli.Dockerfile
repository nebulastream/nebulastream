FROM ubuntu:24.04 AS app
ENV LLVM_TOOLCHAIN_VERSION=19
RUN apt update -y && apt install curl gpg pipx wget -y
RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y \
    && apt install -y libusb-1.0-0 libatomic1 libc++1-${LLVM_TOOLCHAIN_VERSION} libc++abi1-${LLVM_TOOLCHAIN_VERSION}

RUN GRPC_HEALTH_PROBE_VERSION=v0.4.13 && \
  wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-$(dpkg --print-architecture) && \
  chmod +x /bin/grpc_health_probe    

ENV PIPX_HOME=/opt/pipx \
    PIPX_BIN_DIR=/usr/local/bin \
    PATH=$PIPX_BIN_DIR:$PATH

RUN pipx ensurepath
RUN pipx install iree-base-compiler==3.3.0 && pipx inject iree-base-compiler onnx

# Adding query_coordinator.sh content
RUN cat <<EOF > /usr/bin/query_coordinator.sh
#! /usr/bin/bash


function stop_query() {
    nes-nebuli -s nes:8080 stop \$QUERY_ID || echo "Could not stop query \$QUERY_ID"
    echo "Stopped"
}

function check_status() {
    if /bin/grpc_health_probe -addr nes:8080; then
        STATUS=\$(nes-nebuli -s nes:8080 status \$QUERY_ID)
        if [ "\$STATUS" == "Running" ]; then 
            return 0       
        elif [ "\$STATUS" == "Registered" ]; then
            echo "Registered"
            return 0
        elif [ "\$STATUS" == "Started" ]; then
            echo "Started"
            return 0
        else 
            echo \$STATUS
            return 1
        fi
    else
        return 1
    fi
}


QUERY_ID=\$(nes-nebuli -s nes:8080 register -x -i \$@)
if [ \$? -ne 0 ]; then
    echo "Could not start query: \$QUERY_ID"
    exit 1
fi
trap stop_query EXIT
echo "Started"

while check_status; do
    sleep 10
done

echo "Status: \$(nes-nebuli -s nes:8080 status \$QUERY_ID)"
EOF
RUN chmod +x /usr/bin/query_coordinator.sh

ADD nes-nebuli /usr/bin
ENTRYPOINT ["query_coordinator.sh"]
