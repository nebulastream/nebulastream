FROM debian:sid-slim as Builder
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y libstdc++-13-dev libdw-dev p7zip-full cmake git wget libzstd-dev ninja-build
RUN wget https://github.com/nebulastream/nebulastream-dependencies/releases/download/vls-mt-unikernel/nes-dependencies-vls-mt-unikernel-x64-linux-ubuntu-22.04-nes.7z && \
                                                           /usr/lib/p7zip/7z x nes-dependencies-vls-mt-unikernel-x64-linux-ubuntu-22.04-nes.7z && \
                                                          rm nes-dependencies-vls-mt-unikernel-x64-linux-ubuntu-22.04-nes.7z

RUN wget https://github.com/nebulastream/clang-binaries/releases/download/vls-llvm-rtti/nes-clang-16-ubuntu-22.04-X64.7z && \
                                                           /usr/lib/p7zip/7z x nes-clang-16-ubuntu-22.04-X64.7z && \
                                                           rm nes-clang-16-ubuntu-22.04-X64.7z 
COPY . ./nebulastream/

RUN cd nebulastream && mkdir build && cd build && cmake .. -GNinja -DNES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR=ON \
                                                           -DCMAKE_PREFIX_PATH=/home/ls/.local/bin/lib/cmake \
                                                           -DMLIR_DIR=/usr/lib/llvm-16/lib/cmake/mlir \
                                                           -DCMAKE_CXX_COMPILER=/clang/bin/clang++ \
                                                           -DCMAKE_C_COMPILER=/clang/bin/clang \
                                                           -DClang_DIR=/clang/lib/cmake/clang \
                                                           -DMLIR_DIR=/clang/lib/cmake/mlir \
                                                           -DLLVM_DIR=/clang/lib/cmake/llvm \
                                                           -DNES_USE_UNIKERNEL=ON \
                                                           -DNES_SELF_HOSTING=OFF \
                                                           -DCMAKE_TOOLCHAIN_FILE=/nes-dependencies-vls-mt-unikernel-x64-linux-nes/scripts/buildsystems/vcpkg.cmake

RUN cd nebulastream/build && cmake --build . --target NesUnikernelExporter -j$(nproc)

FROM debian:sid-slim as APP
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y libdw-dev libatomic1 build-essential clang-16

WORKDIR /app
RUN ln -s /usr/bin/clang++-16 /usr/bin/clang++ && ln -s /usr/bin/clang-16 /usr/bin/clang
COPY --from=Builder /nebulastream/build/nes-unikernel-export/NesUnikernelExporter /app/exporter
COPY --from=Builder /nebulastream/build/nes-unikernel-export/libUnikernelQueryExport.so /usr/lib/
COPY --from=Builder /nebulastream/build/nes-unikernel-export/libUnikernelLLVMExport.so /usr/lib/
COPY --from=Builder /nebulastream/build/nes-client/libnes-client.so /usr/lib/
COPY --from=Builder /nebulastream/build/nes-operators/libnes-operators.so /usr/lib/
COPY --from=Builder /nebulastream/build/nes-configurations/libnes-configurations.so /usr/lib/
COPY --from=Builder /nebulastream/build/nes-data-types/libnes-data-types.so /usr/lib/
COPY --from=Builder /nebulastream/build/nes-common/libnes-common.so /usr/lib/
COPY --from=Builder /nebulastream/build/nes-common/libnes-grpc.so /usr/lib/
COPY --from=Builder /clang/lib/*.so* /usr/lib/
COPY --from=Builder /nebulastream/build/include /nebulastream/build/include
COPY --from=Builder /nebulastream/build/nes-common /nebulastream/build/nes-common
COPY --from=Builder /nebulastream/build/nes-runtime /nebulastream/build/nes-runtime
COPY --from=Builder /nebulastream/nes-unikernel-export/include /nebulastream/build/nes-unikernel-export/include
COPY --from=Builder /nebulastream/nes-unikernel/include /nebulastream/build/nes-unikernel/include
COPY --from=Builder /nebulastream/nes-common/include /nebulastream/build/nes-common/include
COPY --from=Builder /nebulastream/nes-runtime/include /nebulastream/build/nes-runtime/include
COPY --from=Builder /nebulastream/build/nes-core /nebulastream/build/nes-core
COPY --from=Builder /nebulastream/build/nes-data-types /nebulastream/build/nes-data-types
COPY nes-unikernel-export/docker/entrypoint.sh /app/
ENTRYPOINT "/app/entrypoint.sh"
