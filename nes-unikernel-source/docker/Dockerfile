FROM debian:sid-slim as Builder
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y libdw-dev p7zip-full cmake git wget build-essential libzstd-dev llvm-16-dev llvm-16-tools mlir-16-tools libmlir-16-dev clang-16 libclang-16-dev
RUN wget https://github.com/nebulastream/nebulastream-dependencies/releases/download/vls-boot_iostream1/nes-dependencies-vls-boot_iostream1-x64-linux-ubuntu-22.04-nes.7z && /usr/lib/p7zip/7z x nes-dependencies-vls-boot_iostream1-x64-linux-ubuntu-22.04-nes.7z && rm nes-dependencies-vls-boot_iostream1-x64-linux-ubuntu-22.04-nes.7z
RUN git clone https://github.com/mmahnic/argumentum.git && cd argumentum && cmake -H. -Bbuild -DCMAKE_BUILD_TYPE=Release && cd build && cmake --build . && cmake --install .
COPY . ./nebulastream/
RUN cd nebulastream && mkdir build && cd build && cmake .. -DNES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR=ON \
                                                           -DCMAKE_PREFIX_PATH=/home/ls/.local/bin/lib/cmake \
                                                           -DMLIR_DIR=/usr/lib/llvm-16/lib/cmake/mlir \
                                                           -DNES_SELF_HOSTING=ON \
                                                           -DCMAKE_TOOLCHAIN_FILE=/nes-dependencies-vls-boot_iostream1-x64-linux-nes/scripts/buildsystems/vcpkg.cmake

RUN cd nebulastream/build && cmake --build . --target unikernel-source -j8

FROM debian:sid-slim as APP
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y libdw-dev libatomic1

WORKDIR /app
COPY --from=Builder /nebulastream/build/nes-unikernel-common/libUnikernelTest.so /usr/lib/
COPY --from=Builder /nebulastream/build/nes-common/libnes-common.so /usr/lib/
COPY --from=Builder /nebulastream/build/nes-common/libnes-grpc.so /usr/lib/
COPY --from=Builder /nebulastream/build/nes-configurations/libnes-configurations.so /usr/lib/
COPY --from=Builder /nebulastream/build/nes-data-types/libnes-data-types.so /usr/lib/
COPY --from=Builder /nebulastream/build/nes-unikernel-source/unikernel-source /app/source

ENTRYPOINT "/app/entrypoint.sh"