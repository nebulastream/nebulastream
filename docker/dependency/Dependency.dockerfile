ARG TAG=latest
FROM nebulastream/nes-development-base:${TAG}

ADD vcpkg /vcpkg_input
WORKDIR /vcpkg_input
RUN git clone https://github.com/microsoft/vcpkg.git vcpkg_repository \
    && vcpkg_repository/bootstrap-vcpkg.sh --disableMetrics \
    && vcpkg_repository/vcpkg install --overlay-triplets=custom-triplets --overlay-ports=vcpkg-registry/ports --triplet="x64-linux-nes" --host-triplet="x64-linux-nes" --x-feature="mlir" \
    && vcpkg_repository/vcpkg export --overlay-triplets=custom-triplets --overlay-ports=vcpkg-registry/ports --triplet="x64-linux-nes" --host-triplet="x64-linux-nes" --raw --output-dir / --output vcpkg \
    && cp vcpkg_repository/scripts/vcpkgTools.xml /vcpkg/scripts/ \
    && rm -rf vcpkg_repository \
    && rm -rf /vcpkg_input \
    && chmod -R 777 /vcpkg
ENV VCPKG_ROOT=/vcpkg


