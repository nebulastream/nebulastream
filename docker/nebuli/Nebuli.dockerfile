ARG TAG=latest
FROM nebulastream/nes-development:${TAG} AS build

USER root
ADD . /home/ubuntu/src
RUN cd /home/ubuntu/src \
    && cmake -B build -S . -DCPACK_COMPONENTS_ALL="nes-nebuli" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DNES_ENABLES_TESTS=0 \
    && cmake --build build --target nes-nebuli -j \
    && cd build \
    && cpack \
    && mv *.deb /nes-nebuli.deb

FROM nebulastream/nes-development-base:${TAG} AS app
COPY --from=build /nes-nebuli.deb /nes-nebuli.deb
RUN apt install /nes-nebuli.deb -y
ENTRYPOINT ["nes-nebuli"]
