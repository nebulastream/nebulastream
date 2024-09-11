ARG TAG=latest
FROM nebulastream/nes-development:${TAG} AS build

USER root
ADD . /home/ubuntu/src
RUN cd /home/ubuntu/src \
    && cmake -B build -S . -DCPACK_COMPONENTS_ALL="nebuli" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DNES_ENABLES_TESTS=0 \
    && cmake --build build --target nebuli -j \
    && cd build \
    && cpack \
    && mv *.deb /nebuli.deb

FROM nebulastream/nes-development-base:${TAG} AS app
COPY --from=build /nebuli.deb /nebuli.deb
RUN apt install /nebuli.deb -y
ENTRYPOINT ["nebuli"]
