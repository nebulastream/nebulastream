./scripts/install-local-docker-environment.sh -l --libstdcxx --no-sanitizer --name process-intelligence -y

docker run --workdir $(pwd) -v $(pwd):$(pwd) nebulastream/nes-development:local-process-intelligence cmake -B cmake-build-debug -DCMAKE_BUILD_TYPE=Debug
docker run --workdir $(pwd) -v $(pwd):$(pwd) nebulastream/nes-development:local-process-intelligence cmake --build cmake-build-debug -j