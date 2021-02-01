#!/usr/bin/bash -e


# Run this script from root of Nebula Stream codebase

echo "Building NES Package"
docker run -v $PWD:/nebulastream --entrypoint /nebulastream/docker/executableImage/entrypoint-prepare-nes-package.sh nebulastream/nes-build-image:latest

echo "Building Executable Image as 'bdapro/query-merging:latest'"
pushd docker/executableImage
  docker build . -f Dockerfile-NES-Executable -t bdapro/query-merging:latest
popd