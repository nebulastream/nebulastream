# syntax=docker/dockerfile:1
# IREE compiler tools for ML inference (ONNX -> IREE compilation at runtime), layered on top of the
# slim runtime base. Kept out of the base image because it is ~85% of its size and only reachable
# from binaries that lower a query plan themselves: nes-inference shells out to iree-import-onnx and
# iree-compile from LowerToPhysicalInferModel, it does not link the IREE compiler.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ARG IREE_COMPILER_VERSION=3.11.0
# iree-turbine is deliberately not installed. Both console scripts we use are entry points of
# iree-base-compiler; turbine only pulls in iree-base-runtime (~28MB of runtime + tracy libs, incl.
# an 11MB iree-tracy-capture), and the C++ side links iree_runtime_runtime statically from the
# vcpkg ireeruntime port instead. pip itself is removed once the venv is populated.
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-venv && \
    python3 -m venv /opt/iree && \
    /opt/iree/bin/pip install --no-cache-dir --no-compile \
        iree-base-compiler==${IREE_COMPILER_VERSION} \
        onnx && \
    rm -rf /opt/iree/bin/pip* /opt/iree/lib/python*/site-packages/pip \
           /opt/iree/lib/python*/site-packages/pip-*.dist-info && \
    find /opt/iree -name '__pycache__' -type d -prune -exec rm -rf {} + && \
    ln -s /opt/iree/bin/iree-compile /usr/local/bin/iree-compile && \
    ln -s /opt/iree/bin/iree-import-onnx /usr/local/bin/iree-import-onnx && \
    iree-compile --version && \
    iree-import-onnx --help > /dev/null && \
    apt clean && rm -rf /var/lib/apt/lists/*
