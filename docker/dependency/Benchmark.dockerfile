# The benchmarking image adds common benchmarking tools and python modules need to run the benchmarking scripts and
# upload the results
ARG TAG=latest
FROM nebulastream/nes-development:${TAG}

RUN apt-get update && apt-get install python3-pip -y

RUN apt-get install linux-tools-common linux-tools-generic linux-tools-`uname -r` -y

RUN python3 -m pip install benchadapt benchrun --break-system-packages
