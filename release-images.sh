docker build -f Nebuli.Dockerfile -t nebuli:local /home/ls/dima/nebulastream-public2/cmake-build-relwithdebinfo/nes-nebuli/
docker build -f Worker.Dockerfile -t worker:local /home/ls/dima/nebulastream-public2/cmake-build-relwithdebinfo/nes-single-node-worker 
