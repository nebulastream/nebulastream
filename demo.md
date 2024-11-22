### Misc
symlink nebuli and single-node

### Links
- Discussion: https://github.com/nebulastream/nebulastream-public/discussions/328
- Design Doc Rendered: https://github.com/nebulastream/nebulastream-public/pull/464/files?short_path=a814e14#diff-a814e140f83559dd1120083c07c3972f8cd539e4b35ff9c01909107be09cc407
- Design Doc Comments: https://github.com/nebulastream/nebulastream-public/pull/464/files#r1829294094
- Query Engine Interface: https://github.com/nebulastream/nebulastream-public/blob/59220ac4ad780d1bb3af751fd3cfc5d4d9236b6d/nes-query-engine/include/QueryEngine.hpp#L32-L33
- Source Failure Test: https://github.com/nebulastream/nebulastream-public/blob/59220ac4ad780d1bb3af751fd3cfc5d4d9236b6d/nes-query-engine/tests/QueryManagerTest.cpp#L200-L239
- Complex Test Scenario: https://github.com/nebulastream/nebulastream-public/blob/59220ac4ad780d1bb3af751fd3cfc5d4d9236b6d/nes-query-engine/tests/QueryManagerTest.cpp#L370-L411

```bash
    ln -sf cmake-build-debug/nes-nebuli/nes-nebuli nebuli
    ln -sf cmake-build-debug/nes-single-node-worker/nes-single-node-worker single-node
```
### Single Node

Start single node engine with enough memory.

```bash
./single-node --workerConfiguration.numberOfBuffersInGlobalBufferManager=5000000    
```

### Launch Queries

```bash
./nes-systests/tcp-server.py -p 5000 -n 100000 -i 0.001
```

```bash
for i in $(seq 1 50)
  cat ./nes-systests/test.yaml | sed "s/GENERATE/\/tmp\/output\/query$i.csv/" | ./nebuli register -x -s localhost:8080
done
```
```bash
for i in (seq 1 50)
  cat ./nes-systests/test.yaml | sed "s/GENERATE/\/tmp\/output\/query$i.csv/" | ./nebuli register -x -s localhost:8080
end
```

### Stop Queries

```bash
for i in (seq 1 50)
  ./nebuli stop $i -s localhost:8080
end
```

### Visualize
```bash
watch -c -n 0.1 dust -n 50
```
