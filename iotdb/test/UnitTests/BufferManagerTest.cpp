#include <iostream>
#include <map>
#include <vector>
// #include <string>
// #include <utility>

#include <Runtime/BufferManager.hpp>

namespace iotdb {
  void test() {
    std::vector<TupleBufferPtr> buffers;
    std::cout << "get buffer" << std::endl;
    for (int i = 0; i < 15; i ++) {
      buffers.push_back(BufferManager::instance().getBuffer());
    }
    std::cout << "release buffer" << std::endl;
    for (int i = 0; i < 5; i ++) {
      if (buffers[i]) {         // make sure not nullptr
        BufferManager::instance().releaseBuffer(buffers[i]);
      }
    }
    std::cout << "get buffer again" << std::endl;
    for (int i = 0; i < 15; i ++) {
      buffers.push_back(BufferManager::instance().getBuffer());
    }
  }
}

int main() {
  iotdb::BufferManager::instance();
  iotdb::test();
  return 0;
}
