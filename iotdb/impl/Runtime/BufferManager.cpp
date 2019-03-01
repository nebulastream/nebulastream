#include <Runtime/BufferManager.hpp>
#include <cassert>
#include <iostream>
#include <thread>
#include <utility>

namespace iotdb {

BufferManager::BufferManager():mutex() {
	std::cout << "Enter Constructor of BufferManager." << std::endl;
	maxBufferCnt = 3;
	bufferSizeInByte = 4 * 1024;//set buffer to 4KB
	std::cout << "Set maximun number of buffer to "<<  maxBufferCnt
			<< " and a bufferSize of KB:" << bufferSizeInByte/1024 << std::endl;

	std::cout << "initialize buffers" << std::endl;
	for(size_t i = 0; i < maxBufferCnt; i++)
	{
		addBuffer();
	}
}

BufferManager::~BufferManager() {
  std::cout << "Enter Destructor of BufferManager." << std::endl;

  // Release memory.
  for (auto const &buffer_pool_entry : buffer_pool) {
      delete[] buffer_pool_entry.first->buffer;
  }
  buffer_pool.clear();
}

BufferManager &BufferManager::instance() {
  static BufferManager instance;
  return instance;
}

void BufferManager::addBuffer() {
	std::unique_lock<std::mutex> lock(mutex);

	char* buffer = new char[bufferSizeInByte];
	TupleBufferPtr buff = std::make_shared<TupleBuffer>(buffer, bufferSizeInByte, 0, 0);
	buffer_pool.insert(std::make_pair(buff, false));
}

TupleBufferPtr BufferManager::getBuffer() {
//  std::unique_lock<std::mutex> lock(mutex);
  bool found = false;

  while(!found)
  {
	  //find a free buffer
	for(auto& entry : buffer_pool)
	{
	  if(entry.second == false)//found free entry
	  {
		  entry.second = true;
		  std::cout << "BufferManager: Got free buffer " <<  entry.first << std::endl;
		  return entry.first;
	  }
	}
	//add wait
	std::this_thread::sleep_for(std::chrono::seconds(1));
	std::cout << "BufferManager: no buffer free yet --- retry" << std::endl;
  }

}

void BufferManager::releaseBuffer(TupleBufferPtr tuple_buffer) {
	  std::unique_lock<std::mutex> lock(mutex);

	  //find a free buffer
	   for(auto& entry : buffer_pool)
	   {
		  if(entry.first.get() == tuple_buffer.get())//found free entry
		  {
			  entry.second = false;
			  std::cout << "BufferManager: found and release buffer" << std::endl;
			  return;
		  }
	   }

	   std::cout << "BufferManager: buffer not found" << std::endl;
	   assert(0);
}




} // namespace iotdb
