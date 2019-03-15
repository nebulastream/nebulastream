#include <Runtime/BufferManager.hpp>
#include <cassert>
#include <iostream>
#include <thread>
#include <utility>
#include <Util/Logger.hpp>
namespace iotdb {

BufferManager::BufferManager():mutex() {
	IOTDB_DEBUG("BufferManager: Enter Constructor of BufferManager.")
	maxBufferCnt = 1000;//changed from 3
	bufferSizeInByte = 4 * 1024;//set buffer to 4KB
	IOTDB_DEBUG("BufferManager: Set maximun number of buffer to "<<  maxBufferCnt
			<< " and a bufferSize of KB:" << bufferSizeInByte/1024)
	IOTDB_DEBUG("BufferManager: initialize buffers")
	for(size_t i = 0; i < maxBufferCnt; i++)
	{
		addBuffer();
	}
}

BufferManager::~BufferManager() {
	IOTDB_DEBUG("BufferManager: Enter Destructor of BufferManager.")

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
void BufferManager::setNewBufferSize(size_t size)
{
	buffer_pool.clear();
	bufferSizeInByte = size;
	for(size_t i = 0; i < maxBufferCnt; i++)
	{
		addBuffer();
	}
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

		  IOTDB_DEBUG("BufferManager: provide free buffer " <<  entry.first)

		  return entry.first;
	  }
	}
	//add wait
	std::this_thread::sleep_for(std::chrono::seconds(1));
	IOTDB_DEBUG("BufferManager: no buffer free yet --- retry")
  }

}


void BufferManager::releaseBuffer(const TupleBuffer* tuple_buffer) {
	  std::unique_lock<std::mutex> lock(mutex);

	  //find a free buffer
	   for(auto& entry : buffer_pool)
	   {
		  if(entry.first.get() == tuple_buffer)//found entry
		  {
			  entry.second = false;
				IOTDB_DEBUG("BufferManager: found and release buffer")
			  entry.first->num_tuples = 0;
			  entry.first->tuple_size_bytes = 0;

			  return;
		  }
	   }
		IOTDB_ERROR("BufferManager: buffer not found")

	   assert(0);
}



void BufferManager::releaseBuffer(const TupleBufferPtr tuple_buffer) {
	  std::unique_lock<std::mutex> lock(mutex);

	  //find a free buffer
	   for(auto& entry : buffer_pool)
	   {
		  if(entry.first.get() == tuple_buffer.get())//found entry
		  {
			  entry.second = false;
				IOTDB_DEBUG("BufferManager: found and release buffer")
			  entry.first->num_tuples = 0;
			  entry.first->tuple_size_bytes = 0;

			  return;
		  }
	   }
		IOTDB_ERROR("BufferManager: buffer not found")

	   assert(0);
}




} // namespace iotdb
