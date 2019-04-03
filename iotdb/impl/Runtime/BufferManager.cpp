#include <Runtime/BufferManager.hpp>
#include <cassert>
#include <iostream>
#include <thread>
#include <utility>
#include <Util/Logger.hpp>

namespace iotdb {

    BufferManager::BufferManager() : mutex(), noFreeBuffer(0), providedBuffer(0), releasedBuffer(0) {
        IOTDB_DEBUG("BufferManager: Enter Constructor of BufferManager.")
        maxBufferCnt = 10000;//changed from 3
        bufferSizeInByte = 4 * 1024;//set buffer to 4KB
        IOTDB_DEBUG("BufferManager: Set maximun number of buffer to " << maxBufferCnt
                                                                      << " and a bufferSize of KB:"
                                                                      << bufferSizeInByte / 1024)
        IOTDB_DEBUG("BufferManager: initialize buffers")
        for (size_t i = 0; i < maxBufferCnt; i++) {
            addBuffer();
        }
    }

    BufferManager::~BufferManager() {
        IOTDB_DEBUG("BufferManager: Enter Destructor of BufferManager.")

        // Release memory.
        for (auto const &buffer_pool_entry : buffer_pool) {
            delete[] (char *) buffer_pool_entry.first->buffer;
        }
        buffer_pool.clear();
    }

    BufferManager &BufferManager::instance() {
        static BufferManager instance;
        return instance;
    }


    void BufferManager::setNumberOfBuffers(size_t size) {
        for (auto &entry : buffer_pool) {
            delete[] (char *) entry.first->buffer;
        }
        buffer_pool.clear();
        maxBufferCnt = size;
        for (size_t i = 0; i < maxBufferCnt; i++) {
            addBuffer();
        }
    }

    void BufferManager::setBufferSize(size_t size) {
        for (auto &entry : buffer_pool) {
            delete[] (char *) entry.first->buffer;
        }
        buffer_pool.clear();
        bufferSizeInByte = size;
        for (size_t i = 0; i < maxBufferCnt; i++) {
            addBuffer();
        }
    }

    void BufferManager::removeBuffer(TupleBufferPtr tuple_buffer) {
        std::unique_lock<std::mutex> lock(mutex);

        for (auto &entry : buffer_pool) {
            if (entry.first.get() == tuple_buffer.get()) {
                delete (char *) entry.first->buffer;
                buffer_pool.erase(tuple_buffer);
                IOTDB_DEBUG("BufferManager: found and remove Buffer buffer")
                return;
            }
        }
        IOTDB_DEBUG("BufferManager: could not remove buffer, buffer not found")
        return;
    }

    void BufferManager::addBuffer() {
        std::unique_lock<std::mutex> lock(mutex);

        char *buffer = new char[bufferSizeInByte];
        TupleBufferPtr buff = std::make_shared<TupleBuffer>(buffer, bufferSizeInByte, 0, 0);
        buffer_pool.emplace(std::piecewise_construct,
                            std::forward_as_tuple(buff), std::forward_as_tuple(false));

    }

    TupleBufferPtr BufferManager::getBuffer() {
        IOTDB_DEBUG("BufferManager: getBuffer()")


        while (true) {
            //find a free buffer
            for (auto &entry : buffer_pool) {
                bool used = false;
                if (entry.second.compare_exchange_weak(used, true)) {
                    providedBuffer++;
                    return entry.first;
                }
            }
            //add wait
            noFreeBuffer++;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            IOTDB_DEBUG("BufferManager: no buffer free yet --- retry")
        }

    }

    size_t BufferManager::getNumberOfBuffers() {
        return buffer_pool.size();
    }

    size_t BufferManager::getNumberOfFreeBuffers() {
        size_t result = 0;
        for (auto &entry : buffer_pool) {
            if (!entry.second)
                result++;
        }
        return result;
    }


    void BufferManager::releaseBuffer(const TupleBuffer *tuple_buffer) {
        IOTDB_DEBUG("BufferManager: releaseBuffer(TupleBufferPtr)")

        std::unique_lock<std::mutex> lock(mutex);

        //find a free buffer
        for (auto &entry : buffer_pool) {
            if (entry.first.get() == tuple_buffer)//found entry
            {
                entry.second = false;
                IOTDB_DEBUG("BufferManager: found and release buffer")
                entry.first->num_tuples = 0;
                entry.first->tuple_size_bytes = 0;
                releasedBuffer++;
                return;
            }
        }
        IOTDB_ERROR("BufferManager: buffer not found")

        assert(0);
    }


    void BufferManager::releaseBuffer(const TupleBufferPtr tuple_buffer) {
        IOTDB_DEBUG("BufferManager: releaseBuffer(TupleBufferPtr)")

        std::unique_lock<std::mutex> lock(mutex);

        //find a free buffer
        for (auto &entry : buffer_pool) {
            if (entry.first.get() == tuple_buffer.get())//found entry
            {
                entry.second = false;
                IOTDB_DEBUG("BufferManager: found and release buffer")
                entry.first->num_tuples = 0;
                entry.first->tuple_size_bytes = 0;
                releasedBuffer++;

                return;
            }
        }
        IOTDB_ERROR("BufferManager: buffer not found")

        assert(0);
    }

    void BufferManager::printStatistics() {
        IOTDB_INFO("BufferManager Statistics:")
        IOTDB_INFO("\t noFreeBuffer=" << noFreeBuffer)
        IOTDB_INFO("\t providedBuffer=" << providedBuffer)
        IOTDB_INFO("\t releasedBuffer=" << releasedBuffer)
    }


} // namespace iotdb
