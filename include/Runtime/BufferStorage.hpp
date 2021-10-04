//
// Created by Anastasiia Kozar on 01.10.21.
//

#ifndef NES_BUFFERSTORAGE_H
#define NES_BUFFERSTORAGE_H

#include <Runtime/TupleBuffer.hpp>
#include <queue>
namespace NES {

/**
 * @brief The Buffer Storage class stores tuples inside a queue and trims it when the right acknowledgement is received
 */
class BufferStorage {
  private:
    std::queue<std::pair<uint64_t, Runtime::TupleBuffer>> buffer;

  public:
    /**
     * @brief Inserts a pair id, buffer link to the buffer storage queue
     * @param id id of the buffer
     * @param bufferPtr pointer to the buffer that will be stored
     */
    void insertBuffer(uint64_t id, NES::Runtime::TupleBuffer bufferPtr);
    /**
     * @brief Deletes a pair id, buffer link from the buffer storage queue with a given id
     * @param id id of the buffer
     */
    bool trimBuffer(uint64_t id);
    /**
     * @brief Return current queue size
     * @return Current queue size
     */
    size_t getStorageSize();
};

}// namespace NES

#endif//NES_BUFFERSTORAGE_H


