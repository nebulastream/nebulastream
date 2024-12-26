/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATESERIALIZATIONUTIL_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATESERIALIZATIONUTIL_HPP_

#include "Runtime/BufferManager.hpp"
#include "Runtime/TupleBuffer.hpp"
#include <cstdint>
#include <span>
#include <vector>

namespace NES {
/**
 * @brief The StateSerializationUtil offers functionality to serialize and deserialize uint_64_t variables to and from buffers
 */
class StateSerializationUtil {
  public:
    /**
     * @brief Writes next value to current buffer
     * @param currBuffer buffer to which write should be applied
     * @param dataPtr pointer to the data inside buffer
     * @param dataIdx index of the current value inside buffer
     * @param dataBuffersIdx index of already written buffers
     * @param buffers with the already writen data
     * @param dataToWrite value to write to buffer
     */
    static void writeToBuffer(Runtime::BufferManagerPtr bufferManager,
                              uint64_t const bufferSize,
                              uint64_t*& dataPtr,
                              uint64_t& dataIdx,
                              uint64_t& dataBuffersIdx,
                              std::vector<Runtime::TupleBuffer>& buffers,
                              uint64_t dataToWrite);

    /**
     * @brief Reads next value from current buffer
     * @param dataPtr pointer to the data inside buffer
     * @param dataIdx index of the current value inside buffer
     * @param dataBuffersIdx index of already read buffers
     * @param buffers with the already read data
     * @return read value
     */
    static uint64_t
    readFromBuffer(uint64_t*& dataPtr, uint64_t& dataIdx, uint64_t& dataBuffersIdx, std::vector<Runtime::TupleBuffer>& buffers);
    static uint64_t readFromBuffer(const uint64_t*& dataPtr,
                                   uint64_t& dataIdx,
                                   uint64_t& dataBuffersIdx,
                                   std::span<const Runtime::TupleBuffer>& buffers);

  private:
    /**
     * @brief returns true if there is enought space to write to tupple buffer
     * @param used space
     * @param needed space
     * @param size of tuple buffer
     * @return TupleBuffer
     */
    static bool hasSpaceLeft(uint64_t used, uint64_t needed, uint64_t size);
};
}// namespace NES
#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATESERIALIZATIONUTIL_HPP_
