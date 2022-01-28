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

#ifndef NES_INCLUDE_RUNTIME_MEMORYLAYOUT_MEMORYLAYOUTTUPLEBUFFER_HPP_
#define NES_INCLUDE_RUNTIME_MEMORYLAYOUT_MEMORYLAYOUTTUPLEBUFFER_HPP_

#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <cstring>

namespace NES::Runtime::MemoryLayouts {

using FIELD_SIZE = uint64_t;

/**
 * @brief This abstract class is the base class for DynamicRowLayoutBuffer and DynamicColumnLayoutBuffer.
 * As the base class, it has multiple methods or members that are useful for both derived classes.
 * @caution This class is non-thread safe
 */
class MemoryLayoutTupleBuffer {

  public:
    /**
     * @brief Constructor for DynamicLayoutBuffer
     * @param tupleBuffer
     * @param capacity
     */
    MemoryLayoutTupleBuffer(TupleBuffer tupleBuffer, uint64_t capacity);

    virtual ~MemoryLayoutTupleBuffer() = default;

    /**
    * @brief This method returns the maximum number of records, so the capacity.
    * @return
    */
    uint64_t getCapacity() const;

    /**
     * @brief This method returns the current number of records that are in the associated buffer
     * @return
     */
    uint64_t getNumberOfRecords() const;

    /**
     * @brief This methods returns a reference to the associated buffer
     * @return
     */
    TupleBuffer getTupleBuffer();

  protected:
    TupleBuffer tupleBuffer;
    uint64_t capacity;
    uint64_t numberOfRecords = 0;
};
}// namespace NES::Runtime::MemoryLayouts

#endif  // NES_INCLUDE_RUNTIME_MEMORYLAYOUT_MEMORYLAYOUTTUPLEBUFFER_HPP_
