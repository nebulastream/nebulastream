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

#ifndef NES_BENCHMARK_INCLUDE_DATAPROVIDER_TUPLEBUFFERHOLDER_HPP_
#define NES_BENCHMARK_INCLUDE_DATAPROVIDER_TUPLEBUFFERHOLDER_HPP_

#include <Runtime/TupleBuffer.hpp>
#include <memory>

namespace NES::Benchmark::DataProvision {
class TupleBufferHolder {
  public:
    /**
         * @brief default constructor
    */
    TupleBufferHolder() = default;

    /**
         * @brief constructor via an reference to the buffer to be hold
         * @param ref
    */
    TupleBufferHolder(const Runtime::TupleBuffer& ref) : bufferToHold(ref) {}

    /**
     * @brief constructor via && reference to the buffer to be hold
     * @param ref
     */
    TupleBufferHolder(TupleBufferHolder&& rhs) noexcept : bufferToHold(std::move(rhs.bufferToHold)) {}

    /**
     * @brief equal sign operator via a reference
     * @param other
     * @return
     */
    TupleBufferHolder& operator=(const TupleBufferHolder& other) {
        bufferToHold = other.bufferToHold;
        return *this;
    }

    /**
     * @brief equal sign operator via a reference/reference
     * @param other
     * @return
     */
    TupleBufferHolder& operator=(TupleBufferHolder&& other) {
        bufferToHold = std::move(other.bufferToHold);
        return *this;
    }

    Runtime::TupleBuffer bufferToHold;
};
}// namespace NES::Benchmark::DataProvision

#endif // NES_BENCHMARK_INCLUDE_DATAPROVIDER_TUPLEBUFFERHOLDER_HPP_
