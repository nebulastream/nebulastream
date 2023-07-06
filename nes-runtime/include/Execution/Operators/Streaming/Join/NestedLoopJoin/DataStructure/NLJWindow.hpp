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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_DATASTRUCTURE_NLJWINDOW_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_DATASTRUCTURE_NLJWINDOW_HPP_

#include <Execution/Operators/Streaming/Join/StreamWindow.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <ostream>
#include <vector>

namespace NES::Runtime::Execution {

/**
 * @brief This class represents a single window for the nested loop join. It stores all values for the left and right stream.
 * Later on this class can be reused for a slice.
 */
class NLJWindow : public StreamWindow {
  public:
    /**
     * @brief Constructor for creating a window
     * @param windowStart: Start timestamp of this window
     * @param windowEnd: End timestamp of this window
     * @param numWorkerThreads: The number of worker threads that will operate on this window
     * @param leftEntrySize: Size of the tuple on the left
     * @param rightEntrySize: Size of the tuple on the right tuple
     * @param leftPageSize: Size of a single page for the left paged vectors
     * @param rightPageSize: Size of a singe page for the right paged vectors
     */
    explicit NLJWindow(uint64_t windowStart, uint64_t windowEnd, uint64_t numWorkerThreads, uint64_t leftEntrySize,
                       uint64_t leftPageSize, uint64_t rightEntrySize, uint64_t rightPageSize);

    ~NLJWindow() = default;

    /**
     * @brief Retrieves the pointer to paged vector for the left or right side
     * @param workerId: The id of the worker, which request the PagedVectorRef
     * @return Void pointer to the pagedVector
     */
    void* getPagedVectorRefLeft(uint64_t workerId);

    /**
     * @brief Retrieves the pointer to paged vector for the left or right side
     * @param workerId: The id of the worker, which request the PagedVectorRef
     * @return Void pointer to the pagedVector
     */
    void* getPagedVectorRefRight(uint64_t workerId);

    /**
     * @brief combines the PagedVectors for the left and right side. Afterwards, all tuples are stored in the first
     * index of the vectors
     */
    void combinePagedVectors();

    /**
     * @brief Returns the number of tuples in this window for the left side
     * @return uint64_t
     */
    uint64_t getNumberOfTuplesLeft() override;

    /**
     * @brief Returns the number of tuples in this window for the right side
     * @return uint64_t
     */
    uint64_t getNumberOfTuplesRight() override;

    /**
     * @brief Creates a string representation of this window
     * @return String
     */
    std::string toString() override;

  private:
    std::vector<std::unique_ptr<Nautilus::Interface::PagedVector>> leftTuples;
    std::vector<std::unique_ptr<Nautilus::Interface::PagedVector>> rightTuples;
};
}// namespace NES::Runtime::Execution

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_DATASTRUCTURE_NLJWINDOW_HPP_
