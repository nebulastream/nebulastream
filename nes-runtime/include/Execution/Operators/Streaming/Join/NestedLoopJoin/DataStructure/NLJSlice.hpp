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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_DATASTRUCTURE_NLJSLICE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_DATASTRUCTURE_NLJSLICE_HPP_

#include <Execution/Operators/Streaming/Join/StreamSlice.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <ostream>
#include <vector>

namespace NES::Runtime::Execution {

/**
 * @brief This class represents a single slice for the nested loop join. It stores all values for the left and right stream.
 * Later on this class can be reused for a slice.
 */
class NLJSlice : public StreamSlice {
  public:
    /**
     * @brief Constructor for creating a slice
     * @param sliceStart: Start timestamp of this slice
     * @param sliceEnd: End timestamp of this slice
     * @param numWorkerThreads: The number of worker threads that will operate on this slice
     * @param leftEntrySize: Size of the tuple on the left
     * @param rightEntrySize: Size of the tuple on the right tuple
     * @param leftPageSize: Size of a single page for the left paged vectors
     * @param rightPageSize: Size of a singe page for the right paged vectors
     */
    explicit NLJSlice(uint64_t sliceStart,
                       uint64_t sliceEnd,
                       uint64_t numWorkerThreads,
                       uint64_t leftEntrySize,
                       uint64_t leftPageSize,
                       uint64_t rightEntrySize,
                       uint64_t rightPageSize);

    ~NLJSlice() = default;

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
     * @brief Returns the number of tuples in this slice for the left side
     * @return uint64_t
     */
    uint64_t getNumberOfTuplesLeft() override;

    /**
     * @brief Returns the number of tuples in this slice for the right side
     * @return uint64_t
     */
    uint64_t getNumberOfTuplesRight() override;

    /**
     * @brief Creates a string representation of this slice
     * @return String
     */
    std::string toString() override;

  private:
    std::vector<std::unique_ptr<Nautilus::Interface::PagedVector>> leftTuples;
    std::vector<std::unique_ptr<Nautilus::Interface::PagedVector>> rightTuples;
};
}// namespace NES::Runtime::Execution

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_DATASTRUCTURE_NLJSLICE_HPP_
