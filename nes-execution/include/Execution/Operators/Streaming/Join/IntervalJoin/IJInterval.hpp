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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_INTERVALJOIN_IJINTERVAL_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_INTERVALJOIN_IJINTERVAL_HPP_

#include <Execution/Operators/Streaming/Join/StreamSlice.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSized.hpp>
#include <cstdint>
#include <ostream>
#include <vector>

namespace NES::Runtime::Execution {

class IJInterval;
using IJIntervalPtr = std::shared_ptr<IJInterval>;

enum class IJIntervalInfoState : uint8_t {
    LEFT_SIDE_FILLED,
    ONCE_SEEN_DURING_TERMINATION,
    EMITTED_TO_PROBE,
    MARKED_FOR_DELETION
};

/**
 * @brief This class represents a single "slice" (=interval) for the interval join.
 * It always stores one left tuples and an arbitrary amount of right tuples.
 */
class IJInterval {
  public:
    /**
     * @brief Constructor for creating a slice
     * @param intervalStart: Start timestamp of this slice
     * @param intervalEnd: End timestamp of this slice
     * @param numWorkerThreads: The number of worker threads that will operate on this slice
     * @param bufferManager: Allocates pages for both pagedVectorVarSized
     * @param leftSchema: schema of the tuple on the left
     * @param leftPageSize: Size of a single page for the left paged vectors
     * @param rightSchema: schema of the tuple on the right
     * @param rightPageSize: Size of a singe page for the right paged vectors
     */
    IJInterval(int64_t intervalStart,
               int64_t intervalEnd,
               uint64_t numWorkerThreads,
               BufferManagerPtr& bufferManager,
               const SchemaPtr& leftSchema,
               uint64_t leftPageSize,
               const SchemaPtr& rightSchema,
               uint64_t rightPageSize);

    ~IJInterval() = default;

    /**
     * @brief Retrieves the pointer to paged vector for the left side
     * @note as the left side creates the slide, this only contains a single tuple and requires no worker thread id
     * @return Void pointer to the pagedVector
     */
    void* getPagedVectorRefLeft();

    /**
     * @brief Retrieves the pointer to paged vector for the right side
     * @param workerThreadId: The id of the worker, which request the PagedVectorRef
     * @return Void pointer to the pagedVector
     */
    void* getPagedVectorRefRight(WorkerThreadId workerThreadId);

    /**
     * @brief Returns the number of tuples in this interval for the right side
     * @return uint64_t
     */
    uint64_t getNumberOfTuplesRight();

    /**
     * @brief Creates a string representation of this interval
     * @return String
     */
    std::string toString();

    /**
     * @brief returns the id of this interval object
     * @return uint64_t
     */
    [[nodiscard]] uint64_t getId() const;
    int64_t getIntervalStart() const;
    int64_t getIntervalEnd() const;

    static uint64_t nextId;
    IJIntervalInfoState intervalState;

  protected:
    const uint64_t id;
    int64_t intervalStart;
    int64_t intervalEnd;
    uint64_t numWorkerThreads;
    BufferManagerPtr& bufferManager;
    const SchemaPtr& leftSchema;
    uint64_t leftPageSize;
    const SchemaPtr& rightSchema;
    uint64_t rightPageSize;

  private:
    std::unique_ptr<Nautilus::Interface::PagedVectorVarSized> leftTuples;
    std::vector<std::unique_ptr<Nautilus::Interface::PagedVectorVarSized>> rightTuples;
};

}// namespace NES::Runtime::Execution
#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_INTERVALJOIN_IJINTERVAL_HPP_
