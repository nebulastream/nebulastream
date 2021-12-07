/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_RUNTIME_TASK_HPP_
#define NES_INCLUDE_RUNTIME_TASK_HPP_

#include <Runtime/ExecutionResult.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <limits>
#include <memory>
namespace NES::Runtime {

/**
 * @brief Task abstraction to bind processing (compiled binary) and data (incoming buffers
 * @Limitations:
 *    -
 */
class alignas(64) Task {
  public:
    /**
     * @brief Task constructor
     * @param pointer to query execution plan that should be applied on the incoming buffer
     * @param id of the pipeline stage inside the QEP that should be applied
     * @param pointer to the tuple buffer that has to be process
     */
    explicit Task(Execution::SuccessorExecutablePipeline pipeline, TupleBuffer buf, uint64_t taskId);

    constexpr explicit Task() noexcept = default;

    /**
     * @brief execute the task by calling executeStage of QEP and providing the stageId and the buffer
     */
    ExecutionResult operator()(WorkerContext& workerContext);

    /**
     * @brief return the number of tuples in the working buffer
     * @return number of input tuples in working buffer
     */
    uint64_t getNumberOfTuples();

    /**
    * @brief return the number of tuples in the input buffer (for statistics)
    * @return number of input tuples in input buffer
    */
    uint64_t getNumberOfInputTuples();

    /**
     * @brief method to return the qep of a task
     * @return
     */
    Execution::SuccessorExecutablePipeline getExecutable();

    /**
   * @brief method to check if it is a watermark-only buffer
   * @retun bool indicating if this buffer is for watermarks only
   */
    bool isReconfiguration();

    /**
     * @return true if this Task is valid and it is safe to execute
     */
    explicit operator bool() const;

    /**
     * @return true if this Task is valid and it is safe to execute
     */
    bool operator!() const;

    std::string toString();

    uint64_t getId() const;

    /**
     * This method returns the reference to the buffer of this task
     * @return
     */
    TupleBuffer& getBufferRef();

  private:
    Execution::SuccessorExecutablePipeline pipeline{};
    TupleBuffer buf{};
    uint64_t id{std::numeric_limits<decltype(id)>::max()};
    uint64_t inputTupleCount = 0;
};
static_assert(sizeof(Task) == 64);
}// namespace NES::Runtime

#endif// NES_INCLUDE_RUNTIME_TASK_HPP_
