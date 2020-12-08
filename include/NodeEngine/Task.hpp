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

#ifndef INCLUDE_TASK_H_
#define INCLUDE_TASK_H_
#include <NodeEngine/TupleBuffer.hpp>
#include <memory>
namespace NES {

class WorkerContext;
class ExecutablePipeline;
typedef std::shared_ptr<ExecutablePipeline> PipelineStagePtr;

/**
 * @brief Task abstraction to bind processing (compiled binary) and data (incoming buffers
 * @Limitations:
 *    -
 */
class Task {
  public:
    /**
     * @brief Task constructor
     * @param pointer to query execution plan that should be applied on the incoming buffer
     * @param id of the pipeline stage inside the QEP that should be applied
     * @param pointer to the tuple buffer that has to be process
     */
    explicit Task(PipelineStagePtr pipeline, TupleBuffer& buf);

    explicit Task();

    ~Task() = default;

    /**
     * @brief execute the task by calling executeStage of QEP and providing the stageId and the buffer
     */
    bool operator()(WorkerContext& workerContext);

    /**
     * @brief return the number of tuples in the input buffer (for statistics)
     * @return number of input tuples in buffer
     */
    uint64_t getNumberOfTuples();

    /**
     * @brief method to return the qep of a task
     * @return
     */
    PipelineStagePtr getPipelineStage();

    /**
     * @brief method to check if it is a watermark-only buffer
     * @retun bool indicating if this buffer is for watermarks only
     */
    bool isWatermarkOnly();

    /**
     * @return true if this Task is valid and it is safe to execute
     */
    explicit operator bool() const;

    /**
     * @return true if this Task is valid and it is safe to execute
     */
    bool operator!() const;

    std::string toString();

    uint64_t getId();

  private:
    PipelineStagePtr pipeline;
    TupleBuffer buf;
    uint64_t id;
};

}// namespace NES

#endif /* INCLUDE_TASK_H_ */
