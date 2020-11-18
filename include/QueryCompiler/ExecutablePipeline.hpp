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

#ifndef NES_INCLUDE_QUERYCOMPILER_EXECUTABLEPIPELINE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_EXECUTABLEPIPELINE_HPP_

#include <memory>
#include <string>

namespace NES::Windowing {

class WindowManager;
typedef std::shared_ptr<WindowManager> WindowManagerPtr;

}// namespace NES::Windowing

namespace NES {

class TupleBuffer;

class ExecutablePipeline;
typedef std::shared_ptr<ExecutablePipeline> ExecutablePipelinePtr;

class BufferManager;
typedef std::shared_ptr<BufferManager> BufferManagerPtr;

class PipelineExecutionContext;
typedef std::shared_ptr<PipelineExecutionContext> QueryExecutionContextPtr;

class WorkerContext;
typedef WorkerContext& WorkerContextRef;

// TODO move state and windowManager inside the PipelineExecutionContext
class ExecutablePipeline {

  protected:
    bool reconfiguration;

  public:
    explicit ExecutablePipeline(bool reconfiguration = false) : reconfiguration(reconfiguration) {}

    virtual ~ExecutablePipeline() = default;

    /**
     * @brief Executes the pipeline given the input
     * @return error code: 0 for valid execution, 1 for error
     */
    // TODO use dedicate type for return
    virtual uint32_t execute(TupleBuffer& input_buffers, QueryExecutionContextPtr context, WorkerContextRef wctx) = 0;

    /**
     * @return returns true if the pipeline contains a function pointer for a reconfiguration task
     */
    bool isReconfiguration() const { return reconfiguration; }
};

}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_EXECUTABLEPIPELINE_HPP_
