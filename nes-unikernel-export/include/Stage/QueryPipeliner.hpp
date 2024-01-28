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

#ifndef QUERYOPTIMIZER_HPP
#define QUERYOPTIMIZER_HPP
#include <OperatorHandlerTracer.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <map>
#include <vector>

namespace NES::Unikernel::Export {
using LocalOperatorHandlerIndex = size_t;
using SharedOperatorHandlerIndex = size_t;

struct IntermediateStage {
    std::vector<Runtime::Unikernel::GlobalOperatorHandlerIndex> operatorHandlers;
    QueryCompilation::OperatorPipelinePtr pipeline;
    std::optional<PipelineId> successors;
    std::vector<PipelineId> predecessor;
};

struct Stage {
    std::vector<std::pair<LocalOperatorHandlerIndex, Runtime::Unikernel::OperatorHandlerDescriptor>> handler;
    std::vector<std::pair<LocalOperatorHandlerIndex, SharedOperatorHandlerIndex>> sharedHandlerIndices;
    QueryCompilation::OperatorPipelinePtr pipeline;
    std::optional<PipelineId> successor;
    std::vector<PipelineId> predecessors;

    static Stage
    from(IntermediateStage&& im,
         const std::unordered_map<Runtime::Unikernel::GlobalOperatorHandlerIndex, Runtime::Execution::OperatorHandlerPtr>&
             handlerById,
         const std::unordered_map<Runtime::Unikernel::GlobalOperatorHandlerIndex, size_t>& uniqueOperatorHandlers,
         const std::unordered_map<Runtime::Unikernel::GlobalOperatorHandlerIndex, SharedOperatorHandlerIndex>& sharedByGlobal);
};

class QueryPipeliner {
  public:
    explicit QueryPipeliner(size_t bufferSize) : bufferSize(bufferSize) {}

    struct Result {
        std::unordered_map<SharedOperatorHandlerIndex, Runtime::Unikernel::OperatorHandlerDescriptor> sharedOperatorHandler;
        std::vector<Stage> stages;
    };

  private:
    size_t bufferSize;
    QueryCompilation::QueryCompilerOptionsPtr options = createOptions();

    // keep track weather a operator handler is shared between stages or not
    std::unordered_map<Runtime::Unikernel::GlobalOperatorHandlerIndex, size_t> handlerIndexCounter = {};
    std::unordered_map<Runtime::Unikernel::GlobalOperatorHandlerIndex, Runtime::Execution::OperatorHandlerPtr>
        handlerByHandlerIndex = {};

    static QueryCompilation::QueryCompilerOptionsPtr createOptions();

    IntermediateStage lowerToNautilus(const QueryCompilation::OperatorPipelinePtr& pipeline);

    std::pair<std::unordered_map<SharedOperatorHandlerIndex, Runtime::Unikernel::OperatorHandlerDescriptor>,
              std::unordered_map<Runtime::Unikernel::GlobalOperatorHandlerIndex, SharedOperatorHandlerIndex>>
    sharedHandlers();

  public:
    Result lowerQuery(const QueryPlanPtr& unikernelWorkerQueryPlan);
};

}// namespace NES::Unikernel::Export
#endif//QUERYOPTIMIZER_HPP
