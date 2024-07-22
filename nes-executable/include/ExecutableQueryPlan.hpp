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

#ifndef NES_EXECUTABLE_INCLUDE_EXECUTABLEQUERYPLAN_HPP_
#define NES_EXECUTABLE_INCLUDE_EXECUTABLEQUERYPLAN_HPP_
#include <Runtime/TupleBuffer.hpp>
#include <memory>

#include <Identifiers/Identifiers.hpp>
#include <unordered_map>
#include <vector>

namespace NES {
class SourceDescriptor {
    bool operator==(const SourceDescriptor&) const = default;
};
class SinkDescriptor {
    bool operator==(const SinkDescriptor&) const = default;
};
}// namespace NES
template<>
struct std::hash<NES::SourceDescriptor> {
    std::size_t operator()(const NES::SourceDescriptor&) const noexcept { return 0; }
};

template<>
struct std::hash<NES::SinkDescriptor> {
    std::size_t operator()(const NES::SinkDescriptor&) const noexcept { return 0; }
};

namespace NES::Executable {

/**
 * @brief The PipelineExecutionContext is the base class of the injectable Pipeline Context which is used by the
 * ExecutablePipelineStage to interact with the Query Execution Engine.
 */
class PipelineExecutionContext {
  public:
    virtual ~PipelineExecutionContext() = default;
    virtual Runtime::TupleBuffer allocateBuffer() = 0;
    virtual void emitBuffer() = 0;
    virtual void dispatchBuffer() = 0;
    virtual WorkerThreadId getThreadId() = 0;
};

/**
 * @brief The ExecutablePipelineStage class is the base class of any executable logic in a Query.
 */
class ExecutablePipelineStage {
  public:
    virtual ~ExecutablePipelineStage() = default;
    virtual void setup() = 0;
    virtual void execute(Runtime::TupleBuffer buffer, PipelineExecutionContext& pec) = 0;
    virtual void stop(PipelineExecutionContext& pec) = 0;
};

/**
 * A ExecutablePipeline is a node in the pipeline tree. It may have 0 or more successor pipelines. Successor sinks are not yet
 * created the number of successor might increase after QueryInstantiation. The ExecutablePipeline owns a ExecutablePipelineStage
 * which contains the actual computation.
 */
struct ExecutablePipeline {
    std::unique_ptr<ExecutablePipelineStage> stage;
    std::vector<ExecutablePipeline*> successors;
};

/**
 * @brief A ExecutableQueryPlan represents the pipeline tree that make up an Executable Query Plan. It is the common interface
 * between the QueryCompiler and the NodeEngine. The ExecutableQueryPlan does not contain executable sources or sinks, as at the
 * time of compilation both source and sink descriptors are opaque.
 */
struct ExecutableQueryPlan {
    std::unordered_map<SourceDescriptor, std::vector<ExecutablePipeline*>> sources;
    std::unordered_map<SinkDescriptor, std::vector<ExecutablePipeline*>> sinks;
    std::vector<std::unique_ptr<ExecutablePipeline>> pipelines;
};
}// namespace NES::Executable
#endif// NES_EXECUTABLE_INCLUDE_EXECUTABLEQUERYPLAN_HPP_
