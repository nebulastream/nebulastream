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
#include <Experimental/ExecutionEngine/ExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/InterpretationBasedExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/InterpretationBasedPipelineExecutionEngine.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Experimental/Runtime/RuntimeExecutionContext.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Timer.hpp>
#include <memory>

namespace NES::ExecutionEngine::Experimental {

InterpretationBasedPipelineExecutionEngine::InterpretationBasedPipelineExecutionEngine() : PipelineExecutionEngine() {}

std::shared_ptr<ExecutablePipeline>
InterpretationBasedPipelineExecutionEngine::compile(std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline) {
    auto pipelineContext = std::make_shared<Runtime::Execution::RuntimePipelineContext>();
    return std::make_shared<InterpretationBasedExecutablePipeline>(pipelineContext, physicalOperatorPipeline);
}

}// namespace NES::ExecutionEngine::Experimental