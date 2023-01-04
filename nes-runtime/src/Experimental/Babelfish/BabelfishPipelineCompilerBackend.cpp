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
#include "Util/Timer.hpp"
#include <Experimental/Babelfish/BabelfishExecutablePipeline.hpp>
#include <Experimental/Babelfish/BabelfishPipelineCompilerBackend.hpp>
#include <Experimental/Babelfish/IRSerialization.hpp>
#include <babelfish.h>

namespace NES::Runtime::ProxyFunctions {
void* NES__Runtime__TupleBuffer__getBuffer(void* thisPtr);
uint64_t NES__Runtime__TupleBuffer__getNumberOfTuples(void* thisPtr);
}// namespace NES::Runtime::ProxyFunctions
extern "C" long TupleBuffer_getNumberOfTuples(void* ptr) {
    return NES::Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getNumberOfTuples(ptr);
}

extern "C" void* TupleBuffer_getBuffer(void* ptr) {
    return NES::Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getBuffer(ptr);
}

namespace NES::ExecutionEngine::Experimental {

std::shared_ptr<ExecutablePipeline>
BabelfishPipelineCompilerBackend::compile(std::shared_ptr<Runtime::Execution::RuntimePipelineContext> executionContext,
                                          std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline,
                                          std::shared_ptr<IR::NESIR> ir) {
    Timer timer("CompilationBasedPipelineExecutionEngine");
    timer.start();

    auto serializedIr = IRSerialization().serialize(ir);
    NES_DEBUG(serializedIr);
    graal_isolatethread_t* isolate = NULL;
    NES_ASSERT(graal_create_isolate(NULL, NULL, &isolate) == 0, "Babelfish isolate could not be initialized");

    auto pipelineContext = initializePipeline(isolate, serializedIr.data());
    auto exec =
        std::make_shared<BabelfishExecutablePipeline>(executionContext, physicalOperatorPipeline, isolate, pipelineContext);
    timer.snapshot("Babelfish generation");
    timer.pause();
    NES_INFO("BabelfishPipelineCompilerBackend TIME: " << timer);
    return exec;
}
}// namespace NES::ExecutionEngine::Experimental