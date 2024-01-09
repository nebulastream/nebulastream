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

#ifndef NES_NES_RUNTIME_TESTS_INCLUDE_TESTUTILS_ABSTRACTCOMPILATIONBACKENDTEST_HPP_
#define NES_NES_RUNTIME_TESTS_INCLUDE_TESTUTILS_ABSTRACTCOMPILATIONBACKENDTEST_HPP_

#include <Nautilus/Backends/CompilationBackend.hpp>
#include <Nautilus/Backends/Executable.hpp>
#include <Nautilus/IR/Phases/LoopDetectionPhase.hpp>
#include <Nautilus/IR/Phases/RemoveBrOnlyBlocksPhase.hpp>
#include <Nautilus/IR/Phases/StructuredControlFlowPhase.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

namespace NES::Nautilus {
/**
 * @brief This is an abstract test class for compilation backend tests
 */
class AbstractCompilationBackendTest : public ::testing::WithParamInterface<std::string> {
  public:
    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    Nautilus::Tracing::TraceToIRConversionPhase irCreationPhase;
    Nautilus::IR::RemoveBrOnlyBlocksPhase removeBrOnlyBlocksPhase;
    Nautilus::IR::LoopDetectionPhase loopDetectionPhase;
    Nautilus::IR::StructuredControlFlowPhase structuredControlFlowPhase;
    std::unique_ptr<Nautilus::Backends::Executable>
    prepare(std::shared_ptr<Nautilus::Tracing::ExecutionTrace> executionTrace,
            const CompilationOptions& options = CompilationOptions(),
            const DumpHelper& dumpHelper = DumpHelper::create("", true, false, ""));
};
}// namespace NES::Nautilus

#endif//NES_NES_RUNTIME_TESTS_INCLUDE_TESTUTILS_ABSTRACTCOMPILATIONBACKENDTEST_HPP_
