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

#ifndef NES_EXECUTION_TESTS_INCLUDE_TESTUTILS_ABSTRACTPIPELINEEXECUTIONTEST_HPP_
#define NES_EXECUTION_TESTS_INCLUDE_TESTUTILS_ABSTRACTPIPELINEEXECUTIONTEST_HPP_

#include <Nautilus/Backends/CompilationBackend.hpp>
#include <Nautilus/Backends/Executable.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <utility>
#include <vector>

namespace NES::Runtime {
/**
 * @brief This is an abstract test class for pipeline execution tests.
 */
class AbstractPipelineExecutionTest : public ::testing::WithParamInterface<std::string> {};
}// namespace NES::Runtime

#endif // NES_EXECUTION_TESTS_INCLUDE_TESTUTILS_ABSTRACTPIPELINEEXECUTIONTEST_HPP_
