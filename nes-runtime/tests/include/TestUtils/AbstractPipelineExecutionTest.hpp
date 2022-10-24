

#ifndef NES_NES_RUNTIME_TESTS_INCLUDE_TESTUTILS_ABSTRACTPIPELINEEXECUTIONTEST_HPP_
#define NES_NES_RUNTIME_TESTS_INCLUDE_TESTUTILS_ABSTRACTPIPELINEEXECUTIONTEST_HPP_

#include <Nautilus/Backends/CompilationBackend.hpp>
#include <Nautilus/Backends/Executable.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <vector>

namespace NES::Runtime {
/**
 * @brief This test tests execution of scala expression
 */
class AbstractPipelineExecutionTest : public ::testing::WithParamInterface<std::string> {};

class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    MockedPipelineExecutionContext()
        : PipelineExecutionContext(
            -1,// mock pipeline id
            0, // mock query id
            nullptr,
            [this](TupleBuffer& buffer, Runtime::WorkerContextRef) {
                this->buffers.emplace_back(std::move(buffer));
            },
            [this](TupleBuffer& buffer) {
                this->buffers.emplace_back(std::move(buffer));
            },
            {}){
            // nop
        };

    std::vector<TupleBuffer> buffers;
};
}// namespace NES::Nautilus

#endif//NES_NES_RUNTIME_TESTS_INCLUDE_TESTUTILS_ABSTRACTPIPELINEEXECUTIONTEST_HPP_
