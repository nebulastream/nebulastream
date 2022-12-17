#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <vector>
#ifndef NES_NES_RUNTIME_TESTS_INCLUDE_TESTUTILS_MOKEDPIPELINECONTEXT_HPP_
#define NES_NES_RUNTIME_TESTS_INCLUDE_TESTUTILS_MOKEDPIPELINECONTEXT_HPP_
namespace NES::Runtime::Execution::Operators {
class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    explicit MockedPipelineExecutionContext(OperatorHandlerPtr handler)
        : PipelineExecutionContext(
            -1,// mock pipeline id
            0, // mock query id
            nullptr,
            1,
            [this](TupleBuffer& buffer, Runtime::WorkerContextRef) {
                this->buffers.emplace_back(std::move(buffer));
            },
            [this](TupleBuffer& buffer) {
                this->buffers.emplace_back(std::move(buffer));
            },
            {std::move(handler)}){
            // nop
        };
    MockedPipelineExecutionContext()
        : PipelineExecutionContext(
            -1,// mock pipeline id
            0, // mock query id
            nullptr,
            1,
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
}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_RUNTIME_TESTS_INCLUDE_TESTUTILS_MOKEDPIPELINECONTEXT_HPP_
