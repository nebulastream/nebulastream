
#include <TestUtils/MockedPipelineExecutionContext.hpp>
namespace NES::Runtime::Execution {

MockedPipelineExecutionContext::MockedPipelineExecutionContext()
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

MockedPipelineExecutionContext::MockedPipelineExecutionContext(OperatorHandlerPtr handler)
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

}// namespace NES::Runtime::Execution