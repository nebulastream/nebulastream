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
