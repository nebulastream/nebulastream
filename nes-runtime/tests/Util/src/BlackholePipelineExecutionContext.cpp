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

#include <TestUtils/BlackholePipelineExecutionContext.hpp>
namespace NES::Runtime::Execution {

BlackholePipelineExecutionContext::BlackholePipelineExecutionContext()
    : PipelineExecutionContext(
        -1,// mock pipeline id
        0, // mock query id
        nullptr,
        1,
        [](TupleBuffer&, Runtime::WorkerContextRef) {
        },
        [](TupleBuffer&) {
        },
        {}){
        // nop
    };

BlackholePipelineExecutionContext::BlackholePipelineExecutionContext(std::vector<OperatorHandlerPtr> handler)
    : PipelineExecutionContext(
        -1,// mock pipeline id
        0, // mock query id
        nullptr,
        1,
        [](TupleBuffer&, Runtime::WorkerContextRef) {
        },
        [](TupleBuffer&) {
        },
        handler){
        // nop
    };

}// namespace NES::Runtime::Execution