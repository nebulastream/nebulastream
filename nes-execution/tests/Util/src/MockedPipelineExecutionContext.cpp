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

#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <utility>
namespace NES::Runtime::Execution {

MockedPipelineExecutionContext::MockedPipelineExecutionContext()
    : PipelineExecutionContext(
        -1,// mock pipeline id
        0, // mock query id
        nullptr,
        1,
        [this](TupleBuffer& buffer, Runtime::WorkerContextRef) {
            if (this->seenSeqNumbers.contains(buffer.getSequenceNumber())) {
                NES_ERROR("Already seen sequenceNumber {}", buffer.getSequenceNumber());
            }
            this->seenSeqNumbers.insert(buffer.getSequenceNumber());
            this->buffers.emplace_back(std::move(buffer));
        },
        [this](TupleBuffer& buffer) {
              if (this->seenSeqNumbers.contains(buffer.getSequenceNumber())) {
                  NES_ERROR("Already seen sequenceNumber {}", buffer.getSequenceNumber());
              }
              this->seenSeqNumbers.insert(buffer.getSequenceNumber());
              this->buffers.emplace_back(std::move(buffer));
        },
        {}){
        // nop
    };


MockedPipelineExecutionContext::MockedPipelineExecutionContext(std::vector<OperatorHandlerPtr> handler)
    : PipelineExecutionContext(
        -1,// mock pipeline id
        0, // mock query id
        nullptr,
        1,
        [this](TupleBuffer& buffer, Runtime::WorkerContextRef) {
              if (this->seenSeqNumbers.contains(buffer.getSequenceNumber())) {
                  NES_FATAL_ERROR("Already seen sequenceNumber {}", buffer.getSequenceNumber());
              }
              this->seenSeqNumbers.insert(buffer.getSequenceNumber());
              this->buffers.emplace_back(std::move(buffer));
        },
        [this](TupleBuffer& buffer) {
              if (this->seenSeqNumbers.contains(buffer.getSequenceNumber())) {
                  NES_FATAL_ERROR("Already seen sequenceNumber {}", buffer.getSequenceNumber());
              }
              this->seenSeqNumbers.insert(buffer.getSequenceNumber());
              this->buffers.emplace_back(std::move(buffer));
        },
        std::move(handler)){
        // nop
    };

}// namespace NES::Runtime::Execution