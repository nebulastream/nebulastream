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

#ifndef NES_COORDINATOR_TESTS_INCLUDE_UTIL_NONRUNNABLEDATASOURCE_HPP_
#define NES_COORDINATOR_TESTS_INCLUDE_UTIL_NONRUNNABLEDATASOURCE_HPP_

#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/DefaultSource.hpp>
namespace NES::Testing {

class NonRunnableDataSource : public NES::DefaultSource {
  public:
    explicit NonRunnableDataSource(
        const SchemaPtr& schema,
        const Runtime::BufferManagerPtr& bufferManager,
        const Runtime::QueryManagerPtr& queryManager,
        uint64_t numbersOfBufferToProduce,
        uint64_t gatheringInterval,
        OperatorId operatorId,
        OriginId originId,
        size_t numSourceLocalBuffers,
        const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors,
        NES::Runtime::QueryTerminationType terminationType = NES::Runtime::QueryTerminationType::HardStop);

    void runningRoutine() override;

    bool stop(Runtime::QueryTerminationType termination) override;

    Runtime::MemoryLayouts::DynamicTupleBuffer getBuffer();

    void emitBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buffer);

    void emitBuffer(Runtime::TupleBuffer& buffer);

  private:
    std::promise<bool> canTerminate;
};

DataSourcePtr
createNonRunnableSource(const SchemaPtr& schema,
                        const Runtime::BufferManagerPtr& bufferManager,
                        const Runtime::QueryManagerPtr& queryManager,
                        OperatorId operatorId,
                        OriginId originId,
                        size_t numSourceLocalBuffers,
                        const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors,
                        NES::Runtime::QueryTerminationType terminationType = Runtime::QueryTerminationType::HardStop);
}// namespace NES::Testing

#endif// NES_COORDINATOR_TESTS_INCLUDE_UTIL_NONRUNNABLEDATASOURCE_HPP_
