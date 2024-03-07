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

#include <Util/NonRunnableDataSource.hpp>

namespace NES::Testing {

NonRunnableDataSource::NonRunnableDataSource(const SchemaPtr& schema,
                                             const Runtime::BufferManagerPtr& bufferManager,
                                             const Runtime::QueryManagerPtr& queryManager,
                                             uint64_t numbersOfBufferToProduce,
                                             uint64_t gatheringInterval,
                                             OperatorId operatorId,
                                             OriginId originId,
                                             StatisticId statisticId,
                                             size_t numSourceLocalBuffers,
                                             const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors,
                                             NES::Runtime::QueryTerminationType type)
    : DefaultSource(schema,
                    bufferManager,
                    queryManager,
                    numbersOfBufferToProduce,
                    gatheringInterval,
                    operatorId,
                    originId,
                    statisticId,
                    numSourceLocalBuffers,
                    successors) {
    wasGracefullyStopped = type;
}

void NonRunnableDataSource::runningRoutine() {
    open();
    completedPromise.set_value(canTerminate.get_future().get());
    close();
}

bool NonRunnableDataSource::stop(Runtime::QueryTerminationType termination) {
    if (!isRunning()) {
        // the source is already stopped, we don't have to do anything
        return true;
    }
    canTerminate.set_value(true);
    return NES::DefaultSource::stop(termination);
}

Runtime::MemoryLayouts::DynamicTupleBuffer NonRunnableDataSource::getBuffer() { return allocateBuffer(); }

void NonRunnableDataSource::emitBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buffer) {
    auto buf = buffer.getBuffer();
    emitBuffer(buf);
}

void NonRunnableDataSource::emitBuffer(Runtime::TupleBuffer& buffer) { DataSource::emitWorkFromSource(buffer); }

DataSourcePtr createNonRunnableSource(const SchemaPtr& schema,
                                      const Runtime::BufferManagerPtr& bufferManager,
                                      const Runtime::QueryManagerPtr& queryManager,
                                      OperatorId operatorId,
                                      OriginId originId,
                                      StatisticId statisticId,
                                      size_t numSourceLocalBuffers,
                                      const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors,
                                      NES::Runtime::QueryTerminationType terminationType) {
    return std::make_shared<NonRunnableDataSource>(schema,
                                                   bufferManager,
                                                   queryManager,
                                                   /*bufferCnt*/ 1,
                                                   /*frequency*/ 1000,
                                                   operatorId,
                                                   originId,
                                                   statisticId,
                                                   numSourceLocalBuffers,
                                                   successors,
                                                   terminationType);
}

}// namespace NES::Testing