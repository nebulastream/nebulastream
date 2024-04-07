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
#include "Sinks/Mediums/NullOutputSink.hpp"
#include <NoOp/NoOpSource.hpp>
#include <QueryCompiler/Phases/Translations/ConvertLogicalToPhysicalSource.hpp>
#include <Runtime/TupleBuffer.hpp>
namespace NES {
DataSourcePtr
ConvertLogicalToPhysicalSource::createNoOpSource(const SchemaPtr& schema,
                                                 const Runtime::BufferManagerPtr& bufferManager,
                                                 const Runtime::QueryManagerPtr& queryManager,
                                                 OperatorId operatorId,
                                                 OriginId originId,
                                                 StatisticId statisticId,
                                                 size_t numSourceLocalBuffers,
                                                 const std::string& physicalSourceName,
                                                 GatheringMode gatheringMode,
                                                 const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<NoOpSource>(schema,
                                        bufferManager,
                                        queryManager,
                                        operatorId,
                                        originId,
                                        statisticId,
                                        numSourceLocalBuffers,
                                        gatheringMode,
                                        physicalSourceName);
}

DataSinkPtr createNullOutputSink(SharedQueryId queryId,
                                 DecomposedQueryPlanId querySubPlanId,
                                 const Runtime::NodeEnginePtr& nodeEngine,
                                 uint32_t activeProducers,
                                 uint64_t numberOfOrigins) {
    return std::make_shared<NullOutputSink>(nodeEngine, activeProducers, queryId, querySubPlanId, numberOfOrigins);
}
}// namespace NES
