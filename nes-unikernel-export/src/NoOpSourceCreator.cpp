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
#include <Phases/ConvertLogicalToPhysicalSource.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Mediums/NullOutputSink.hpp>
#include <Sources/NoOpSource.hpp>
namespace NES {
DataSourcePtr
ConvertLogicalToPhysicalSource::createNoOpSource(const SchemaPtr& schema,
                                                 const Runtime::BufferManagerPtr& bufferManager,
                                                 const Runtime::QueryManagerPtr& queryManager,
                                                 OperatorId operatorId,
                                                 OriginId originId,
                                                 size_t numSourceLocalBuffers,
                                                 const std::string& physicalSourceName,
                                                 const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<NoOpSource>(schema,
                                        bufferManager,
                                        queryManager,
                                        operatorId,
                                        originId,
                                        numSourceLocalBuffers,
                                        physicalSourceName,
                                        successors);
}

DataSinkPtr createNullOutputSink(QueryId queryId,
                                 QuerySubPlanId querySubPlanId,
                                 const Runtime::NodeEnginePtr& nodeEngine,
                                 uint32_t activeProducers,
                                 FaultToleranceType faultToleranceType,
                                 uint64_t numberOfOrigins) {
    return std::make_shared<NullOutputSink>(nodeEngine,
                                            activeProducers,
                                            queryId,
                                            querySubPlanId,
                                            faultToleranceType,
                                            numberOfOrigins);
}
}// namespace NES
