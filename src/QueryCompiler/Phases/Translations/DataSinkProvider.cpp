/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Phases/ConvertLogicalToPhysicalSink.hpp>
#include <QueryCompiler/Phases/Translations/DataSinkProvider.hpp>
#include <Runtime/NodeEngine.hpp>
#include <utility>

namespace NES::QueryCompilation {

DataSinkProviderPtr DataSinkProvider::create() { return std::make_shared<DataSinkProvider>(); }

DataSinkPtr DataSinkProvider::lower(OperatorId sinkId,
                                    OperatorId logicalSinkOperatorId,
                                    SinkDescriptorPtr sinkDescriptor,
                                    SchemaPtr schema,
                                    Runtime::NodeEnginePtr nodeEngine,
                                    QuerySubPlanId querySubPlanId) {
    std::map<OperatorId, DataSinkPtr>& operatorCache = nodeEngine->getSinkOperatorCache();
    if (operatorCache.find(logicalSinkOperatorId) != operatorCache.end()) {
        return operatorCache[logicalSinkOperatorId];
    }
    const DataSinkPtr& sinkPtr = ConvertLogicalToPhysicalSink::createDataSink(sinkId,
                                                                              logicalSinkOperatorId,
                                                                              std::move(sinkDescriptor),
                                                                              std::move(schema),
                                                                              std::move(nodeEngine),
                                                                              querySubPlanId);
    if (!std::dynamic_pointer_cast<Network::NetworkSink>(sinkPtr)) {
        operatorCache[logicalSinkOperatorId] = sinkPtr;
    }
    return sinkPtr;
}

}// namespace NES::QueryCompilation