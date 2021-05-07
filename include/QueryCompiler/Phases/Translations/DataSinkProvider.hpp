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

#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DATASINKPROVIDER_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DATASINKPROVIDER_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <Operators/OperatorId.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
namespace NES {
namespace QueryCompilation {
class DataSinkProvider {
  public:
    static DataSinkProviderPtr create();
    /**
     * @brief Lower a
     * @param operatorId
     * @param sinkDescriptor
     * @param schema
     * @param nodeEngine
     * @param querySubPlanId
     * @return
     */
    virtual DataSinkPtr lower(OperatorId operatorId, SinkDescriptorPtr sinkDescriptor,
                      SchemaPtr schema, NodeEngine::NodeEnginePtr nodeEngine,
                      QuerySubPlanId querySubPlanId);
};
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DATASINKPROVIDER_HPP_
