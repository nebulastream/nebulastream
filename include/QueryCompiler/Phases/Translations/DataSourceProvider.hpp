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
#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DATASOURCEPROVIDER_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DATASOURCEPROVIDER_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <Operators/OperatorId.hpp>
namespace NES {
namespace QueryCompilation {
class DataSourceProvider {
  public:
    static DataSourceProviderPtr create();
    virtual DataSourcePtr lower(OperatorId operatorId, SourceDescriptorPtr sourceDescriptor,
                        NodeEngine::NodeEnginePtr nodeEngine,
                        size_t numSourceLocalBuffers,
                        std::vector<NodeEngine::Execution::SuccessorExecutablePipeline> successors);
};
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DATASOURCEPROVIDER_HPP_
