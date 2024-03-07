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

#include <API/Schema.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Sources/Arrow/ArrowSource.hpp>
#include <Sources/Arrow/ArrowSourceDescriptor.hpp>
#include <Sources/DataSourcePlugin.hpp>

namespace NES {

class ArrowSourcePlugin : public DataSourcePlugin {
  public:
    std::optional<DataSourcePtr>
    createDataSource(OperatorId operatorId,
                     OriginId originId,
                     StatisticId statisticId,
                     const SourceDescriptorPtr& sourceDescriptor,
                     const Runtime::NodeEnginePtr& nodeEngine,
                     size_t numSourceLocalBuffers,
                     const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) override {
        if (sourceDescriptor->instanceOf<ArrowSourceDescriptor>()) {
            auto bufferManager = nodeEngine->getBufferManager(0u);
            auto queryManager = nodeEngine->getQueryManager();

            NES_INFO("ConvertLogicalToPhysicalSource: Creating Arrow file source");
            const ArrowSourceDescriptorPtr arrowSourceDescriptor = sourceDescriptor->as<ArrowSourceDescriptor>();
            return std::make_shared<ArrowSource>(arrowSourceDescriptor->getSchema(),
                                                 bufferManager,
                                                 queryManager,
                                                 arrowSourceDescriptor->getSourceConfig(),
                                                 operatorId,
                                                 originId,
                                                 statisticId,
                                                 numSourceLocalBuffers,
                                                 GatheringMode::INTERVAL_MODE,
                                                 sourceDescriptor->getPhysicalSourceName(),
                                                 successors);
        }
        return {};
    };
};

// Register source plugin
[[maybe_unused]] static SourcePluginRegistry ::Add<ArrowSourcePlugin> arrowSourcePlugin;
}// namespace NES