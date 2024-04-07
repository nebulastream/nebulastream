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

#ifndef NES_RUNTIME_INCLUDE_SOURCES_DATASOURCEPLUGIN_HPP_
#define NES_RUNTIME_INCLUDE_SOURCES_DATASOURCEPLUGIN_HPP_
#include <Util/PluginRegistry.hpp>
#include <optional>
namespace NES {

class DataSource;
using DataSourcePtr = std::shared_ptr<DataSource>;

/**
 * @brief DataSource interface to register a new physical data source plugin.
 * Implementations of this plugin must implement createDatasource that translates a source descriptor to a DataSourcePtr.
 */
class DataSourcePlugin {
  public:
    DataSourcePlugin() = default;

    /**
     * @brief Translates a source descriptor to a data source.
     * @param sourceId id of the source
     * @param originId id of the origin for which data is produced.
     * @param sourceDescriptor the source descriptor.
     * @param nodeEngine reference to the node engine
     * @param numSourceLocalBuffers number of source buffers that can be allocated.
     * @param successors the successor pipeline, which is used to emit data.
     * @return std::optional<DataSourcePtr>
     */
    virtual std::optional<DataSourcePtr>
    createDataSource(OperatorId sourceId,
                     OriginId originId,
                     StatisticId statisticId,
                     const SourceDescriptorPtr& sourceDescriptor,
                     const Runtime::NodeEnginePtr& nodeEngine,
                     size_t numSourceLocalBuffers,
                     const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) = 0;

    virtual ~DataSourcePlugin() = default;
};

using SourcePluginRegistry = Util::PluginRegistry<DataSourcePlugin>;
}// namespace NES

#endif// NES_RUNTIME_INCLUDE_SOURCES_DATASOURCEPLUGIN_HPP_
