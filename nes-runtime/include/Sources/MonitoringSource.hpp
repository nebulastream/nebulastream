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

#ifndef NES_RUNTIME_INCLUDE_SOURCES_MONITORINGSOURCE_HPP_
#define NES_RUNTIME_INCLUDE_SOURCES_MONITORINGSOURCE_HPP_

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Sources/DataSource.hpp>
#include <Util/MetricCollectorType.hpp>
#include <chrono>

namespace NES {

/**
 * @brief this class provides a monitoring source to generate metrics based on a given metric collector.
 */
class MonitoringSource : public DataSource {
  public:
    /**
     * @brief Constructor for the monitoring source
     * @param metricCollector the metric collector
     * @param waitTime wait time during fill buffer until a new metric is read
     * @param bufferManager pointer to the buffer manager
     * @param queryManager pointer to the query manager
     * @param operatorId current operator id
     * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
     * @param statisticId represents the unique identifier of components that we can track statistics for
     * @param numSourceLocalBuffers the number of buffers allocated to a source
     * @param physicalSourceName the name and unique identifier of a physical source
     * @param successors the subsequent operators in the pipeline to which the data is pushed
     */
    MonitoringSource(Monitoring::MetricCollectorPtr metricCollector,
                     std::chrono::milliseconds waitTime,
                     Runtime::BufferManagerPtr bufferManager,
                     Runtime::QueryManagerPtr queryManager,
                     OperatorId operatorId,
                     OriginId originId,
                     StatisticId statisticId,
                     size_t numSourceLocalBuffers,
                     const std::string& physicalSourceName,
                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors = {});

    SourceType getType() const override;

    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
     * @brief Returns the collector type of the monitoring source.
     * @return the collector type
     */
    Monitoring::MetricCollectorType getCollectorType();

    /**
     * @brief Returns the wait time of the monitoring source
     * @return wait time
     */
    std::chrono::milliseconds getWaitTime() const;

    /**
     * @brief override the toString method for the binary source
     * @return returns string describing the binary source
     */
    std::string toString() const override;

  private:
    Monitoring::MetricCollectorPtr metricCollector;
    std::chrono::milliseconds waitTime;

  public:
    static constexpr std::chrono::milliseconds DEFAULT_WAIT_TIME = std::chrono::milliseconds(1000);
};

using MonitoringSourcePtr = std::shared_ptr<MonitoringSource>;
};// namespace NES

#endif// NES_RUNTIME_INCLUDE_SOURCES_MONITORINGSOURCE_HPP_
