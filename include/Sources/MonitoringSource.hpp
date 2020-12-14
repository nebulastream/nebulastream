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

#ifndef NES_INCLUDE_SOURCES_MONITORINGSOURCE_HPP_
#define NES_INCLUDE_SOURCES_MONITORINGSOURCE_HPP_

#include <Sources/DefaultSource.hpp>

namespace NES {

class MonitoringPlan;
typedef std::shared_ptr<MonitoringPlan> MonitoringPlanPtr;

class MetricGroup;
typedef std::shared_ptr<MetricGroup> MetricGroupPtr;

class MonitoringSource : public DefaultSource {
  public:
    MonitoringSource(MonitoringPlanPtr monitoringPlan, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                     const uint64_t numbersOfBufferToProduce, uint64_t frequency, OperatorId operatorId);

    SourceType getType() const override;

    std::optional<TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method for the binary source
     * @return returns string describing the binary source
     */
    const std::string toString() const override;

  private:
    MonitoringPlanPtr monitoringPlan;
    MetricGroupPtr metricGroup;
};

typedef std::shared_ptr<MonitoringSource> MonitoringSourcePtr;
};// namespace NES

#endif//NES_INCLUDE_SOURCES_MONITORINGSOURCE_HPP_
