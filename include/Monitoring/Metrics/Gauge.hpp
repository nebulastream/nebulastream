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

#ifndef NES_INCLUDE_MONITORING_METRICS_GAUGE_HPP_
#define NES_INCLUDE_MONITORING_METRICS_GAUGE_HPP_

#include <Monitoring/Metrics/MetricType.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <Util/Logger.hpp>
#include <functional>
#include <memory>

namespace NES {
class Schema;
class MonitoringPlan;

template<typename T>
/**
 * A Gauge is a metric that calculates a specific value at a point in time.
 */
class Gauge {
  public:
    explicit Gauge(std::function<T()>&& probingFunc) : probingFunc(probingFunc) { NES_DEBUG("Gauge: Initializing"); }

    /**
     * @brief Calculates and returns the measured value.
     * @return the value
     */
    T measure() { return probingFunc(); }

  private:
    std::function<T()> probingFunc;
};

template<typename T>
MetricType getMetricType(const Gauge<T>&) {
    return MetricType::GaugeType;
}

template<typename T>
void serialize(Gauge<T>& metric, std::shared_ptr<Schema> schema, NodeEngine::TupleBuffer& buf, const std::string& prefix) {
    serialize(metric.measure(), schema, buf, prefix);
}

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICS_GAUGE_HPP_
