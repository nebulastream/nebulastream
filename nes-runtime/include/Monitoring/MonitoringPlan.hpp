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

#ifndef NES_RUNTIME_INCLUDE_MONITORING_MONITORINGPLAN_HPP_
#define NES_RUNTIME_INCLUDE_MONITORING_MONITORINGPLAN_HPP_

#include <Monitoring/Metrics/MetricType.hpp>
#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Util/MetricCollectorType.hpp>
#include <memory>
#include <set>
#include <string>

namespace NES::Monitoring {

/**
* @brief The MonitoringPlan is a config class to represent what metrics shall be collected and how.
*/
class MonitoringPlan {
  public:
    static MonitoringPlanPtr create(const std::set<MetricType>& metrics);
    static MonitoringPlanPtr defaultPlan();

    /**
     * @brief Returns the default collectors of the plan.
     * @return A set of collectors.
     */
    static std::set<MetricCollectorType> defaultCollectors();

    /**
     * @brief Add a specific metric to the plan
     * @param metric
    */
    bool addMetric(MetricType metric);

    /**
     * @brief Checks if a metric is part of the MonitoringPlan
     * @param metric
     * @return true if contained, else false
    */
    [[nodiscard]] bool hasMetric(MetricType metric) const;

    /**
     * @brief Returns a string representation of the plan
     * @return The string representation
    */
    [[nodiscard]] std::string toString() const;

    /**
     * @brief Returns the MetricType objects that represent the plan.
     * @return A set of metric type objects.
    */
    [[nodiscard]] const std::set<MetricType>& getMetricTypes() const;

    /**
     * @brief Returns the MetricType objects that represent the plan.
     * @return A set of metric type objects.
    */
    [[nodiscard]] const std::set<MetricCollectorType> getCollectorTypes() const;

    friend std::ostream& operator<<(std::ostream&, const MonitoringPlan&);

  private:
    explicit MonitoringPlan(const std::set<MetricType>& metrics);

    //enum defined in SerializableDataType.proto
    std::set<MetricType> metricTypes;
};

}// namespace NES::Monitoring

#endif  // NES_RUNTIME_INCLUDE_MONITORING_MONITORINGPLAN_HPP_
