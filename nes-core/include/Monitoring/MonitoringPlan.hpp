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

#ifndef NES_INCLUDE_MONITORING_METRICS_MONITORING_PLAN_HPP_
#define NES_INCLUDE_MONITORING_METRICS_MONITORING_PLAN_HPP_

#include <memory>
#include <set>
#include <string>
#include <WorkerRPCService.pb.h>

namespace NES {

    class MonitoringPlan;
    using MonitoringPlanPtr = std::shared_ptr<MonitoringPlan>;
    class Schema;
    using SchemaPtr = std::shared_ptr<Schema>;

    /**
* @brief The MonitoringPlan is a config class to represent what metrics shall be collected and how.
*/
    class MonitoringPlan {
      public:
        static MonitoringPlanPtr create(const std::set<MetricCollectorType>& metrics);
        static MonitoringPlanPtr create(const SerializableMonitoringPlan& shippable);
        static MonitoringPlanPtr createDefaultPlan();

        /**
 * @brief Add a specific metric to the plan
 * @param metric
 */
        bool addMetric(MetricCollectorType metric);

        /**
 * @brief Checks if a metric is part of the MonitoringPlan
 * @param metric
 * @return true if contained, else false
 */
        [[nodiscard]] bool hasMetric(MetricCollectorType metric) const;

        /**
 * @brief Creates a serializable monitoring plan according to the Protobuf definition.
 * @return the serializable monitoring plan
 */
        [[nodiscard]] SerializableMonitoringPlan serialize() const;

        /**
 * @brief Returns a string representation of the plan
 * @return The string representation
 */
        [[nodiscard]] std::string toString() const;

        /**
 * @brief Returns the MetricColletorType objects that represent the plan.
 * @return A set of MetricCollectorType objects.
 */
        [[nodiscard]] const std::set<MetricCollectorType>& getMetricTypes() const;

        friend std::ostream& operator<<(std::ostream&, const MonitoringPlan&);

      private:
        explicit MonitoringPlan(const std::set<MetricCollectorType>& metrics);
        explicit MonitoringPlan(const SerializableMonitoringPlan& plan);

        //enum defined in SerializableDataType.proto
        std::set<MetricCollectorType> metricTypes;
    };

}// namespace NES

#endif// NES_INCLUDE_MONITORING_METRICS_MONITORING_PLAN_HPP_