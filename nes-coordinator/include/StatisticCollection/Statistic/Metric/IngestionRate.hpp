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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTIC_INGESTIONRATE_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTIC_INGESTIONRATE_HPP_
#include <StatisticCollection/Statistic/Metric/Metric.hpp>

namespace NES::Statistic {

/**
 * @brief Collects the ingestion rate on a node
 */
class IngestionRate : public Metric {
  public:
    /**
     * @brief Creates a IngestionRate wrapped in a MetricPtr
     * @return MetricPtr
     */
    static MetricPtr create();

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if equal otherwise false
     */
    bool operator==(const Metric& rhs) const override;

    /**
     * @brief Gets the fieldName
     * @return Const reference to std::string
     */
    const std::string& getFieldName() const;

  private:
    /**
     * @brief Private constructor for IngestionRate
     */
    explicit IngestionRate();

    const std::string fieldName;
};

}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTIC_INGESTIONRATE_HPP_
