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

#ifndef NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_QUERYGENERATION_ABSTRACTSTATISTICQUERYGENERATOR_HPP_
#define NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_QUERYGENERATION_ABSTRACTSTATISTICQUERYGENERATOR_HPP_

#include <API/Query.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicy.hpp>
#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/TriggerCondition.hpp>
#include <StatisticCollection/Characteristic/Characteristic.hpp>

namespace NES::Statistic {

class AbstractStatisticQueryGenerator;
using StatisticQueryGeneratorPtr = std::shared_ptr<AbstractStatisticQueryGenerator>;

/**
 * @brief Interface for creating a statistic query
 */
class AbstractStatisticQueryGenerator {
  public:
    /**
     * @brief Creates a query that is used for creating the statistics
     * @param characteristic
     * @param window
     * @param sendingPolicy
     * @param triggerCondition
     * @param queryCatalog
     * @return Query
     */
    virtual Query createStatisticQuery(const Characteristic& characteristic,
                                       const Windowing::WindowTypePtr& window,
                                       const SendingPolicyPtr& sendingPolicy,
                                       const TriggerConditionPtr& triggerCondition,
                                       const Catalogs::Query::QueryCatalog& queryCatalog) = 0;

    /**
     * @brief Virtual destructor
     */
    virtual ~AbstractStatisticQueryGenerator() = default;
};

}// namespace NES::Statistic

#endif // NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_QUERYGENERATION_ABSTRACTSTATISTICQUERYGENERATOR_HPP_
