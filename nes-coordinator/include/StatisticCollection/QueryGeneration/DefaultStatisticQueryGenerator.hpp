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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_QUERYGENERATION_DEFAULTSTATISTICQUERYGENERATOR_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_QUERYGENERATION_DEFAULTSTATISTICQUERYGENERATOR_HPP_

#include <StatisticCollection/QueryGeneration/AbstractStatisticQueryGenerator.hpp>

namespace NES::Statistic {

/**
 * @brief Generates a statistic query by a simple mapping strategy of
 * metric types ----> Synopsis
 */
class DefaultStatisticQueryGenerator : public AbstractStatisticQueryGenerator {
  public:
    /**
     * @brief Creates a DefaultStatisticQueryGenerator
     * @return AbstractStatisticQueryGeneratorPtr
     */
    static StatisticQueryGeneratorPtr create();

    /**
     * @brief Create a statistic query via a mapping strategy
     * @param characteristic
     * @param window
     * @param sendingPolicy
     * @param triggerCondition
     * @param queryCatalog
     * @return Query
     */
    Query createStatisticQuery(const Characteristic& characteristic,
                               const Windowing::WindowTypePtr& window,
                               const SendingPolicyPtr& sendingPolicy,
                               const TriggerConditionPtr& triggerCondition,
                               const Catalogs::Query::QueryCatalog& queryCatalog) override;

    /**
     * @brief Virtual destructor
     */
    virtual ~DefaultStatisticQueryGenerator();
};

}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_QUERYGENERATION_DEFAULTSTATISTICQUERYGENERATOR_HPP_
