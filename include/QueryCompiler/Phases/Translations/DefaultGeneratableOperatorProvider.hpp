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
#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_DEFAULTGENERATABLEOPERATORPROVIDER_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_DEFAULTGENERATABLEOPERATORPROVIDER_HPP_
#include <QueryCompiler/Phases/Translations/GeneratableOperatorProvider.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/Aggregations/GeneratableWindowAggregation.hpp>
namespace NES {
namespace QueryCompilation {

/**
 * @brief Provides a set of default lowerings for logical operators to corresponding physical operators.
 */
class DefaultGeneratableOperatorProvider : public GeneratableOperatorProvider {
  public:
    static GeneratableOperatorProviderPtr create();
    void lower(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode) override;

  protected:
    /**
     * @brief Lowers a source operator. In this case we perform no action, as physical source operators can't be lowered.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerSource(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);

    /**
     * @brief Lowers a sink operator. In this case we perform no action, as physical sink operators can't be lowered.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerSink(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);

    /**
     * @brief Lowers a scan operator and creates a corresponding generatable buffer scan.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerScan(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);

    /**
     * @brief Lowers a source operator. In this case we perform no action, as physical source operators can't be lowered.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerEmit(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);

    /**
     * @brief Lowers a source operator. In this case we perform no action, as physical source operators can't be lowered.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerProjection(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);

    /**
     * @brief Lowers a source operator. In this case we perform no action, as physical source operators can't be lowered.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerFilter(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);

    /**
     * @brief Lowers a source operator. In this case we perform no action, as physical source operators can't be lowered.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerMap(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);

    /**
     * @brief Lowers a source operator. In this case we perform no action, as physical source operators can't be lowered.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerWatermarkAssignment(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);

    /**
     * @brief Lowers a source operator. In this case we perform no action, as physical source operators can't be lowered.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerWindowSink(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);

    /**
     * @brief Lowers a source operator. In this case we perform no action, as physical source operators can't be lowered.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerSlicePreAggregation(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);

    /**
     * @brief Lowers a source operator. In this case we perform no action, as physical source operators can't be lowered.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerJoinBuild(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);

    /**
     * @brief Lowers a source operator. In this case we perform no action, as physical source operators can't be lowered.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerJoinSink(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);

    /**
     * @brief Lowers a source operator. In this case we perform no action, as physical source operators can't be lowered.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    GeneratableWindowAggregationPtr lowerWindowAggregation(Windowing::WindowAggregationDescriptorPtr windowAggregationDescriptor);
};

}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_DEFAULTGENERATABLEOPERATORPROVIDER_HPP_
