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
    void lowerSource(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);
    void lowerSink(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);
    void lowerScan(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);
    void lowerEmit(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);
    void lowerProjection(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);
    void lowerFilter(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);
    void lowerMap(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);
    void lowerWatermarkAssignment(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);
};

}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_DEFAULTGENERATABLEOPERATORPROVIDER_HPP_
