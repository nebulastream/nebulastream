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

#include <Exceptions/UDFException.hpp>
#include <Operators/LogicalOperators/OpenCLLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UDFs/JavaUDFDescriptor.hpp>
#include <Operators/LogicalOperators/UDFs/MapUDF/MapUDFLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UDFs/UDFDescriptor.hpp>
#include <Optimizer/QueryRewrite/MapUDFsToOpenCLOperatorsRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <memory>

namespace NES::Optimizer {

MapUDFsToOpenCLOperatorsRulePtr NES::Optimizer::MapUDFsToOpenCLOperatorsRule::create() {
    return std::make_shared<MapUDFsToOpenCLOperatorsRule>(MapUDFsToOpenCLOperatorsRule());
}

QueryPlanPtr MapUDFsToOpenCLOperatorsRule::apply(NES::QueryPlanPtr queryPlan) {

    auto mapJavaUDFOperatorsToReplace = queryPlan->getOperatorByType<MapUDFLogicalOperatorNode>();
    if (mapJavaUDFOperatorsToReplace.empty()) {
        return queryPlan;
    }

    for (const auto& mapJavaUDFOperator : mapJavaUDFOperatorsToReplace) {
        //Create new open cl operator
        auto udfDescriptor = mapJavaUDFOperator->getUDFDescriptor();
        if (udfDescriptor->instanceOf<Catalogs::UDF::JavaUDFDescriptor>()) {
            auto javaUDFDescriptor = udfDescriptor->as<Catalogs::UDF::JavaUDFDescriptor>(udfDescriptor);
            auto openCLOperator = std::make_shared<OpenCLLogicalOperatorNode>(javaUDFDescriptor, getNextOperatorId());
            //replace map java udf operator with open cl operator
            if (!mapJavaUDFOperator->replace(openCLOperator)) {
                NES_ERROR("MapUDFsToOpenCLOperatorsRule: Unable to replace map java UDF with Open cl operator");
                throw UDFException("MapUDFsToOpenCLOperatorsRule: Unable to replace map java UDF with Open cl operator");
            }
        }
    }
    return queryPlan;
}

}// namespace NES::Optimizer
