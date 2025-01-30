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

#include <memory>
#include <utility>
#include <vector>
#include <Functions/WriteFieldPhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/MapLogicalOperator.hpp>
#include <Operators/LogicalOperators/SelectionLogicalOperator.hpp>
#include <Plans/DecomposedQueryPlan.hpp>
#include <Plans/PhysicalQueryPlan.hpp>
#include <Plans/QueryPlan.hpp>
#include <TraitSets/RewriteRules/RewriteRuleRegistry.hpp>
#include <Util/Common.hpp>
#include <grpcpp/impl/codegen/config_protobuf.h>
#include <AbstractPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <MapPhysicalOperator.hpp>
#include <ScanPhysicalOperator.hpp>
#include <SelectionPhysicalOperator.hpp>
#include <Functions/FunctionProvider.hpp>

using namespace NES;

namespace NES::QueryCompilation::LowerLogicalToNautilusOperators
{

std::shared_ptr<PhysicalOperatorNode> lowerFilter(const std::shared_ptr<SelectionLogicalOperator>& operatorPtr)
{
    auto function = FunctionProvider::lowerFunction(operatorPtr->getPredicate());
    auto selection = std::make_shared<SelectionPhysicalOperator>(std::move(function));
    return std::make_shared<PhysicalOperatorNode>(selection);
}

std::shared_ptr<PhysicalOperatorNode> lowerMap(const std::shared_ptr<MapLogicalOperator>& operatorPtr)
{
    auto assignmentField = operatorPtr->getMapFunction()->getField();
    auto assignmentFunction = operatorPtr->getMapFunction()->getAssignment();
    auto function = FunctionProvider::lowerFunction(assignmentFunction);
    auto writeField = std::make_unique<Functions::WriteFieldPhysicalFunction>(assignmentField->getFieldName(), std::move(function));
    auto map = std::make_shared<MapPhysicalOperator>(std::move(writeField));
    return std::make_shared<PhysicalOperatorNode>(map);
}

std::shared_ptr<PhysicalOperatorNode> lower(const std::shared_ptr<NES::Operator>& operatorNode)
{
    if (NES::Util::instanceOf<SelectionLogicalOperator>(operatorNode))
    {
        auto filter = lowerFilter(NES::Util::as<SelectionLogicalOperator>(operatorNode));
        return filter;
    }

    if (NES::Util::instanceOf<MapLogicalOperator>(operatorNode))
    {
        auto map = lowerMap(NES::Util::as<MapLogicalOperator>(operatorNode));
        return map;
    }

    throw UnknownLogicalOperator("Cannot lower {}", operatorNode->toString());
}

std::shared_ptr<PhysicalQueryPlan> apply(const std::shared_ptr<DecomposedQueryPlan>& decomposedQueryPlan)
{
    std::shared_ptr<PhysicalOperatorNode> rootOperator;
    std::shared_ptr<PhysicalOperatorNode> currentOperator;
    // TODO this is wrong...
    for (const auto& operatorNode : BFSRange<Operator>(decomposedQueryPlan->getRootOperators()[0]))
    {
        if (NES::Util::instanceOf<SinkLogicalOperator>(operatorNode))
        {
            rootOperator = std::make_shared<PhysicalOperatorNode>(NES::Util::as<SinkLogicalOperator>(operatorNode));
            currentOperator = rootOperator;
        }
        else if (NES::Util::instanceOf<SourceDescriptorLogicalOperator>(operatorNode))
        {
            auto tmp = currentOperator;
            currentOperator = std::make_shared<PhysicalOperatorNode>(NES::Util::as<SourceDescriptorLogicalOperator>(operatorNode));
            tmp->children.push_back(currentOperator);
        }
        else if (NES::Util::instanceOf<LogicalOperator>(operatorNode))
        {
            auto tmp = currentOperator;
            currentOperator = lower(NES::Util::as<LogicalOperator>(operatorNode));
            tmp->children.push_back(currentOperator); // TODO this might not work for more complex variants
        }
        else if (auto logicalOperator = RewriteRuleRegistry::instance().create("LowerToPhysicalMap"))
        {

        }
        else
        {
            throw UnknownLogicalOperator("Cannot lower {}", operatorNode->toString());
        }
    }
    auto plan = std::make_shared<PhysicalQueryPlan>(decomposedQueryPlan->getQueryId(), std::vector{rootOperator});
    std::cout << printPhysicalQueryPlan(*plan);
    return plan;
}
}
