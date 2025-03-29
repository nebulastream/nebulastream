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
#include <Functions/ExecutableFunctionWriteField.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Phases/Translations/FunctionProvider.hpp>
#include <Plans/PhysicalQueryPlan.hpp>
#include <Plans/PlanIterator.hpp>
#include <Plans/QueryPlan.hpp>
#include <Util/Common.hpp>
#include <grpcpp/impl/codegen/config_protobuf.h>
#include <ErrorHandling.hpp>
#include <Map.hpp>
#include <PhysicalOperator.hpp>
#include <Scan.hpp>
#include <Selection.hpp>

// TODO we have muliple occurences of ExceuctableOperators. Should there be only one?

using namespace NES;

namespace NES::QueryCompilation::LowerLogicalToNautilusOperators
{

// TODO get rid of the shared_ptrs
std::shared_ptr<PhysicalOperatorNode> lowerFilter(const std::shared_ptr<LogicalFilterOperator>& operatorPtr)
{
    auto function = FunctionProvider::lowerFunction(operatorPtr->getPredicate());
    auto selection = std::make_shared<Selection>(std::move(function));
    return std::make_shared<PhysicalOperatorNode>(selection);
}

std::shared_ptr<PhysicalOperatorNode> lowerMap(const std::shared_ptr<LogicalMapOperator>& operatorPtr)
{
    auto assignmentField = operatorPtr->getMapFunction()->getField();
    auto assignmentFunction = operatorPtr->getMapFunction()->getAssignment();
    auto function = FunctionProvider::lowerFunction(assignmentFunction);
    auto writeField = std::make_unique<Functions::ExecutableFunctionWriteField>(assignmentField->getFieldName(), std::move(function));
    auto map = std::make_shared<Map>(std::move(writeField));
    return std::make_shared<PhysicalOperatorNode>(map);
}

std::shared_ptr<PhysicalOperatorNode> lower(const std::shared_ptr<NES::Operator>& operatorNode)
{
    if (NES::Util::instanceOf<LogicalFilterOperator>(operatorNode))
    {
        auto filter = lowerFilter(NES::Util::as<LogicalFilterOperator>(operatorNode));
        return filter;
    }

    if (NES::Util::instanceOf<LogicalMapOperator>(operatorNode))
    {
        auto map = lowerMap(NES::Util::as<LogicalMapOperator>(operatorNode));
        return map;
    }

    throw UnknownLogicalOperator("Cannot lower {}", operatorNode->toString());
}

std::shared_ptr<PhysicalQueryPlan> apply(const std::shared_ptr<DecomposedQueryPlan>& decomposedQueryPlan)
{
    std::shared_ptr<PhysicalOperatorNode> rootOperator;
    std::shared_ptr<PhysicalOperatorNode> currentOperator;
    for (const auto& node : PlanIterator(decomposedQueryPlan).snapshot())
    {
        if (NES::Util::instanceOf<SinkLogicalOperator>(node))
        {
            rootOperator = std::make_shared<PhysicalOperatorNode>(NES::Util::as<SinkLogicalOperator>(node));
            currentOperator = rootOperator;
        }
        else if (NES::Util::instanceOf<SourceDescriptorLogicalOperator>(node))
        {
            auto tmp = currentOperator;
            currentOperator = std::make_shared<PhysicalOperatorNode>(NES::Util::as<SourceDescriptorLogicalOperator>(node));
            tmp->children.push_back(currentOperator);
        }
        else if (NES::Util::instanceOf<LogicalOperator>(node))
        {
            auto tmp = currentOperator;
            currentOperator = lower(NES::Util::as<LogicalOperator>(node));
            tmp->children.push_back(currentOperator); // TODO this might not work for more complex variants
        }
        else
        {
            throw UnknownLogicalOperator("Cannot lower {}", node->toString());
        }
    }
    auto plan = std::make_shared<PhysicalQueryPlan>(decomposedQueryPlan->getQueryId(), std::vector{rootOperator});
    std::cout << printPhysicalQueryPlan(*plan);
    return plan;
}
}
