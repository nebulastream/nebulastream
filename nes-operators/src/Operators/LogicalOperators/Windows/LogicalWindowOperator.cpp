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
#include <ostream>
#include <ranges>
#include <sstream>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowOperator.hpp>
#include <Types/ContentBasedWindowType.hpp>
#include <Types/ThresholdWindow.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <Common/DataTypes/BasicTypes.hpp>

namespace NES
{

LogicalWindowOperator::LogicalWindowOperator(
    const std::shared_ptr<Windowing::LogicalWindowDescriptor>& windowDefinition, const OperatorId id)
    : Operator(id), WindowOperator(windowDefinition, id)
{
}

std::ostream& LogicalWindowOperator::toDebugString(std::ostream& os) const
{
    auto windowType = windowDefinition->getWindowType();
    auto windowAggregation = windowDefinition->getWindowAggregation();
    return os << fmt::format(
               "WINDOW AGGREGATION({}, {}, window type: {})",
               id,
               fmt::join(std::views::transform(windowAggregation, [](const auto& agg) { return agg->toString(); }), ", "),
               windowType->toString());
}

std::ostream& LogicalWindowOperator::toQueryPlanString(std::ostream& os) const
{
    auto windowAggregation = windowDefinition->getWindowAggregation();
    return os << fmt::format(
               "WINDOW AGG({})",
               fmt::join(std::views::transform(windowAggregation, [](const auto& agg) { return agg->getTypeAsString(); }), ", "));
}

bool LogicalWindowOperator::isIdentical(const std::shared_ptr<Node>& rhs) const
{
    return equal(rhs) && (NES::Util::as<LogicalWindowOperator>(rhs)->getId() == id);
}

bool LogicalWindowOperator::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<LogicalWindowOperator>(rhs))
    {
        const auto rhsWindow = NES::Util::as<LogicalWindowOperator>(rhs);
        return windowDefinition->equal(rhsWindow->windowDefinition);
    }
    return false;
}

std::shared_ptr<Operator> LogicalWindowOperator::copy()
{
    auto copy = std::make_shared<LogicalWindowOperator>(windowDefinition, id);
    copy->setOriginId(originId);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}

bool LogicalWindowOperator::inferSchema()
{
    if (!WindowOperator::inferSchema())
    {
        return false;
    }
    /// infer the default input and output schema
    NES_DEBUG("LogicalWindowOperator: TypeInferencePhase: infer types for window operator with input schema {}", inputSchema->toString());

    /// infer type of aggregation
    auto windowAggregation = windowDefinition->getWindowAggregation();
    for (const auto& agg : windowAggregation)
    {
        agg->inferStamp(*inputSchema);
    }

    ///Construct output schema
    ///First clear()
    outputSchema->clear();

    /// Distinguish process between different window types (currently time-based and content-based)
    const auto windowType = windowDefinition->getWindowType();
    if (Util::instanceOf<Windowing::TimeBasedWindowType>(windowType))
    {
        /// typeInference
        if (!Util::as<Windowing::TimeBasedWindowType>(windowType)->inferStamp(*inputSchema))
        {
            return false;
        }
        const auto& sourceName = inputSchema->getQualifierNameForSystemGeneratedFields();
        const auto& newQualifierForSystemField = sourceName;

        windowMetaData.windowStartFieldName = newQualifierForSystemField + "$start";
        windowMetaData.windowEndFieldName = newQualifierForSystemField + "$end";
        outputSchema->addField(windowMetaData.windowStartFieldName, BasicType::UINT64);
        outputSchema->addField(windowMetaData.windowEndFieldName, BasicType::UINT64);
    }
    else if (Util::instanceOf<Windowing::ContentBasedWindowType>(windowType))
    {
        /// type Inference for Content-based Windows requires the typeInferencePhaseContext
        const auto contentBasedWindowType = Util::as<Windowing::ContentBasedWindowType>(windowType);
        if (contentBasedWindowType->getContentBasedSubWindowType()
            == Windowing::ContentBasedWindowType::ContentBasedSubWindowType::THRESHOLDWINDOW)
        {
            const auto thresholdWindow = Windowing::ContentBasedWindowType::asThresholdWindow(contentBasedWindowType);
            if (!thresholdWindow->inferStamp(*inputSchema))
            {
                return false;
            }
        }
    }
    else
    {
        throw CannotInferSchema("Unsupported window type {}", windowDefinition->getWindowType()->toString());
    }

    if (windowDefinition->isKeyed())
    {
        /// infer the data type of the key field.
        auto keyList = windowDefinition->getKeys();
        for (const auto& key : keyList)
        {
            key->inferStamp(*inputSchema);
            outputSchema->addField(AttributeField::create(key->getFieldName(), key->getStamp()));
        }
    }
    for (const auto& agg : windowAggregation)
    {
        outputSchema->addField(
            AttributeField::create(NES::Util::as<NodeFunctionFieldAccess>(agg->as())->getFieldName(), agg->as()->getStamp()));
    }

    NES_DEBUG("Outputschema for window={}", outputSchema->toString());

    return true;
}

void LogicalWindowOperator::inferStringSignature()
{
    const std::shared_ptr<Operator> operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("Inferring String signature for {}", *operatorNode);

    ///Infer query signatures for child operators
    for (const auto& child : children)
    {
        const std::shared_ptr<LogicalOperator> childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }

    std::stringstream signatureStream;
    const auto windowType = windowDefinition->getWindowType();
    const auto windowAggregation = windowDefinition->getWindowAggregation();
    if (windowDefinition->isKeyed())
    {
        signatureStream << "WINDOW-BY-KEY(";
        for (const auto& key : windowDefinition->getKeys())
        {
            signatureStream << *key << ",";
        }
    }
    else
    {
        signatureStream << "WINDOW(";
    }
    signatureStream << "WINDOW-TYPE: " << windowType->toString() << ",";
    signatureStream << "AGGREGATION: ";
    for (const auto& agg : windowAggregation)
    {
        signatureStream << agg->toString() << ",";
    }
    signatureStream << ")";
    const auto childSignature = NES::Util::as<LogicalOperator>(children[0])->getHashBasedSignature();
    signatureStream << "." << *childSignature.begin()->second.begin();

    auto signature = signatureStream.str();
    ///Update the signature
    const auto hashCode = hashGenerator(signature);
    hashBasedSignature[hashCode] = {signature};
}

std::vector<std::string> LogicalWindowOperator::getGroupByKeyNames() const
{
    std::vector<std::string> groupByKeyNames = {};
    const auto windowDefinition = this->getWindowDefinition();
    if (windowDefinition->isKeyed())
    {
        const std::vector<std::shared_ptr<NodeFunctionFieldAccess>> groupByKeys = windowDefinition->getKeys();
        groupByKeyNames.reserve(groupByKeys.size());
        for (const auto& groupByKey : groupByKeys)
        {
            groupByKeyNames.push_back(groupByKey->getFieldName());
        }
    }
    return groupByKeyNames;
}

}
