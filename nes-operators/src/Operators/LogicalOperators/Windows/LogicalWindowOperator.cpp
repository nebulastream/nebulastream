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
#include <sstream>
#include <API/Schema.hpp>
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


namespace NES
{

LogicalWindowOperator::LogicalWindowOperator(
    const std::shared_ptr<Windowing::LogicalWindowDescriptor>& windowDefinition, const OperatorId id)
    : Operator(id), WindowOperator(windowDefinition, id)
{
}

std::string LogicalWindowOperator::toString() const
{
    std::stringstream ss;
    auto windowType = windowDefinition->getWindowType();
    const auto windowAggregation = windowDefinition->getWindowAggregation();
    ss << "WINDOW AGGREGATION(OP-" << id << ", ";
    for (const auto& agg : windowAggregation)
    {
        ss << agg->toString() << ";";
    }
    ss << ") on window type: " << windowType->toString();
    return ss.str();
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
    NES_DEBUG("LogicalWindowOperator: TypeInferencePhase: infer types for window operator with input schema {}", inputSchema);

    /// infer type of aggregation
    auto windowAggregation = windowDefinition->getWindowAggregation();
    for (const auto& agg : windowAggregation)
    {
        agg->inferStamp(inputSchema);
    }

    ///Construct output schema
    ///First clear()
    outputSchema = {};

    /// Distinguish process between different window types (currently time-based and content-based)
    const auto windowType = windowDefinition->getWindowType();
    if (Util::instanceOf<Windowing::TimeBasedWindowType>(windowType))
    {
        /// typeInference
        if (!Util::as<Windowing::TimeBasedWindowType>(windowType)->inferStamp(inputSchema))
        {
            return false;
        }

        windowMetaData.windowStartFieldName = "start";
        windowMetaData.windowEndFieldName = "end";
        outputSchema.addField(
            Schema::Identifier(windowMetaData.windowStartFieldName), DataTypeRegistry::instance().lookup("Integer").value());
        outputSchema.addField(
            Schema::Identifier(windowMetaData.windowEndFieldName), DataTypeRegistry::instance().lookup("Integer").value());
    }
    else if (Util::instanceOf<Windowing::ContentBasedWindowType>(windowType))
    {
        /// type Inference for Content-based Windows requires the typeInferencePhaseContext
        const auto contentBasedWindowType = Util::as<Windowing::ContentBasedWindowType>(windowType);
        if (contentBasedWindowType->getContentBasedSubWindowType()
            == Windowing::ContentBasedWindowType::ContentBasedSubWindowType::THRESHOLDWINDOW)
        {
            const auto thresholdWindow = Windowing::ContentBasedWindowType::asThresholdWindow(contentBasedWindowType);
            if (!thresholdWindow->inferStamp(inputSchema))
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
        for (auto& key : keyList)
        {
            key.inferStamp(inputSchema);
            if (const auto* access = key.as<FieldAccessExpression>())
            {
                outputSchema.addField(access->getFieldName(), access->getStamp().value());
            }
            else
            {
                outputSchema.addField(Schema::Identifier("key"), key.getStamp().value());
            }
        }
    }

    for (const auto& agg : windowAggregation)
    {
        outputSchema.addField(agg->as(), agg->getFinalAggregateStamp());
    }

    NES_DEBUG("Outputschema for window={}", outputSchema);

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
            signatureStream << format_as(key) << ",";
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
    const auto childSignature = NES::Util::as<LogicalOperator>(children.at(0))->getHashBasedSignature();
    signatureStream << "." << *childSignature.begin()->second.begin();

    auto signature = signatureStream.str();
    ///Update the signature
    const auto hashCode = hashGenerator(signature);
    hashBasedSignature[hashCode] = {signature};
}

std::vector<Schema::Identifier> LogicalWindowOperator::getGroupByKeyNames() const
{
    std::vector<Schema::Identifier> groupByKeyNames = {};
    size_t anonymousKeyIndex = 0;
    const auto windowDefinition = this->getWindowDefinition();
    if (windowDefinition->isKeyed())
    {
        const std::vector<ExpressionValue> groupByKeys = windowDefinition->getKeys();
        groupByKeyNames.reserve(groupByKeys.size());
        for (const auto& groupByKey : groupByKeys)
        {
            if (auto fieldAccess = groupByKey.as<FieldAccessExpression>())
            {
                groupByKeyNames.push_back(fieldAccess->getFieldName());
            }
            else
            {
                groupByKeyNames.push_back(Schema::Identifier(fmt::format("Key{}", anonymousKeyIndex++)));
            }
        }
    }
    return groupByKeyNames;
}

}
