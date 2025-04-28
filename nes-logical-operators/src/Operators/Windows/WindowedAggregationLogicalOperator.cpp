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
#include <string>
#include <vector>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Types/ContentBasedWindowType.hpp>
#include <WindowTypes/Types/ThresholdWindow.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <Common/DataTypes/BasicTypes.hpp>

namespace NES
{

WindowedAggregationLogicalOperator::WindowedAggregationLogicalOperator(
    std::vector<FieldAccessLogicalFunction> onKey,
    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> windowAggregation,
    std::shared_ptr<Windowing::WindowType> windowType)
    : windowAggregation(std::move(windowAggregation)), windowType(std::move(windowType)), onKey(onKey)
{
}

std::string_view WindowedAggregationLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string WindowedAggregationLogicalOperator::toString() const
{
    std::stringstream ss;
    auto& wt = getWindowType();
    const auto& aggs = getWindowAggregation();
    ss << "WINDOW AGGREGATION(OP-" << id << ", ";
    for (const auto& agg : aggs)
    {
        ss << agg->toString() << ";";
    }
    ss << ") on window type: " << wt.toString();
    return ss.str();
}

bool WindowedAggregationLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const WindowedAggregationLogicalOperator*>(&rhs))
    {
        if (this->isKeyed() != rhsOperator->isKeyed())
        {
            return false;
        }

        if (this->getKeys().size() != rhsOperator->getKeys().size())
        {
            return false;
        }

        for (uint64_t i = 0; i < this->getKeys().size(); i++)
        {
            if (this->getKeys()[i] != rhsOperator->getKeys()[i])
            {
                return false;
            }
        }

        if (this->getWindowAggregation().size() != rhsOperator->getWindowAggregation().size())
        {
            return false;
        }

        for (uint64_t i = 0; i < this->getWindowAggregation().size(); i++)
        {
            if (this->getWindowAggregation()[i] != rhsOperator->getWindowAggregation()[i])
            {
                return false;
            }
        }
        return *windowType == rhsOperator->getWindowType();
    }
    return false;
}

LogicalOperator WindowedAggregationLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    PRECONDITION(inputSchemas.size() == 1, "WindowAggregation should have only one input");
    const auto& inputSchema = inputSchemas[0];

    // Infer type of aggregation.
    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> newFunctions;
    for (const auto& agg : getWindowAggregation())
    {
        agg->inferStamp(inputSchema);
        newFunctions.push_back(agg);
    }
    copy.windowAggregation = newFunctions;

    copy.windowType->inferStamp(inputSchema);
    copy.inputSchema = inputSchema;
    copy.outputSchema.clear();

    if (auto* timeWindow = dynamic_cast<Windowing::TimeBasedWindowType*>(&getWindowType()))
    {
        const auto& sourceName = inputSchema.getQualifierNameForSystemGeneratedFields();
        const auto& newQualifierForSystemField = sourceName;

        copy.windowStartFieldName = newQualifierForSystemField + "$start";
        copy.windowEndFieldName = newQualifierForSystemField + "$end";
        copy.outputSchema.addField(copy.windowStartFieldName, BasicType::UINT64);
        copy.outputSchema.addField(copy.windowEndFieldName, BasicType::UINT64);
    }
    else if (auto* contentBasedWindowType = dynamic_cast<Windowing::ContentBasedWindowType*>(&getWindowType()))
    {
        if (contentBasedWindowType->getContentBasedSubWindowType()
            == Windowing::ContentBasedWindowType::ContentBasedSubWindowType::THRESHOLDWINDOW)
        {
            auto thresholdWindow = Windowing::ContentBasedWindowType::asThresholdWindow(*contentBasedWindowType);
        }
    }
    else
    {
        throw CannotInferSchema("Unsupported window type {}", getWindowType().toString());
    }

    if (isKeyed())
    {
        auto keys = getKeys();
        auto newKeys = std::vector<FieldAccessLogicalFunction>();
        for (auto& key : keys)
        {
            auto newKey = key.withInferredStamp(inputSchema).get<FieldAccessLogicalFunction>();
            newKeys.push_back(newKey);
            copy.outputSchema.addField(AttributeField(newKey.getFieldName(), newKey.getStamp()));
        }
        copy.onKey = newKeys;
    }
    for (const auto& agg : copy.windowAggregation)
    {
        copy.outputSchema.addField(AttributeField(agg->asField.getFieldName(), agg->asField.getStamp()));
    }
    return copy;
}

Optimizer::TraitSet WindowedAggregationLogicalOperator::getTraitSet() const
{
    return {originIdTrait};
}

LogicalOperator WindowedAggregationLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> WindowedAggregationLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema WindowedAggregationLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> WindowedAggregationLogicalOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> WindowedAggregationLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator WindowedAggregationLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "Windowed aggregation should have only one input");
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}

LogicalOperator WindowedAggregationLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    PRECONDITION(ids.size() == 1, "Windowed aggregation should have only one output OriginId");
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> WindowedAggregationLogicalOperator::getChildren() const
{
    return children;
}

bool WindowedAggregationLogicalOperator::isKeyed() const
{
    return !onKey.empty();
}

std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> WindowedAggregationLogicalOperator::getWindowAggregation() const
{
    return windowAggregation;
}

void WindowedAggregationLogicalOperator::setWindowAggregation(std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> wa)
{
    windowAggregation = std::move(wa);
}

Windowing::WindowType& WindowedAggregationLogicalOperator::getWindowType() const
{
    return *windowType;
}

void WindowedAggregationLogicalOperator::setWindowType(std::unique_ptr<Windowing::WindowType> wt)
{
    windowType = std::move(wt);
}

std::vector<FieldAccessLogicalFunction> WindowedAggregationLogicalOperator::getKeys() const
{
    return onKey;
}

[[nodiscard]] std::string WindowedAggregationLogicalOperator::getWindowStartFieldName() const
{
    return windowStartFieldName;
}

[[nodiscard]] std::string WindowedAggregationLogicalOperator::getWindowEndFieldName() const
{
    return windowEndFieldName;
}

SerializableOperator WindowedAggregationLogicalOperator::serialize() const
{
    SerializableOperator serializedOperator;

    auto* opDesc = new SerializableOperator_LogicalOperator();
    opDesc->set_operatortype(NAME);
    serializedOperator.set_operatorid(this->id.getRawValue());
    serializedOperator.add_childrenids(getChildren()[0].getId().getRawValue());

    auto* unaryOpDesc = new SerializableOperator_UnaryLogicalOperator();
    auto* inputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->getInputSchemas()[0], inputSchema);
    unaryOpDesc->set_allocated_inputschema(inputSchema);

    auto ids = this->getInputOriginIds()[0];
    for (const auto& originId : ids)
    {
        unaryOpDesc->add_originids(originId.getRawValue());
    }

    opDesc->set_allocated_unaryoperator(unaryOpDesc);
    auto* outputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->getOutputSchema(), outputSchema);
    serializedOperator.set_allocated_outputschema(outputSchema);
    serializedOperator.set_allocated_operator_(opDesc);

    return serializedOperator;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterWindowedAggregationLogicalOperator(NES::LogicalOperatorRegistryArguments)
{
    /// TODO
    //return WindowedAggregationLogicalOperator();
    throw UnsupportedOperation();
}

}
