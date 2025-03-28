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
    : WindowOperator(), windowAggregation(std::move(windowAggregation)), windowType(std::move(windowType)), onKey(onKey)
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

/*
bool WindowedAggregationLogicalOperator::inferSchema()
{
    if (!WindowOperator::inferSchema())
    {
        return false;
    }
    // Infer the default input and output schema.
    NES_DEBUG("WindowLogicalOperator: TypeInferencePhase: infer types for window operator with input schema {}",
              inputSchema.toString());

    // Infer type of aggregation.
    auto &aggs = getWindowAggregation();
    for (const auto& agg : aggs)
    {
        agg->inferStamp(inputSchema);
    }

    // Construct output schema: clear first.
    outputSchema.clear();

    // Distinguish processing for different window types (time-based or content-based).
    if (auto* timeWindow = dynamic_cast<Windowing::TimeBasedWindowType*>(&getWindowType()))
    {
        // Type inference for time-based windows.
        if (!timeWindow->inferStamp(inputSchema))
        {
            return false;
        }
        const auto& sourceName = inputSchema.getQualifierNameForSystemGeneratedFields();
        const auto& newQualifierForSystemField = sourceName;

        windowStartFieldName = newQualifierForSystemField + "$start";
        windowEndFieldName   = newQualifierForSystemField + "$end";
        outputSchema.addField(windowStartFieldName, BasicType::UINT64);
        outputSchema.addField(windowEndFieldName, BasicType::UINT64);
    }
    else if (auto* contentBasedWindowType = dynamic_cast<Windowing::ContentBasedWindowType*>(&getWindowType()))
    {
        if (contentBasedWindowType->getContentBasedSubWindowType() ==
            Windowing::ContentBasedWindowType::ContentBasedSubWindowType::THRESHOLDWINDOW)
        {
            auto thresholdWindow = Windowing::ContentBasedWindowType::asThresholdWindow(std::unique_ptr<Windowing::ContentBasedWindowType>(contentBasedWindowType));
            if (!thresholdWindow->inferStamp(inputSchema))
            {
                return false;
            }
        }
    }
    else
    {
        throw CannotInferSchema("Unsupported window type {}", getWindowType().toString());
    }

    if (isKeyed())
    {
        // Infer the data type of each key field.
        auto& keys = getKeys();
        for (const auto& key : keys)
        {
            key->inferStamp(inputSchema);
            outputSchema.addField(AttributeField(key->getFieldName(), key->getStamp().clone()));
        }
    }
    for (const auto& agg : aggs)
    {
        outputSchema.addField(
            AttributeField(dynamic_cast<FieldAccessLogicalFunction*>(&agg->as())->getFieldName(),
                                    agg->as().getStamp().clone()));
    }

    NES_DEBUG("Outputschema for window={}", outputSchema.toString());
    return true;
}
 */

Optimizer::TraitSet WindowedAggregationLogicalOperator::getTraitSet() const
{
    return {};
}

void WindowedAggregationLogicalOperator::setChildren(std::vector<LogicalOperator> children)
{
    this->children = children;
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

void WindowedAggregationLogicalOperator::setOutputOriginIds(std::vector<OriginId> ids)
{
    outputOriginIds = ids;
}

void WindowedAggregationLogicalOperator::setInputOriginIds(std::vector<std::vector<OriginId>> ids)
{
    inputOriginIds = ids[0];
}

std::vector<LogicalOperator> WindowedAggregationLogicalOperator::getChildren() const
{
    return children;
}

bool WindowedAggregationLogicalOperator::isKeyed() const
{
    return !onKey.empty();
}

const std::vector<std::shared_ptr<WindowAggregationLogicalFunction>>& WindowedAggregationLogicalOperator::getWindowAggregation() const
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

const std::vector<FieldAccessLogicalFunction>& WindowedAggregationLogicalOperator::getKeys() const
{
    return onKey;
}

OriginId WindowedAggregationLogicalOperator::getOriginId() const
{
    return originId;
}

void WindowedAggregationLogicalOperator::setInputOriginIds(const std::vector<OriginId>& inputIds)
{
    inputOriginIds = inputIds;
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
