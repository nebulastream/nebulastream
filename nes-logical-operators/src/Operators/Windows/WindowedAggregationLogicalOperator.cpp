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
#include <Operators/Windows/Aggregations/WindowAggregationFunction.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <API/AttributeField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Serialization/SchemaSerializationUtil.hpp>
#include <SerializableOperator.pb.h>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Types/ContentBasedWindowType.hpp>
#include <WindowTypes/Types/ThresholdWindow.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

WindowedAggregationLogicalOperator::WindowedAggregationLogicalOperator(
    const std::vector<std::shared_ptr<FieldAccessLogicalFunction>>& onKey,
    std::vector<std::shared_ptr<Windowing::WindowAggregationFunction>> windowAggregation,
    std::shared_ptr<Windowing::WindowType> windowType)
    : Operator()
    , WindowOperator()
    , windowAggregation(windowAggregation)
    , windowType(windowType)
    , onKey(onKey)
{
}

std::string WindowedAggregationLogicalOperator::toString() const
{
    std::stringstream ss;
    auto wt = getWindowType();
    const auto aggs = getWindowAggregation();
    ss << "WINDOW AGGREGATION(OP-" << id << ", ";
    for (const auto& agg : aggs)
    {
        ss << agg->toString() << ";";
    }
    ss << ") on window type: " << wt->toString();
    return ss.str();
}

bool WindowedAggregationLogicalOperator::isIdentical(const Operator& rhs) const
{
    return *this == rhs &&
           (dynamic_cast<const WindowedAggregationLogicalOperator*>(&rhs)->id == id);
}

bool WindowedAggregationLogicalOperator::operator==(Operator const& rhs) const
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
            if (!this->getWindowAggregation()[i]->equal(rhsOperator->getWindowAggregation()[i]))
            {
                return false;
            }
        }
        return this->windowType->equal(rhsOperator->getWindowType());
    }
    return false;
}

std::shared_ptr<Operator> WindowedAggregationLogicalOperator::clone() const
{
    auto copy = std::make_shared<WindowedAggregationLogicalOperator>(onKey, windowAggregation, windowType);
    copy->setOriginId(originId);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setNumberOfInputEdges(numberOfInputEdges);
    return copy;
}

bool WindowedAggregationLogicalOperator::inferSchema()
{
    if (!WindowOperator::inferSchema())
    {
        return false;
    }
    // Infer the default input and output schema.
    NES_DEBUG("WindowLogicalOperator: TypeInferencePhase: infer types for window operator with input schema {}",
              inputSchema->toString());

    // Infer type of aggregation.
    auto aggs = getWindowAggregation();
    for (const auto& agg : aggs)
    {
        agg->inferStamp(*inputSchema);
    }

    // Construct output schema: clear first.
    outputSchema->clear();

    // Distinguish processing for different window types (time-based or content-based).
    auto wt = getWindowType();
    if (Util::instanceOf<Windowing::TimeBasedWindowType>(wt))
    {
        // Type inference for time-based windows.
        if (!Util::as<Windowing::TimeBasedWindowType>(wt)->inferStamp(*inputSchema))
        {
            return false;
        }
        const auto& sourceName = inputSchema->getQualifierNameForSystemGeneratedFields();
        const auto& newQualifierForSystemField = sourceName;

        windowStartFieldName = newQualifierForSystemField + "$start";
        windowEndFieldName   = newQualifierForSystemField + "$end";
        outputSchema->addField(windowStartFieldName, BasicType::UINT64);
        outputSchema->addField(windowEndFieldName, BasicType::UINT64);
    }
    else if (Util::instanceOf<Windowing::ContentBasedWindowType>(wt))
    {
        // Type inference for content-based windows.
        auto contentBasedWindowType = Util::as<Windowing::ContentBasedWindowType>(wt);
        if (contentBasedWindowType->getContentBasedSubWindowType() ==
            Windowing::ContentBasedWindowType::ContentBasedSubWindowType::THRESHOLDWINDOW)
        {
            auto thresholdWindow = Windowing::ContentBasedWindowType::asThresholdWindow(contentBasedWindowType);
            if (!thresholdWindow->inferStamp(*inputSchema))
            {
                return false;
            }
        }
    }
    else
    {
        throw CannotInferSchema("Unsupported window type {}", getWindowType()->toString());
    }

    if (isKeyed())
    {
        // Infer the data type of each key field.
        auto keys = getKeys();
        for (const auto& key : keys)
        {
            key->inferStamp(*inputSchema);
            outputSchema->addField(AttributeField::create(key->getFieldName(), key->getStamp()));
        }
    }
    for (const auto& agg : aggs)
    {
        outputSchema->addField(
            AttributeField::create(NES::Util::as<FieldAccessLogicalFunction>(agg->as())->getFieldName(),
                                    agg->as()->getStamp()));
    }

    NES_DEBUG("Outputschema for window={}", outputSchema->toString());
    return true;
}

std::vector<std::string> WindowedAggregationLogicalOperator::getGroupByKeyNames() const
{
    std::vector<std::string> groupByKeyNames;
    if (isKeyed())
    {
        auto keys = getKeys();
        groupByKeyNames.reserve(keys.size());
        for (const auto& key : keys)
        {
            groupByKeyNames.push_back(key->getFieldName());
        }
    }
    return groupByKeyNames;
}

bool WindowedAggregationLogicalOperator::isKeyed() const
{
    return !onKey.empty();
}

uint64_t WindowedAggregationLogicalOperator::getNumberOfInputEdges() const
{
    return numberOfInputEdges;
}

void WindowedAggregationLogicalOperator::setNumberOfInputEdges(uint64_t num)
{
    numberOfInputEdges = num;
}

std::vector<std::shared_ptr<Windowing::WindowAggregationFunction>> WindowedAggregationLogicalOperator::getWindowAggregation() const
{
    return windowAggregation;
}

void WindowedAggregationLogicalOperator::setWindowAggregation(
    const std::vector<std::shared_ptr<Windowing::WindowAggregationFunction>>& wa)
{
    windowAggregation = wa;
}

std::shared_ptr<Windowing::WindowType> WindowedAggregationLogicalOperator::getWindowType() const
{
    return windowType;
}

void WindowedAggregationLogicalOperator::setWindowType(std::shared_ptr<Windowing::WindowType> wt)
{
    windowType = wt;
}

std::vector<std::shared_ptr<FieldAccessLogicalFunction>> WindowedAggregationLogicalOperator::getKeys() const
{
    return onKey;
}

void WindowedAggregationLogicalOperator::setOnKey(
    const std::vector<std::shared_ptr<FieldAccessLogicalFunction>>& keys)
{
    onKey = keys;
}

uint64_t WindowedAggregationLogicalOperator::getAllowedLateness() const
{
    return 0;
}

OriginId WindowedAggregationLogicalOperator::getOriginId() const
{
    return originId;
}

const std::vector<OriginId>& WindowedAggregationLogicalOperator::getInputOriginIds() const
{
    return inputOriginIds;
}

void WindowedAggregationLogicalOperator::setInputOriginIds(const std::vector<OriginId>& inputIds)
{
    inputOriginIds = inputIds;
}


SerializableOperator WindowedAggregationLogicalOperator::serialize() const
{
    SerializableOperator serializedOperator;

    auto* opDesc = new SerializableOperator_LogicalOperator();
    opDesc->set_operatortype(NAME);
    serializedOperator.set_operatorid(this->id.getRawValue());
    serializedOperator.add_childrenids(children[0]->id.getRawValue());

    auto* unaryOpDesc = new SerializableOperator_UnaryLogicalOperator();
    auto* inputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->getInputSchema(), inputSchema);
    unaryOpDesc->set_allocated_inputschema(inputSchema);

    for (const auto& originId : this->getInputOriginIds()) {
        unaryOpDesc->add_originids(originId.getRawValue());
    }

    opDesc->set_allocated_unaryoperator(unaryOpDesc);
    auto* outputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->outputSchema, outputSchema);
    serializedOperator.set_allocated_outputschema(outputSchema);
    serializedOperator.set_allocated_operator_(opDesc);

    return serializedOperator;
}

std::unique_ptr<LogicalOperator>
LogicalOperatorGeneratedRegistrar::RegisterWindowedAggregationLogicalOperator(NES::LogicalOperatorRegistryArguments)
{
    /// TODO
    return nullptr;
}

}
