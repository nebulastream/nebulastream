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

#include <vector>
#include <API/Schema.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Functions/FunctionSerializationUtil.hpp>
#include <Operators/Serialization/OperatorSerializationUtil.hpp>
#include <Operators/Serialization/SchemaSerializationUtil.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/Operator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <google/protobuf/json/json.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include "Operators/EventTimeWatermarkAssignerLogicalOperator.hpp"
#include "Operators/UnionLogicalOperator.hpp"

namespace NES
{

SerializableOperator OperatorSerializationUtil::serializeOperator(const std::shared_ptr<LogicalOperator>& operatorNode)
{
    NES_TRACE("OperatorSerializationUtil:: serialize operator {}", *operatorNode);
    auto serializedOperator = SerializableOperator();
    if (NES::Util::instanceOf<SourceDescriptorLogicalOperator>(operatorNode))
    {
        /// serialize source operator
        serializeSourceOperator(*NES::Util::as<SourceDescriptorLogicalOperator>(operatorNode), serializedOperator);
    }
    else if (NES::Util::instanceOf<SinkLogicalOperator>(operatorNode))
    {
        /// serialize sink operator
        serializeSinkOperator(*NES::Util::as<SinkLogicalOperator>(operatorNode), serializedOperator);
    }
    else if (NES::Util::instanceOf<SelectionLogicalOperator>(operatorNode))
    {
        serializeSelectionOperator(*NES::Util::as<SelectionLogicalOperator>(operatorNode), serializedOperator);
    }
    else if (NES::Util::instanceOf<ProjectionLogicalOperator>(operatorNode))
    {
        /// serialize projection operator
        serializeProjectionOperator(*NES::Util::as<ProjectionLogicalOperator>(operatorNode), serializedOperator);
    }
    else if (NES::Util::instanceOf<UnionLogicalOperator>(operatorNode))
    {
        /// serialize union operator
        NES_TRACE("OperatorSerializationUtil:: serialize to UnionLogicalOperator");
        const auto unionDetails = SerializableOperator_UnionDetails();
        serializedOperator.mutable_details()->PackFrom(unionDetails);
    }
    else if (NES::Util::instanceOf<MapLogicalOperator>(operatorNode))
    {
        /// serialize map operator
        serializeMapOperator(*NES::Util::as<MapLogicalOperator>(operatorNode), serializedOperator);
    }
    else if (NES::Util::instanceOf<InferModel::InferModelLogicalOperator>(operatorNode))
    {
        /// serialize infer model
        serializeInferModelOperator(*NES::Util::as<InferModel::InferModelLogicalOperator>(operatorNode), serializedOperator);
    }
    else if (NES::Util::instanceOf<WindowLogicalOperator>(operatorNode))
    {
        /// serialize window operator
        serializeWindowOperator(*NES::Util::as<WindowLogicalOperator>(operatorNode), serializedOperator);
    }
    else if (NES::Util::instanceOf<JoinLogicalOperator>(operatorNode))
    {
        /// serialize streaming join operator
        serializeJoinOperator(*NES::Util::as<JoinLogicalOperator>(operatorNode), serializedOperator);
    }
    else if (NES::Util::instanceOf<WatermarkAssignerLogicalOperator>(operatorNode))
    {
        /// serialize watermarkAssigner operator
        serializeWatermarkAssignerOperator(*NES::Util::as<WatermarkAssignerLogicalOperator>(operatorNode), serializedOperator);
    }
    else
    {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not serialize this operator: {}", *operatorNode);
    }

    /// serialize input schema
    serializeInputSchema(operatorNode, serializedOperator);

    /// serialize output schema
    SchemaSerializationUtil::serializeSchema(operatorNode->getOutputSchema(), serializedOperator.mutable_outputschema());

    /// serialize operator id
    serializedOperator.set_operatorid(operatorNode->getId().getRawValue());

    /// serialize and append children if the node has any
    for (const auto& child : operatorNode->getChildren())
    {
        serializedOperator.add_childrenids(NES::Util::as<Operator>(child)->getId().getRawValue());
    }

    /// serialize and append origin id
    if (NES::Util::instanceOf<BinaryLogicalOperator>(operatorNode))
    {
        const auto binaryOperator = NES::Util::as<BinaryLogicalOperator>(operatorNode);
        for (const auto& originId : binaryOperator->getLeftInputOriginIds())
        {
            serializedOperator.add_leftoriginids(originId.getRawValue());
        }
        for (const auto& originId : binaryOperator->getRightInputOriginIds())
        {
            serializedOperator.add_rightoriginids(originId.getRawValue());
        }
    }
    else
    {
        const auto unaryOperator = NES::Util::as<UnaryLogicalOperator>(operatorNode);
        for (const auto& originId : unaryOperator->getInputOriginIds())
        {
            serializedOperator.add_originids(originId.getRawValue());
        }
    }

    NES_TRACE("OperatorSerializationUtil:: serialize {} to {}", *operatorNode, serializedOperator.details().type_url());
    return serializedOperator;
}

void OperatorSerializationUtil::deserializeInputSchema(
    std::shared_ptr<LogicalOperator> operatorNode, const SerializableOperator& serializedOperator)
{
    /// de-serialize operator input schema
    if (!NES::Util::instanceOf<BinaryLogicalOperator>(operatorNode))
    {
        NES::Util::as<UnaryLogicalOperator>(operatorNode)
            ->setInputSchema(SchemaSerializationUtil::deserializeSchema(serializedOperator.inputschema()));
    }
    else
    {
        const auto binaryOperator = NES::Util::as<BinaryLogicalOperator>(operatorNode);
        binaryOperator->setLeftInputSchema(SchemaSerializationUtil::deserializeSchema(serializedOperator.leftinputschema()));
        binaryOperator->setRightInputSchema(SchemaSerializationUtil::deserializeSchema(serializedOperator.rightinputschema()));
    }
}

std::shared_ptr<UnaryLogicalOperator> deserializeInferModelOperator(const SerializableOperator_InferModelDetails& inferModelDetails)
{
    std::vector<std::shared_ptr<LogicalFunction>> inputFields;
    std::vector<std::shared_ptr<LogicalFunction>> outputFields;

    for (const auto& serializedInputField : inferModelDetails.inputfields())
    {
        auto inputField = FunctionSerializationUtil::deserializeFunction(serializedInputField);
        inputFields.push_back(inputField);
    }
    for (const auto& serializedOutputField : inferModelDetails.outputfields())
    {
        auto outputField = FunctionSerializationUtil::deserializeFunction(serializedOutputField);
        outputFields.push_back(outputField);
    }

    const auto& content = inferModelDetails.mlfilecontent();
    std::ofstream output(inferModelDetails.mlfilename(), std::ios::binary);
    output << content;
    output.close();

    return std::make_shared<InferModel::InferModelLogicalOperator>(
        inferModelDetails.mlfilename(), inputFields, outputFields, getNextOperatorId());
}

std::shared_ptr<Windowing::WatermarkStrategyDescriptor>
deserializeWatermarkStrategyDescriptor(const SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails)
{
    NES_TRACE("OperatorSerializationUtil:: de-serialize watermark strategy ");
    const auto& deserializedWatermarkStrategyDescriptor = watermarkStrategyDetails.strategy();
    if (deserializedWatermarkStrategyDescriptor
            .Is<SerializableOperator_WatermarkStrategyDetails_SerializableEventTimeWatermarkStrategyDescriptor>())
    {
        /// de-serialize print sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized WatermarkStrategy as EventTimeWatermarkStrategyDescriptor");
        auto serializedEventTimeWatermarkStrategyDescriptor
            = SerializableOperator_WatermarkStrategyDetails_SerializableEventTimeWatermarkStrategyDescriptor();
        deserializedWatermarkStrategyDescriptor.UnpackTo(&serializedEventTimeWatermarkStrategyDescriptor);

        const auto onField = Util::as<FieldAccessLogicalFunction>(
            FunctionSerializationUtil::deserializeFunction(serializedEventTimeWatermarkStrategyDescriptor.onfield()));
        NES_DEBUG("OperatorSerializationUtil:: deserialized field name {}", onField->getFieldName());
        auto eventTimeWatermarkStrategyDescriptor = Windowing::EventTimeWatermarkStrategyDescriptor::create(
            FieldAccessLogicalFunction::create(onField->getFieldName()),
            Windowing::TimeUnit(serializedEventTimeWatermarkStrategyDescriptor.multiplier()));
        return eventTimeWatermarkStrategyDescriptor;
    }
    if (deserializedWatermarkStrategyDescriptor
            .Is<SerializableOperator_WatermarkStrategyDetails_SerializableIngestionTimeWatermarkStrategyDescriptor>())
    {
        return Windowing::IngestionTimeWatermarkStrategyDescriptor::create();
    }
    else
    {
        NES_ERROR("OperatorSerializationUtil: Unknown Serialized Watermark Strategy Descriptor Type");
        throw std::invalid_argument("Unknown Serialized Watermark Strategy Descriptor Type");
    }
}

std::shared_ptr<UnaryLogicalOperator>
deserializeWatermarkAssignerOperator(const SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails)
{
    const auto watermarkStrategyDescriptor = deserializeWatermarkStrategyDescriptor(watermarkStrategyDetails);
    return std::make_shared<WatermarkAssignerLogicalOperator>(watermarkStrategyDescriptor, getNextOperatorId());
}

std::shared_ptr<UnaryLogicalOperator>
deserializeWindowOperator(const SerializableOperator_WindowDetails& windowDetails, OperatorId operatorId)
{
    const auto& serializedWindowAggregations = windowDetails.windowaggregations();
    const auto& serializedWindowType = windowDetails.windowtype();

    std::vector<std::shared_ptr<Windowing::WindowAggregationFunction>> aggregation;
    for (const auto& serializedWindowAggregation : serializedWindowAggregations)
    {
        auto onField
            = Util::as<FieldAccessLogicalFunction>(FunctionSerializationUtil::deserializeFunction(serializedWindowAggregation.onfield()));
        auto asField
            = Util::as<FieldAccessLogicalFunction>(FunctionSerializationUtil::deserializeFunction(serializedWindowAggregation.asfield()));
        if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_SUM)
        {
            aggregation.emplace_back(Windowing::SumAggregationFunction::create(onField, asField));
        }
        else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_MAX)
        {
            aggregation.emplace_back(Windowing::MaxAggregationFunction::create(onField, asField));
        }
        else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_MIN)
        {
            aggregation.emplace_back(Windowing::MinAggregationFunction::create(onField, asField));
        }
        else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_COUNT)
        {
            aggregation.emplace_back(Windowing::CountAggregationFunction::create(onField, asField));
        }
        else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_AVG)
        {
            aggregation.emplace_back(Windowing::AvgAggregationFunction::create(onField, asField));
        }
        else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_MEDIAN)
        {
            aggregation.emplace_back(Windowing::MedianAggregationFunction::create(onField, asField));
        }
        else
        {
            NES_FATAL_ERROR(
                "OperatorSerializationUtil: could not de-serialize window aggregation: {}", serializedWindowAggregation.DebugString());
        }
    }

    std::shared_ptr<Windowing::WindowType> window;
    if (serializedWindowType.Is<SerializableOperator_TumblingWindow>())
    {
        auto serializedTumblingWindow = SerializableOperator_TumblingWindow();
        serializedWindowType.UnpackTo(&serializedTumblingWindow);
        auto serializedTimeCharacteristic = serializedTumblingWindow.timecharacteristic();
        auto multiplier = serializedTimeCharacteristic.multiplier();
        if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_EventTime)
        {
            auto field = FieldAccessLogicalFunction::create(serializedTimeCharacteristic.field());
            window = Windowing::TumblingWindow::of(
                Windowing::TimeCharacteristic::createEventTime(field, Windowing::TimeUnit(multiplier)),
                Windowing::TimeMeasure(serializedTumblingWindow.size()));
        }
        else if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_IngestionTime)
        {
            window = Windowing::TumblingWindow::of(
                Windowing::TimeCharacteristic::createIngestionTime(), Windowing::TimeMeasure(serializedTumblingWindow.size()));
        }
        else
        {
            NES_FATAL_ERROR(
                "OperatorSerializationUtil: could not de-serialize window time characteristic: {}",
                serializedTimeCharacteristic.DebugString());
        }
    }
    else if (serializedWindowType.Is<SerializableOperator_SlidingWindow>())
    {
        auto serializedSlidingWindow = SerializableOperator_SlidingWindow();
        serializedWindowType.UnpackTo(&serializedSlidingWindow);
        auto serializedTimeCharacteristic = serializedSlidingWindow.timecharacteristic();
        auto multiplier = serializedTimeCharacteristic.multiplier();
        if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_EventTime)
        {
            auto field = FieldAccessLogicalFunction::create(serializedTimeCharacteristic.field());
            window = Windowing::SlidingWindow::of(
                Windowing::TimeCharacteristic::createEventTime(field, Windowing::TimeUnit(multiplier)),
                Windowing::TimeMeasure(serializedSlidingWindow.size()),
                Windowing::TimeMeasure(serializedSlidingWindow.slide()));
        }
        else if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_IngestionTime)
        {
            window = Windowing::SlidingWindow::of(
                Windowing::TimeCharacteristic::createIngestionTime(),
                Windowing::TimeMeasure(serializedSlidingWindow.size()),
                Windowing::TimeMeasure(serializedSlidingWindow.slide()));
        }
        else
        {
            NES_FATAL_ERROR(
                "OperatorSerializationUtil: could not de-serialize window time characteristic: {}",
                serializedTimeCharacteristic.DebugString());
        }
    }
    else if (serializedWindowType.Is<SerializableOperator_ThresholdWindow>())
    {
        auto serializedThresholdWindow = SerializableOperator_ThresholdWindow();
        serializedWindowType.UnpackTo(&serializedThresholdWindow);
        auto thresholdFunction = FunctionSerializationUtil::deserializeFunction(serializedThresholdWindow.predicate());
        auto thresholdMinimumCount = serializedThresholdWindow.minimumcount();
        window = Windowing::ThresholdWindow::of(thresholdFunction, thresholdMinimumCount);
    }
    else
    {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window type: {}", serializedWindowType.DebugString());
    }

    std::vector<std::shared_ptr<FieldAccessLogicalFunction>> keyAccessFunction;
    for (auto& key : windowDetails.keys())
    {
        keyAccessFunction.emplace_back(Util::as<FieldAccessLogicalFunction>(FunctionSerializationUtil::deserializeFunction(key)));
    }
    auto windowDef = Windowing::LogicalWindowDescriptor::create(keyAccessFunction, aggregation, window);
    windowDef->setOriginId(OriginId(windowDetails.originid()));
    auto windowOperator = std::make_shared<WindowLogicalOperator>(windowDef, operatorId);
    windowOperator->windowMetaData.windowStartFieldName = windowDetails.windowstartfieldname();
    windowOperator->windowMetaData.windowEndFieldName = windowDetails.windowendfieldname();
    return windowOperator;
}


std::shared_ptr<JoinLogicalOperator> deserializeJoinOperator(const SerializableOperator_JoinDetails& joinDetails, OperatorId operatorId)
{
    const auto& serializedWindowType = joinDetails.windowtype();
    const auto& serializedJoinType = joinDetails.jointype();
    /// check which jointype is set
    /// default: JoinType::INNER_JOIN
    Join::LogicalJoinDescriptor::JoinType joinType = Join::LogicalJoinDescriptor::JoinType::INNER_JOIN;
    /// with Cartesian Product is set, change join type
    if (serializedJoinType.jointype() == SerializableOperator_JoinDetails_JoinTypeCharacteristic_JoinType_CARTESIAN_PRODUCT)
    {
        joinType = Join::LogicalJoinDescriptor::JoinType::CARTESIAN_PRODUCT;
    }

    std::shared_ptr<Windowing::WindowType> window;
    if (serializedWindowType.Is<SerializableOperator_TumblingWindow>())
    {
        auto serializedTumblingWindow = SerializableOperator_TumblingWindow();
        serializedWindowType.UnpackTo(&serializedTumblingWindow);
        const auto& serializedTimeCharacteristic = serializedTumblingWindow.timecharacteristic();
        const auto multiplier = serializedTimeCharacteristic.multiplier();
        if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_EventTime)
        {
            const auto field = FieldAccessLogicalFunction::create(serializedTimeCharacteristic.field());
            window = Windowing::TumblingWindow::of(
                Windowing::TimeCharacteristic::createEventTime(field, Windowing::TimeUnit(multiplier)),
                Windowing::TimeMeasure(serializedTumblingWindow.size()));
        }
        else if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_IngestionTime)
        {
            window = Windowing::TumblingWindow::of(
                Windowing::TimeCharacteristic::createIngestionTime(), Windowing::TimeMeasure(serializedTumblingWindow.size()));
        }
        else
        {
            NES_FATAL_ERROR(
                "OperatorSerializationUtil: could not de-serialize window time characteristic: {}",
                serializedTimeCharacteristic.DebugString());
        }
    }
    else if (serializedWindowType.Is<SerializableOperator_SlidingWindow>())
    {
        auto serializedSlidingWindow = SerializableOperator_SlidingWindow();
        serializedWindowType.UnpackTo(&serializedSlidingWindow);
        const auto& serializedTimeCharacteristic = serializedSlidingWindow.timecharacteristic();
        const auto multiplier = serializedTimeCharacteristic.multiplier();
        if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_EventTime)
        {
            const auto field = FieldAccessLogicalFunction::create(serializedTimeCharacteristic.field());
            window = Windowing::SlidingWindow::of(
                Windowing::TimeCharacteristic::createEventTime(field, Windowing::TimeUnit(multiplier)),
                Windowing::TimeMeasure(serializedSlidingWindow.size()),
                Windowing::TimeMeasure(serializedSlidingWindow.slide()));
        }
        else if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_IngestionTime)
        {
            window = Windowing::SlidingWindow::of(
                Windowing::TimeCharacteristic::createIngestionTime(),
                Windowing::TimeMeasure(serializedSlidingWindow.size()),
                Windowing::TimeMeasure(serializedSlidingWindow.slide()));
        }
        else
        {
            NES_FATAL_ERROR(
                "OperatorSerializationUtil: could not de-serialize window time characteristic: {}",
                serializedTimeCharacteristic.DebugString());
        }
    }
    else
    {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window type: {}", serializedWindowType.DebugString());
    }

    std::shared_ptr<LogicalOperator> ptr;
    const auto& serializedJoinFunction = joinDetails.joinfunction();
    const auto joinFunction = Util::as<LogicalFunction>(FunctionSerializationUtil::deserializeFunction(serializedJoinFunction));

    const auto joinDefinition = Join::LogicalJoinDescriptor::create(
        joinFunction, window, joinDetails.numberofinputedgesleft(), joinDetails.numberofinputedgesright(), joinType);
    auto joinOperator = std::make_shared<JoinLogicalOperator>(joinDefinition, operatorId);
    joinOperator->windowMetaData.windowStartFieldName = joinDetails.windowstartfieldname();
    joinOperator->windowMetaData.windowEndFieldName = joinDetails.windowendfieldname();
    joinOperator->setOriginId(OriginId(joinDetails.origin()));
    return joinOperator;
}
* / std::shared_ptr<LogicalOperator> OperatorSerializationUtil::deserializeOperator(SerializableOperator serializedOperator)
{
    if (serializedOperator.has_source())
    {
        const auto& sourceDescriptor = serializedOperator.source();
        return deserializeSourceOperator(sourceDescriptor);
    }
    else if (serializedOperator.has_sink())
    {
        const auto& sink = serializedOperator.sink();
        return deserializeSinkOperator(sink);
    }
    else if (serializedOperator.has_operator_())
    {
        const auto& operator_ = serializedOperator.operator_();
        auto operatorArguments = NES::LogicalOperatorRegistryArguments();
        if (auto logicalOperator = LogicalOperatorRegistry::instance().create(operator_.operatortype(), operatorArguments);
            logicalOperator.has_value())
        {
            return std::move(logicalOperator.value());
        }
    }
    throw CannotDeserialize("could not de-serialize this serialized operator: {}", serializedOperator.DebugString());

    /*
    /// deserialize input schema
    deserializeInputSchema(operatorNode, serializedOperator);

    if (details.Is<SerializableOperator_JoinDetails>())
    {
        auto joinOp = NES::Util::as<JoinLogicalOperator>(operatorNode);
        joinOp->getJoinDefinition()->updateSourceTypes(joinOp->getLeftInputSchema(), joinOp->getRightInputSchema());
        joinOp->getJoinDefinition()->setOutputSchema(joinOp->getOutputSchema());
    }

    /// de-serialize and append origin id
    if (NES::Util::instanceOf<BinaryLogicalOperator>(operatorNode))
    {
        auto binaryOperator = NES::Util::as<BinaryLogicalOperator>(operatorNode);
        std::vector<OriginId> leftOriginIds;
        for (const auto& originId : serializedOperator.leftoriginids())
        {
            leftOriginIds.emplace_back(originId);
        }
        binaryOperator->setLeftInputOriginIds(leftOriginIds);
        std::vector<OriginId> rightOriginIds;
        for (const auto& originId : serializedOperator.rightoriginids())
        {
            rightOriginIds.emplace_back(originId);
        }
        binaryOperator->setRightInputOriginIds(rightOriginIds);
    }
    else
    {
        auto unaryOperator = NES::Util::as<UnaryLogicalOperator>(operatorNode);
        std::vector<OriginId> originIds;
        for (const auto& originId : serializedOperator.originids())
        {
            originIds.emplace_back(originId);
        }
        unaryOperator->setInputOriginIds(originIds);
    }
    NES_TRACE("OperatorSerializationUtil:: de-serialize {} to {}", serializedOperator.DebugString(), *operatorNode);
    return operatorNode;
*/
}

void OperatorSerializationUtil::serializeSourceOperator(
    SourceDescriptorLogicalOperator& sourceOperator, SerializableOperator& serializedOperator)
{
    auto* sourceDetails = new SerializableOperator_SourceDescriptorLogicalOperator();
    const auto& sourceDescriptor = sourceOperator.getSourceDescriptorRef();
    serializeSourceDescriptor(sourceDescriptor, *sourceDetails);
    sourceDetails->set_sourceoriginid(sourceOperator.getOriginId().getRawValue());

    serializedOperator.set_allocated_source(sourceDetails);
}

std::shared_ptr<UnaryLogicalOperator>
OperatorSerializationUtil::deserializeSourceOperator(const SerializableOperator_SourceDescriptorLogicalOperator& sourceDetails)
{
    const auto& serializedSourceDescriptor = sourceDetails.sourcedescriptor();
    auto sourceDescriptor = deserializeSourceDescriptor(serializedSourceDescriptor);
    const auto sourceId = sourceDetails.sourceoriginid();
    return std::make_shared<SourceDescriptorLogicalOperator>(std::move(sourceDescriptor), getNextOperatorId(), OriginId(sourceId));
}

void OperatorSerializationUtil::serializeSinkOperator(const SinkLogicalOperator& sinkOperator, SerializableOperator& serializedOperator)
{
    auto* sinkDetails = new SerializableOperator_SinkLogicalOperator();
    const auto& sinkDescriptor = sinkOperator.getSinkDescriptorRef();
    serializeSinkDescriptor(sinkOperator.getOutputSchema(), sinkDescriptor, *sinkDetails);

    serializedOperator.set_allocated_sink(sinkDetails);
}

std::shared_ptr<UnaryLogicalOperator>
OperatorSerializationUtil::deserializeSinkOperator(const SerializableOperator_SinkLogicalOperator& sinkDetails)
{
    const auto& serializedSinkDescriptor = sinkDetails.sinkdescriptor();
    auto sinkDescriptor = deserializeSinkDescriptor(serializedSinkDescriptor);
    auto sinkOperator = std::make_shared<SinkLogicalOperator>(getNextOperatorId());
    sinkOperator->sinkDescriptor = std::move(sinkDescriptor);
    return sinkOperator;
}

/*
void OperatorSerializationUtil::serializeWindowOperator(const WindowOperator& windowOperator, SerializableOperator& serializedOperator)
{
    NES_TRACE("OperatorSerializationUtil:: serialize to WindowOperator");
    auto windowDetails = SerializableOperator_WindowDetails();
    auto windowDefinition = windowOperator.getWindowDefinition();

    if (windowDefinition->isKeyed())
    {
        for (const auto& key : windowDefinition->getKeys())
        {
            auto function = windowDetails.mutable_keys()->Add();
            FunctionSerializationUtil::serializeFunction(key, function);
        }
    }
    windowDetails.set_originid(windowOperator.getOriginId().getRawValue());
    auto windowType = windowDefinition->getWindowType();

    if (Util::instanceOf<Windowing::TimeBasedWindowType>(windowType))
    {
        auto timeBasedWindowType = Util::as<Windowing::TimeBasedWindowType>(windowType);
        auto timeCharacteristic = timeBasedWindowType->getTimeCharacteristic();
        auto timeCharacteristicDetails = SerializableOperator_TimeCharacteristic();
        if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::Type::EventTime)
        {
            timeCharacteristicDetails.set_type(SerializableOperator_TimeCharacteristic_Type_EventTime);
            timeCharacteristicDetails.set_field(timeCharacteristic->field->getName());
        }
        else if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::Type::IngestionTime)
        {
            timeCharacteristicDetails.set_type(SerializableOperator_TimeCharacteristic_Type_IngestionTime);
        }
        else
        {
            NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Characteristic");
        }
        timeCharacteristicDetails.set_multiplier(timeCharacteristic->getTimeUnit().getMillisecondsConversionMultiplier());

        if (Util::instanceOf<Windowing::TumblingWindow>(windowType))
        {
            auto tumblingWindow = Util::as<Windowing::TumblingWindow>(windowType);
            auto tumblingWindowDetails = SerializableOperator_TumblingWindow();
            tumblingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
            tumblingWindowDetails.set_size(tumblingWindow->getSize().getTime());
            windowDetails.mutable_windowtype()->PackFrom(tumblingWindowDetails);
        }
        else if (Util::instanceOf<Windowing::SlidingWindow>(windowType))
        {
            auto slidingWindow = Util::as<Windowing::SlidingWindow>(windowType);
            auto slidingWindowDetails = SerializableOperator_SlidingWindow();
            slidingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
            slidingWindowDetails.set_size(slidingWindow->getSize().getTime());
            slidingWindowDetails.set_slide(slidingWindow->getSlide().getTime());
            windowDetails.mutable_windowtype()->PackFrom(slidingWindowDetails);
        }
        else
        {
            NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Type");
        }
    }
    else if (Util::instanceOf<Windowing::ContentBasedWindowType>(windowType))
    {
        auto contentBasedWindowType = Util::as<Windowing::ContentBasedWindowType>(windowType);
        if (contentBasedWindowType->getContentBasedSubWindowType()
            == Windowing::ContentBasedWindowType::ContentBasedSubWindowType::THRESHOLDWINDOW)
        {
            auto thresholdWindow = std::dynamic_pointer_cast<Windowing::ThresholdWindow>(windowType);
            auto thresholdWindowDetails = SerializableOperator_ThresholdWindow();
            FunctionSerializationUtil::serializeFunction(thresholdWindow->getPredicate(), thresholdWindowDetails.mutable_predicate());
            thresholdWindowDetails.set_minimumcount(thresholdWindow->getMinimumCount());
            windowDetails.mutable_windowtype()->PackFrom(thresholdWindowDetails);
        }
        else
        {
            NES_ERROR("OperatorSerializationUtil: Cant serialize this content based window Type");
        }
    }

    /// serialize aggregation
    for (auto aggregation : windowDefinition->getWindowAggregation())
    {
        auto* windowAggregation = windowDetails.mutable_windowaggregations()->Add();
        FunctionSerializationUtil::serializeFunction(aggregation->as(), windowAggregation->mutable_asfield());
        FunctionSerializationUtil::serializeFunction(aggregation->on(), windowAggregation->mutable_onfield());

        using enum Windowing::WindowAggregationFunction::Type;
        switch (aggregation->getType())
        {
            case Count:
                windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_COUNT);
                break;
            case Max:
                windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_MAX);
                break;
            case Min:
                windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_MIN);
                break;
            case Sum:
                windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_SUM);
                break;
            case Avg:
                windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_AVG);
                break;
            case Median:
                windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_MEDIAN);
                break;
            default:
                NES_FATAL_ERROR("OperatorSerializationUtil: could not cast aggregation type");
        }
    }

    windowDetails.set_windowstartfieldname(windowOperator.windowMetaData.windowStartFieldName);
    windowDetails.set_windowendfieldname(windowOperator.windowMetaData.windowEndFieldName);


    serializedOperator.mutable_details()->PackFrom(windowDetails);
}
*/
/*
void OperatorSerializationUtil::serializeJoinOperator(const JoinLogicalOperator& joinOperator, SerializableOperator& serializedOperator)
{
    NES_TRACE("OperatorSerializationUtil:: serialize to JoinLogicalOperator");
    auto joinDetails = SerializableOperator_JoinDetails();
    const auto joinDefinition = joinOperator.getJoinDefinition();

    FunctionSerializationUtil::serializeFunction(joinDefinition->getJoinFunction(), joinDetails.mutable_joinfunction());

    const auto windowType = joinDefinition->getWindowType();
    const auto timeBasedWindowType = Util::as<Windowing::TimeBasedWindowType>(windowType);
    const auto timeCharacteristic = timeBasedWindowType->getTimeCharacteristic();
    auto timeCharacteristicDetails = SerializableOperator_TimeCharacteristic();
    timeCharacteristicDetails.set_multiplier(timeCharacteristic->getTimeUnit().getMillisecondsConversionMultiplier());
    if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::Type::EventTime)
    {
        timeCharacteristicDetails.set_type(SerializableOperator_TimeCharacteristic_Type_EventTime);
        timeCharacteristicDetails.set_field(timeCharacteristic->field->getName());
    }
    else if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::Type::IngestionTime)
    {
        timeCharacteristicDetails.set_type(SerializableOperator_TimeCharacteristic_Type_IngestionTime);
    }
    else
    {
        NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Characteristic");
    }
    if (Util::instanceOf<Windowing::TumblingWindow>(windowType))
    {
        const auto tumblingWindow = Util::as<Windowing::TumblingWindow>(windowType);
        auto tumblingWindowDetails = SerializableOperator_TumblingWindow();
        tumblingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
        tumblingWindowDetails.set_size(tumblingWindow->getSize().getTime());
        joinDetails.mutable_windowtype()->PackFrom(tumblingWindowDetails);
    }
    else if (Util::instanceOf<Windowing::SlidingWindow>(windowType))
    {
        const auto slidingWindow = Util::as<Windowing::SlidingWindow>(windowType);
        auto slidingWindowDetails = SerializableOperator_SlidingWindow();
        slidingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
        slidingWindowDetails.set_size(slidingWindow->getSize().getTime());
        slidingWindowDetails.set_slide(slidingWindow->getSlide().getTime());
        joinDetails.mutable_windowtype()->PackFrom(slidingWindowDetails);
    }
    else
    {
        NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Type");
    }

    joinDetails.set_numberofinputedgesleft(joinDefinition->getNumberOfInputEdgesLeft());
    joinDetails.set_numberofinputedgesright(joinDefinition->getNumberOfInputEdgesRight());
    joinDetails.set_windowstartfieldname(joinOperator.windowMetaData.windowStartFieldName);
    joinDetails.set_windowendfieldname(joinOperator.windowMetaData.windowEndFieldName);
    joinDetails.set_origin(joinOperator.getOutputOriginIds()[0].getRawValue());

    if (joinDefinition->getJoinType() == Join::LogicalJoinDescriptor::JoinType::INNER_JOIN)
    {
        joinDetails.mutable_jointype()->set_jointype(SerializableOperator_JoinDetails_JoinTypeCharacteristic_JoinType_INNER_JOIN);
    }
    else if (joinDefinition->getJoinType() == Join::LogicalJoinDescriptor::JoinType::CARTESIAN_PRODUCT)
    {
        joinDetails.mutable_jointype()->set_jointype(SerializableOperator_JoinDetails_JoinTypeCharacteristic_JoinType_CARTESIAN_PRODUCT);
    }

    serializedOperator.mutable_details()->PackFrom(joinDetails);
}
*/

void OperatorSerializationUtil::serializeSourceDescriptor(
    const Sources::SourceDescriptor& sourceDescriptor, SerializableOperator_SourceDescriptorLogicalOperator& sourceDetails)
{
    const auto serializedSourceDescriptor
        = SerializableOperator_SourceDescriptorLogicalOperator_SourceDescriptor().New(); /// cleaned up by protobuf

    SchemaSerializationUtil::serializeSchema(sourceDescriptor.schema, serializedSourceDescriptor->mutable_sourceschema());
    serializedSourceDescriptor->set_logicalsourcename(sourceDescriptor.logicalSourceName);
    serializedSourceDescriptor->set_sourcetype(sourceDescriptor.sourceType);

    /// Serialize parser config.
    auto* const serializedParserConfig = ParserConfig().New();
    serializedParserConfig->set_type(sourceDescriptor.parserConfig.parserType);
    serializedParserConfig->set_tupledelimiter(sourceDescriptor.parserConfig.tupleDelimiter);
    serializedParserConfig->set_fielddelimiter(sourceDescriptor.parserConfig.fieldDelimiter);
    serializedSourceDescriptor->set_allocated_parserconfig(serializedParserConfig);

    /// Iterate over SourceDescriptor config and serialize all key-value pairs.
    for (const auto& [key, value] : sourceDescriptor.config)
    {
        auto* kv = serializedSourceDescriptor->mutable_config();
        kv->emplace(key, descriptorConfigTypeToProto(value));
    }
    sourceDetails.set_allocated_sourcedescriptor(serializedSourceDescriptor);
}

Configurations::DescriptorConfig::ConfigType protoToDescriptorConfigType(const SerializableVariantDescriptor& proto_var)
{
    switch (proto_var.value_case())
    {
        case SerializableVariantDescriptor::kIntValue:
            return proto_var.int_value();
        case SerializableVariantDescriptor::kUintValue:
            return proto_var.uint_value();
        case SerializableVariantDescriptor::kBoolValue:
            return proto_var.bool_value();
        case SerializableVariantDescriptor::kCharValue:
            return static_cast<char>(proto_var.char_value()); /// Convert (fixed32) ascii number to char.
        case SerializableVariantDescriptor::kFloatValue:
            return proto_var.float_value();
        case SerializableVariantDescriptor::kDoubleValue:
            return proto_var.double_value();
        case SerializableVariantDescriptor::kStringValue:
            return proto_var.string_value();
        case SerializableVariantDescriptor::kEnumValue:
            return Configurations::EnumWrapper(proto_var.enum_value().value());
        default:
            std::string protoVarAsJson;
            /// Log proto variable as json, in exception, if possible.
            if (const auto conversionResult = google::protobuf::json::MessageToJsonString(proto_var, &protoVarAsJson);
                conversionResult.ok())
            {
                throw CannotSerialize(fmt::format("Unknown variant type: {}", protoVarAsJson));
            }
            throw CannotSerialize("Unknown variant type.");
    }
}

std::unique_ptr<LogicalOperator>
OperatorSerializationUtil::deserializeLogicalOperator(const SerializableOperator_LogicalOperator& operatorDescriptor)
{
    auto operatorType = operatorDescriptor.operatortype();

    /// Deserialize config. Convert from protobuf variant to ConfigType.
    Configurations::DescriptorConfig::Config operatorDescriptorConfig{};
    for (const auto& [key, value] : operatorDescriptor.config())
    {
        operatorDescriptorConfig[key] = protoToDescriptorConfigType(value);
    }

    auto operatorArguments = LogicalOperatorRegistryArguments(operatorDescriptorConfig);
    if (auto logicalOperator = LogicalOperatorRegistry::instance().create(operatorType, operatorArguments))
    {
        return std::move(logicalOperator.value());
    }
    return nullptr;
}

std::unique_ptr<Sources::SourceDescriptor> OperatorSerializationUtil::deserializeSourceDescriptor(
    const SerializableOperator_SourceDescriptorLogicalOperator_SourceDescriptor& sourceDescriptor)
{
    /// Declaring variables outside of SourceDescriptor for readability/debuggability.
    auto schema = SchemaSerializationUtil::deserializeSchema(sourceDescriptor.sourceschema());
    auto logicalSourceName = sourceDescriptor.logicalsourcename();
    auto sourceType = sourceDescriptor.sourcetype();

    /// Deserialize the parser config.
    const auto& serializedParserConfig = sourceDescriptor.parserconfig();
    auto deserializedParserConfig = Sources::ParserConfig{};
    deserializedParserConfig.parserType = serializedParserConfig.type();
    deserializedParserConfig.tupleDelimiter = serializedParserConfig.tupledelimiter();
    deserializedParserConfig.fieldDelimiter = serializedParserConfig.fielddelimiter();

    /// Deserialize SourceDescriptor config. Convert from protobuf variant to SourceDescriptor::ConfigType.
    Configurations::DescriptorConfig::Config SourceDescriptorConfig{};
    for (const auto& [key, value] : sourceDescriptor.config())
    {
        SourceDescriptorConfig[key] = protoToDescriptorConfigType(value);
    }

    return std::make_unique<Sources::SourceDescriptor>(
        std::move(schema),
        std::move(logicalSourceName),
        std::move(sourceType),
        std::move(deserializedParserConfig),
        std::move(SourceDescriptorConfig));
}

void OperatorSerializationUtil::serializeSinkDescriptor(
    std::shared_ptr<Schema> schema, const Sinks::SinkDescriptor& sinkDescriptor, SerializableOperator_SinkLogicalOperator& sinkDetails)
{
    const auto serializedSinkDescriptor
        = SerializableOperator_SinkLogicalOperator_SerializableSinkDescriptor().New(); /// cleaned up by protobuf
    SchemaSerializationUtil::serializeSchema(std::move(schema), serializedSinkDescriptor->mutable_sinkschema());
    serializedSinkDescriptor->set_sinktype(sinkDescriptor.sinkType);
    serializedSinkDescriptor->set_addtimestamp(sinkDescriptor.addTimestamp);
    /// Iterate over SinkDescriptor config and serialize all key-value pairs.
    for (const auto& [key, value] : sinkDescriptor.config)
    {
        auto* kv = serializedSinkDescriptor->mutable_config();
        kv->emplace(key, descriptorConfigTypeToProto(value));
    }
    sinkDetails.set_allocated_sinkdescriptor(serializedSinkDescriptor);
}

std::unique_ptr<Sinks::SinkDescriptor> OperatorSerializationUtil::deserializeSinkDescriptor(
    const SerializableOperator_SinkLogicalOperator_SerializableSinkDescriptor& serializableSinkDescriptor)
{
    /// Declaring variables outside of DescriptorSource for readability/debuggability.
    auto schema = SchemaSerializationUtil::deserializeSchema(serializableSinkDescriptor.sinkschema());
    auto addTimestamp = serializableSinkDescriptor.addtimestamp();
    auto sinkType = serializableSinkDescriptor.sinktype();


    /// Deserialize DescriptorSource config. Convert from protobuf variant to DescriptorSource::ConfigType.
    Configurations::DescriptorConfig::Config sinkDescriptorConfig{};
    for (const auto& kv : serializableSinkDescriptor.config())
    {
        sinkDescriptorConfig[kv.first] = protoToDescriptorConfigType(kv.second);
    }

    auto sinkDescriptor
        = std::make_unique<Sinks::SinkDescriptor>(std::move(sinkType), std::move(sinkDescriptorConfig), std::move(addTimestamp));
    sinkDescriptor->schema = std::move(schema);
    return sinkDescriptor;
}

/*
void OperatorSerializationUtil::serializeWatermarkAssignerOperator(
    const WatermarkAssignerLogicalOperator& watermarkAssignerOperator, SerializableOperator& serializedOperator)
{
    NES_TRACE("OperatorSerializationUtil:: serialize watermark assigner operator ");

    auto watermarkStrategyDetails = SerializableOperator_WatermarkStrategyDetails();
    const auto watermarkStrategyDescriptor = watermarkAssignerOperator.getWatermarkStrategyDescriptor();
    serializeWatermarkStrategyDescriptor(*watermarkStrategyDescriptor, watermarkStrategyDetails);
    serializedOperator.mutable_details()->PackFrom(watermarkStrategyDetails);
}


void OperatorSerializationUtil::serializeWatermarkStrategyDescriptor(
    const Windowing::WatermarkStrategyDescriptor& watermarkStrategyDescriptor,
    SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails)
{
    NES_TRACE("OperatorSerializationUtil:: serialize watermark strategy ");

    if (Util::instanceOf<const Windowing::EventTimeWatermarkStrategyDescriptor>(watermarkStrategyDescriptor))
    {
        const auto eventTimeWatermarkStrategyDescriptor
            = Util::as<const Windowing::EventTimeWatermarkStrategyDescriptor>(watermarkStrategyDescriptor);
        auto serializedWatermarkStrategyDescriptor
            = SerializableOperator_WatermarkStrategyDetails_SerializableEventTimeWatermarkStrategyDescriptor();
        FunctionSerializationUtil::serializeFunction(
            eventTimeWatermarkStrategyDescriptor.getOnField(), serializedWatermarkStrategyDescriptor.mutable_onfield());
        serializedWatermarkStrategyDescriptor.set_multiplier(
            eventTimeWatermarkStrategyDescriptor.getTimeUnit().getMillisecondsConversionMultiplier());
        watermarkStrategyDetails.mutable_strategy()->PackFrom(serializedWatermarkStrategyDescriptor);
    }
    else if (Util::instanceOf<const Windowing::IngestionTimeWatermarkStrategyDescriptor>(watermarkStrategyDescriptor))
    {
        const auto serializedWatermarkStrategyDescriptor
            = SerializableOperator_WatermarkStrategyDetails_SerializableIngestionTimeWatermarkStrategyDescriptor();
        watermarkStrategyDetails.mutable_strategy()->PackFrom(serializedWatermarkStrategyDescriptor);
    }
    else
    {
        NES_ERROR("OperatorSerializationUtil: Unknown Watermark Strategy Descriptor Type");
        throw std::invalid_argument("Unknown Watermark Strategy Descriptor Type");
    }
}

std::shared_ptr<Windowing::WatermarkStrategyDescriptor> OperatorSerializationUtil::deserializeWatermarkStrategyDescriptor(
    const SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails)
{
    NES_TRACE("OperatorSerializationUtil:: de-serialize watermark strategy ");
    const auto& deserializedWatermarkStrategyDescriptor = watermarkStrategyDetails.strategy();
    if (deserializedWatermarkStrategyDescriptor
            .Is<SerializableOperator_WatermarkStrategyDetails_SerializableEventTimeWatermarkStrategyDescriptor>())
    {
        /// de-serialize print sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized WatermarkStrategy as EventTimeWatermarkStrategyDescriptor");
        auto serializedEventTimeWatermarkStrategyDescriptor
            = SerializableOperator_WatermarkStrategyDetails_SerializableEventTimeWatermarkStrategyDescriptor();
        deserializedWatermarkStrategyDescriptor.UnpackTo(&serializedEventTimeWatermarkStrategyDescriptor);

        const auto onField = Util::as<FieldAccessLogicalFunction>(
            FunctionSerializationUtil::deserializeFunction(serializedEventTimeWatermarkStrategyDescriptor.onfield()));
        NES_DEBUG("OperatorSerializationUtil:: deserialized field name {}", onField->getFieldName());
        auto eventTimeWatermarkStrategyDescriptor = Windowing::EventTimeWatermarkStrategyDescriptor::create(
            std::make_shared<FieldAccessLogicalFunction>(onField->getFieldName()),
            Windowing::TimeUnit(serializedEventTimeWatermarkStrategyDescriptor.multiplier()));
        return eventTimeWatermarkStrategyDescriptor;
    }
    if (deserializedWatermarkStrategyDescriptor
            .Is<SerializableOperator_WatermarkStrategyDetails_SerializableIngestionTimeWatermarkStrategyDescriptor>())
    {
        return Windowing::IngestionTimeWatermarkStrategyDescriptor::create();
    }
    else
    {
        NES_ERROR("OperatorSerializationUtil: Unknown Serialized Watermark Strategy Descriptor Type");
        throw std::invalid_argument("Unknown Serialized Watermark Strategy Descriptor Type");
    }
}
*/

/*
void OperatorSerializationUtil::serializeInputSchema(
    const std::shared_ptr<Operator>& operatorNode, SerializableOperator& serializedOperator)
{
    if (!NES::Util::instanceOf<BinaryLogicalOperator>(operatorNode))
    {
        SchemaSerializationUtil::serializeSchema(
            NES::Util::as<UnaryLogicalOperator>(operatorNode)->getInputSchema(), serializedOperator.mutable_inputschema());
    }
    else
    {
        const auto binaryOperator = NES::Util::as<BinaryLogicalOperator>(operatorNode);
        SchemaSerializationUtil::serializeSchema(binaryOperator->getLeftInputSchema(), serializedOperator.mutable_leftinputschema());
        SchemaSerializationUtil::serializeSchema(binaryOperator->getRightInputSchema(), serializedOperator.mutable_rightinputschema());
    }
}
*/
/*
void OperatorSerializationUtil::serializeInferModelOperator(
    const InferModel::InferModelLogicalOperator& inferModelOperator, SerializableOperator& serializedOperator)
{
    /// serialize infer model operator
    NES_TRACE("OperatorSerializationUtil:: serialize to InferModelLogicalOperator");
    auto inferModelDetails = SerializableOperator_InferModelDetails();

    for (auto& exp : inferModelOperator.getInputFields())
    {
        auto* mutableInputFields = inferModelDetails.mutable_inputfields()->Add();
        FunctionSerializationUtil::serializeFunction(exp, mutableInputFields);
    }
    for (auto& exp : inferModelOperator.getOutputFields())
    {
        auto* mutableOutputFields = inferModelDetails.mutable_outputfields()->Add();
        FunctionSerializationUtil::serializeFunction(exp, mutableOutputFields);
    }

    inferModelDetails.set_mlfilename(inferModelOperator.getDeployedModelPath());
    std::ifstream input(inferModelOperator.getModel(), std::ios::binary);

    std::string bytes((std::istreambuf_iterator<char>(input)), (std::istreambuf_iterator<char>()));
    input.close();
    inferModelDetails.set_mlfilecontent(bytes);

    serializedOperator.mutable_details()->PackFrom(inferModelDetails);
}
*/

}
