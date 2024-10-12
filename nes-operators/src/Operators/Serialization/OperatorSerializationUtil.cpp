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
#include <fstream>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Functions/FunctionSerializationUtil.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinDescriptor.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinOperator.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/LogicalLimitOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/RenameSourceOperator.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/AvgAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/CountAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/MaxAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/MedianAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/MinAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/SumAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowOperator.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>
#include <Operators/Operator.hpp>
#include <Operators/Serialization/OperatorSerializationUtil.hpp>
#include <Operators/Serialization/SchemaSerializationUtil.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Sources/SourceCSV.hpp>
#include <Sources/SourceTCP.hpp>
#include <Types/SlidingWindow.hpp>
#include <Types/ThresholdWindow.hpp>
#include <Types/TumblingWindow.hpp>
#include <Types/WindowType.hpp>
#include <Util/Common.hpp>
#include <google/protobuf/json/json.h>
#include <ErrorHandling.hpp>

namespace NES
{

SerializableOperator OperatorSerializationUtil::serializeOperator(const OperatorPtr& operatorNode)
{
    NES_TRACE("OperatorSerializationUtil:: serialize operator {}", operatorNode->toString());
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
    else if (NES::Util::instanceOf<LogicalFilterOperator>(operatorNode))
    {
        /// serialize filter operator
        serializeFilterOperator(*NES::Util::as<LogicalFilterOperator>(operatorNode), serializedOperator);
    }
    else if (NES::Util::instanceOf<LogicalProjectionOperator>(operatorNode))
    {
        /// serialize projection operator
        serializeProjectionOperator(*NES::Util::as<LogicalProjectionOperator>(operatorNode), serializedOperator);
    }
    else if (NES::Util::instanceOf<LogicalUnionOperator>(operatorNode))
    {
        /// serialize union operator
        NES_TRACE("OperatorSerializationUtil:: serialize to LogicalUnionOperator");
        auto unionDetails = SerializableOperator_UnionDetails();
        serializedOperator.mutable_details()->PackFrom(unionDetails);
    }
    else if (NES::Util::instanceOf<LogicalMapOperator>(operatorNode))
    {
        /// serialize map operator
        serializeMapOperator(*NES::Util::as<LogicalMapOperator>(operatorNode), serializedOperator);
    }
    else if (NES::Util::instanceOf<InferModel::LogicalInferModelOperator>(operatorNode))
    {
        /// serialize infer model
        serializeInferModelOperator(*NES::Util::as<InferModel::LogicalInferModelOperator>(operatorNode), serializedOperator);
    }
    else if (NES::Util::instanceOf<LogicalWindowOperator>(operatorNode))
    {
        /// serialize window operator
        serializeWindowOperator(*NES::Util::as<LogicalWindowOperator>(operatorNode), serializedOperator);
    }
    else if (NES::Util::instanceOf<LogicalJoinOperator>(operatorNode))
    {
        /// serialize streaming join operator
        serializeJoinOperator(*NES::Util::as<LogicalJoinOperator>(operatorNode), serializedOperator);
    }
    else if (NES::Util::instanceOf<Experimental::LogicalBatchJoinOperator>(operatorNode))
    {
        /// serialize batch join operator
        serializeBatchJoinOperator(*NES::Util::as<Experimental::LogicalBatchJoinOperator>(operatorNode), serializedOperator);
    }
    else if (NES::Util::instanceOf<LogicalLimitOperator>(operatorNode))
    {
        /// serialize limit operator
        serializeLimitOperator(*NES::Util::as<LogicalLimitOperator>(operatorNode), serializedOperator);
    }
    else if (NES::Util::instanceOf<WatermarkAssignerLogicalOperator>(operatorNode))
    {
        /// serialize watermarkAssigner operator
        serializeWatermarkAssignerOperator(*NES::Util::as<WatermarkAssignerLogicalOperator>(operatorNode), serializedOperator);
    }
    else if (NES::Util::instanceOf<RenameSourceOperator>(operatorNode))
    {
        /// Serialize rename source operator
        NES_TRACE("OperatorSerializationUtil:: serialize to RenameSourceOperator");
        auto renameDetails = SerializableOperator_RenameDetails();
        renameDetails.set_newsourcename(NES::Util::as<RenameSourceOperator>(operatorNode)->getNewSourceName());
        serializedOperator.mutable_details()->PackFrom(renameDetails);
    }
    else
    {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not serialize this operator: {}", operatorNode->toString());
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
    if (NES::Util::instanceOf<BinaryOperator>(operatorNode))
    {
        auto binaryOperator = NES::Util::as<BinaryOperator>(operatorNode);
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
        auto unaryOperator = NES::Util::as<UnaryOperator>(operatorNode);
        for (const auto& originId : unaryOperator->getInputOriginIds())
        {
            serializedOperator.add_originids(originId.getRawValue());
        }
    }

    NES_TRACE("OperatorSerializationUtil:: serialize {} to {}", operatorNode->toString(), serializedOperator.details().type_url());
    return serializedOperator;
}

OperatorPtr OperatorSerializationUtil::deserializeOperator(SerializableOperator serializedOperator)
{
    NES_TRACE("OperatorSerializationUtil:: de-serialize {}", serializedOperator.DebugString());
    auto details = serializedOperator.details();
    LogicalOperatorPtr operatorNode;
    if (details.Is<SerializableOperator_SourceDescriptorLogicalOperator>())
    {
        /// de-serialize source operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to SourceNameLogicalOperator");
        auto serializedSourceDescriptorLogicalOperator = SerializableOperator_SourceDescriptorLogicalOperator();
        details.UnpackTo(&serializedSourceDescriptorLogicalOperator);
        operatorNode = deserializeSourceOperator(serializedSourceDescriptorLogicalOperator);
    }
    else if (details.Is<SerializableOperator_SinkDetails>())
    {
        /// de-serialize sink operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to SinkLogicalOperator");
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails();
        details.UnpackTo(&serializedSinkDescriptor);
        operatorNode = deserializeSinkOperator(serializedSinkDescriptor);
    }
    else if (details.Is<SerializableOperator_FilterDetails>())
    {
        /// de-serialize filter operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to FilterLogicalOperator");
        auto serializedFilterOperator = SerializableOperator_FilterDetails();
        details.UnpackTo(&serializedFilterOperator);
        operatorNode = deserializeFilterOperator(serializedFilterOperator);
    }
    else if (details.Is<SerializableOperator_ProjectionDetails>())
    {
        /// de-serialize projection operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to ProjectionLogicalOperator");
        auto serializedProjectionOperator = SerializableOperator_ProjectionDetails();
        details.UnpackTo(&serializedProjectionOperator);
        operatorNode = deserializeProjectionOperator(serializedProjectionOperator);
    }
    else if (details.Is<SerializableOperator_UnionDetails>())
    {
        /// de-serialize union operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to UnionLogicalOperator");
        auto serializedUnionDescriptor = SerializableOperator_UnionDetails();
        details.UnpackTo(&serializedUnionDescriptor);
        operatorNode = LogicalOperatorFactory::createUnionOperator(getNextOperatorId());
    }
    else if (details.Is<SerializableOperator_MapDetails>())
    {
        /// de-serialize map operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to MapLogicalOperator");
        auto serializedMapOperator = SerializableOperator_MapDetails();
        details.UnpackTo(&serializedMapOperator);
        operatorNode = deserializeMapOperator(serializedMapOperator);
    }
    else if (details.Is<SerializableOperator_InferModelDetails>())
    {
        /// de-serialize infer model operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to InferModelLogicalOperator");
        auto serializedInferModelOperator = SerializableOperator_InferModelDetails();
        details.UnpackTo(&serializedInferModelOperator);
        operatorNode = deserializeInferModelOperator(serializedInferModelOperator);
    }
    else if (details.Is<SerializableOperator_WindowDetails>())
    {
        /// de-serialize window operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to WindowLogicalOperator");
        auto serializedWindowOperator = SerializableOperator_WindowDetails();
        details.UnpackTo(&serializedWindowOperator);
        auto windowNode = deserializeWindowOperator(serializedWindowOperator, getNextOperatorId());
        operatorNode = windowNode;
    }
    else if (details.Is<SerializableOperator_JoinDetails>())
    {
        /// de-serialize streaming join operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to JoinLogicalOperator");
        auto serializedJoinOperator = SerializableOperator_JoinDetails();
        details.UnpackTo(&serializedJoinOperator);
        operatorNode = deserializeJoinOperator(serializedJoinOperator, getNextOperatorId());
    }
    else if (details.Is<SerializableOperator_BatchJoinDetails>())
    {
        /// de-serialize batch join operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to BatchJoinLogicalOperator");
        auto serializedBatchJoinOperator = SerializableOperator_BatchJoinDetails();
        details.UnpackTo(&serializedBatchJoinOperator);
        operatorNode = deserializeBatchJoinOperator(serializedBatchJoinOperator, getNextOperatorId());
    }
    else if (details.Is<SerializableOperator_WatermarkStrategyDetails>())
    {
        /// de-serialize watermark assigner operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to watermarkassigner operator");
        auto serializedWatermarkStrategyDetails = SerializableOperator_WatermarkStrategyDetails();
        details.UnpackTo(&serializedWatermarkStrategyDetails);
        operatorNode = deserializeWatermarkAssignerOperator(serializedWatermarkStrategyDetails);
    }
    else if (details.Is<SerializableOperator_LimitDetails>())
    {
        /// de-serialize limit operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to limit operator");
        auto serializedLimitDetails = SerializableOperator_LimitDetails();
        details.UnpackTo(&serializedLimitDetails);
        operatorNode = deserializeLimitOperator(serializedLimitDetails);
    }
    else if (details.Is<SerializableOperator_RenameDetails>())
    {
        /// Deserialize rename source operator.
        NES_TRACE("OperatorSerializationUtil:: deserialize to rename source operator");
        auto renameDetails = SerializableOperator_RenameDetails();
        details.UnpackTo(&renameDetails);
        operatorNode = LogicalOperatorFactory::createRenameSourceOperator(renameDetails.newsourcename());
    }
    else
    {
        throw CannotDeserialize(
            fmt::format("OperatorSerializationUtil: could not de-serialize this serialized operator: {}", details.DebugString()));
    }

    /// de-serialize operator output schema
    operatorNode->setOutputSchema(SchemaSerializationUtil::deserializeSchema(serializedOperator.outputschema()));
    /// deserialize input schema
    deserializeInputSchema(operatorNode, serializedOperator);

    if (details.Is<SerializableOperator_JoinDetails>())
    {
        auto joinOp = NES::Util::as<LogicalJoinOperator>(operatorNode);
        joinOp->getJoinDefinition()->updateSourceTypes(joinOp->getLeftInputSchema(), joinOp->getRightInputSchema());
        joinOp->getJoinDefinition()->updateOutputDefinition(joinOp->getOutputSchema());
    }

    if (details.Is<SerializableOperator_BatchJoinDetails>())
    {
        auto joinOp = NES::Util::as<Experimental::LogicalBatchJoinOperator>(operatorNode);
        joinOp->getBatchJoinDefinition()->updateInputSchemas(joinOp->getLeftInputSchema(), joinOp->getRightInputSchema());
        joinOp->getBatchJoinDefinition()->updateOutputDefinition(joinOp->getOutputSchema());
    }

    /// de-serialize and append origin id
    if (NES::Util::instanceOf<BinaryOperator>(operatorNode))
    {
        auto binaryOperator = NES::Util::as<BinaryOperator>(operatorNode);
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
        auto unaryOperator = NES::Util::as<UnaryOperator>(operatorNode);
        std::vector<OriginId> originIds;
        for (const auto& originId : serializedOperator.originids())
        {
            originIds.emplace_back(originId);
        }
        unaryOperator->setInputOriginIds(originIds);
    }
    NES_TRACE("OperatorSerializationUtil:: de-serialize {} to {}", serializedOperator.DebugString(), operatorNode->toString());
    return operatorNode;
}

void OperatorSerializationUtil::serializeSourceOperator(
    SourceDescriptorLogicalOperator& sourceOperator, SerializableOperator& serializedOperator)
{
    NES_TRACE("OperatorSerializationUtil:: serialize to SourceNameLogicalOperator");

    auto sourceDetails = SerializableOperator_SourceDescriptorLogicalOperator();
    const auto& sourceDescriptor = sourceOperator.getSourceDescriptorRef();
    serializeSourceDescriptor(sourceDescriptor, sourceDetails);
    sourceDetails.set_sourceoriginid(sourceOperator.getOriginId().getRawValue());

    serializedOperator.mutable_details()->PackFrom(sourceDetails);
}

LogicalUnaryOperatorPtr
OperatorSerializationUtil::deserializeSourceOperator(const SerializableOperator_SourceDescriptorLogicalOperator& sourceDetails)
{
    const auto serializedSourceDescriptor = sourceDetails.sourcedescriptor();
    auto sourceDescriptor = deserializeSourceDescriptor(serializedSourceDescriptor);
    const auto sourceId = sourceDetails.sourceoriginid();
    return LogicalOperatorFactory::createSourceOperator(std::move(sourceDescriptor), getNextOperatorId(), OriginId(sourceId));
}

void OperatorSerializationUtil::serializeFilterOperator(
    const LogicalFilterOperator& filterOperator, SerializableOperator& serializedOperator)
{
    NES_TRACE("OperatorSerializationUtil:: serialize to LogicalFilterOperator");
    auto filterDetails = SerializableOperator_FilterDetails();
    FunctionSerializationUtil::serializeFunction(filterOperator.getPredicate(), filterDetails.mutable_predicate());
    serializedOperator.mutable_details()->PackFrom(filterDetails);
}

LogicalUnaryOperatorPtr OperatorSerializationUtil::deserializeFilterOperator(const SerializableOperator_FilterDetails& filterDetails)
{
    auto filterFunction = FunctionSerializationUtil::deserializeFunction(filterDetails.predicate());
    return LogicalOperatorFactory::createFilterOperator(filterFunction, getNextOperatorId());
}

void OperatorSerializationUtil::serializeProjectionOperator(
    const LogicalProjectionOperator& projectionOperator, SerializableOperator& serializedOperator)
{
    NES_TRACE("OperatorSerializationUtil:: serialize to LogicalProjectionOperator");
    auto projectionDetail = SerializableOperator_ProjectionDetails();
    for (auto& exp : projectionOperator.getFunctions())
    {
        auto* mutableFunction = projectionDetail.mutable_function()->Add();
        FunctionSerializationUtil::serializeFunction(exp, mutableFunction);
    }

    serializedOperator.mutable_details()->PackFrom(projectionDetail);
}

LogicalUnaryOperatorPtr
OperatorSerializationUtil::deserializeProjectionOperator(const SerializableOperator_ProjectionDetails& projectionDetails)
{
    /// serialize and append children if the node has any
    std::vector<NodeFunctionPtr> exps;
    for (const auto& serializedFunction : projectionDetails.function())
    {
        auto projectFunction = FunctionSerializationUtil::deserializeFunction(serializedFunction);
        exps.push_back(projectFunction);
    }

    return LogicalOperatorFactory::createProjectionOperator(exps, getNextOperatorId());
}

void OperatorSerializationUtil::serializeSinkOperator(const SinkLogicalOperator& sinkOperator, SerializableOperator& serializedOperator)
{
    NES_TRACE("OperatorSerializationUtil:: serialize to SinkLogicalOperator");
    auto sinkDetails = SerializableOperator_SinkDetails();
    auto sinkDescriptor = sinkOperator.getSinkDescriptor();
    serializeSinkDescriptor(*sinkDescriptor, sinkDetails, sinkOperator.getInputOriginIds().size());
    serializedOperator.mutable_details()->PackFrom(sinkDetails);
}

LogicalUnaryOperatorPtr OperatorSerializationUtil::deserializeSinkOperator(const SerializableOperator_SinkDetails& sinkDetails)
{
    auto sinkDescriptor = deserializeSinkDescriptor(sinkDetails);
    return LogicalOperatorFactory::createSinkOperator(sinkDescriptor, INVALID_WORKER_NODE_ID, getNextOperatorId());
}

void OperatorSerializationUtil::serializeMapOperator(const LogicalMapOperator& mapOperator, SerializableOperator& serializedOperator)
{
    NES_TRACE("OperatorSerializationUtil:: serialize to LogicalMapOperator");
    auto mapDetails = SerializableOperator_MapDetails();
    FunctionSerializationUtil::serializeFunction(mapOperator.getMapFunction(), mapDetails.mutable_function());
    serializedOperator.mutable_details()->PackFrom(mapDetails);
}

LogicalUnaryOperatorPtr OperatorSerializationUtil::deserializeMapOperator(const SerializableOperator_MapDetails& mapDetails)
{
    auto fieldAssignmentFunction = FunctionSerializationUtil::deserializeFunction(mapDetails.function());
    return LogicalOperatorFactory::createMapOperator(
        NES::Util::as<NodeFunctionFieldAssignment>(fieldAssignmentFunction), getNextOperatorId());
}

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
    windowDetails.set_origin(windowOperator.getOriginId().getRawValue());
    windowDetails.set_allowedlateness(windowDefinition->getAllowedLateness());
    auto windowType = windowDefinition->getWindowType();

    if (Util::instanceOf<Windowing::TimeBasedWindowType>(windowType))
    {
        auto timeBasedWindowType = Util::as<Windowing::TimeBasedWindowType>(windowType);
        auto timeCharacteristic = timeBasedWindowType->getTimeCharacteristic();
        auto timeCharacteristicDetails = SerializableOperator_TimeCharacteristic();
        if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::Type::EventTime)
        {
            timeCharacteristicDetails.set_type(SerializableOperator_TimeCharacteristic_Type_EventTime);
            timeCharacteristicDetails.set_field(timeCharacteristic->getField()->getName());
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

        using enum Windowing::WindowAggregationDescriptor::Type;
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

    serializedOperator.mutable_details()->PackFrom(windowDetails);
}

LogicalUnaryOperatorPtr
OperatorSerializationUtil::deserializeWindowOperator(const SerializableOperator_WindowDetails& windowDetails, OperatorId operatorId)
{
    const auto& serializedWindowAggregations = windowDetails.windowaggregations();
    const auto& serializedWindowType = windowDetails.windowtype();

    std::vector<Windowing::WindowAggregationDescriptorPtr> aggregation;
    for (const auto& serializedWindowAggregation : serializedWindowAggregations)
    {
        auto onField
            = Util::as<NodeFunctionFieldAccess>(FunctionSerializationUtil::deserializeFunction(serializedWindowAggregation.onfield()));
        auto asField
            = Util::as<NodeFunctionFieldAccess>(FunctionSerializationUtil::deserializeFunction(serializedWindowAggregation.asfield()));
        if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_SUM)
        {
            aggregation.emplace_back(Windowing::SumAggregationDescriptor::create(onField, asField));
        }
        else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_MAX)
        {
            aggregation.emplace_back(Windowing::MaxAggregationDescriptor::create(onField, asField));
        }
        else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_MIN)
        {
            aggregation.emplace_back(Windowing::MinAggregationDescriptor::create(onField, asField));
        }
        else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_COUNT)
        {
            aggregation.emplace_back(Windowing::CountAggregationDescriptor::create(onField, asField));
        }
        else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_AVG)
        {
            aggregation.emplace_back(Windowing::AvgAggregationDescriptor::create(onField, asField));
        }
        else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_MEDIAN)
        {
            aggregation.emplace_back(Windowing::MedianAggregationDescriptor::create(onField, asField));
        }
        else
        {
            NES_FATAL_ERROR(
                "OperatorSerializationUtil: could not de-serialize window aggregation: {}", serializedWindowAggregation.DebugString());
        }
    }

    Windowing::WindowTypePtr window;
    if (serializedWindowType.Is<SerializableOperator_TumblingWindow>())
    {
        auto serializedTumblingWindow = SerializableOperator_TumblingWindow();
        serializedWindowType.UnpackTo(&serializedTumblingWindow);
        auto serializedTimeCharacteristic = serializedTumblingWindow.timecharacteristic();
        auto multiplier = serializedTimeCharacteristic.multiplier();
        if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_EventTime)
        {
            auto field = NodeFunctionFieldAccess::create(serializedTimeCharacteristic.field());
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
            auto field = NodeFunctionFieldAccess::create(serializedTimeCharacteristic.field());
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

    auto allowedLateness = windowDetails.allowedlateness();
    std::vector<NodeFunctionFieldAccessPtr> keyAccessFunction;
    for (auto& key : windowDetails.keys())
    {
        keyAccessFunction.emplace_back(Util::as<NodeFunctionFieldAccess>(FunctionSerializationUtil::deserializeFunction(key)));
    }
    auto windowDef = Windowing::LogicalWindowDescriptor::create(keyAccessFunction, aggregation, window, allowedLateness);
    windowDef->setOriginId(OriginId(windowDetails.origin()));
    return LogicalOperatorFactory::createWindowOperator(windowDef, operatorId);
}

void OperatorSerializationUtil::serializeJoinOperator(const LogicalJoinOperator& joinOperator, SerializableOperator& serializedOperator)
{
    NES_TRACE("OperatorSerializationUtil:: serialize to LogicalJoinOperator");
    auto joinDetails = SerializableOperator_JoinDetails();
    auto joinDefinition = joinOperator.getJoinDefinition();

    FunctionSerializationUtil::serializeFunction(joinDefinition->getJoinFunction(), joinDetails.mutable_joinfunction());

    auto windowType = joinDefinition->getWindowType();
    auto timeBasedWindowType = Util::as<Windowing::TimeBasedWindowType>(windowType);
    auto timeCharacteristic = timeBasedWindowType->getTimeCharacteristic();
    auto timeCharacteristicDetails = SerializableOperator_TimeCharacteristic();
    timeCharacteristicDetails.set_multiplier(timeCharacteristic->getTimeUnit().getMillisecondsConversionMultiplier());
    if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::Type::EventTime)
    {
        timeCharacteristicDetails.set_type(SerializableOperator_TimeCharacteristic_Type_EventTime);
        timeCharacteristicDetails.set_field(timeCharacteristic->getField()->getName());
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
        auto tumblingWindow = Util::as<Windowing::TumblingWindow>(windowType);
        auto tumblingWindowDetails = SerializableOperator_TumblingWindow();
        tumblingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
        tumblingWindowDetails.set_size(tumblingWindow->getSize().getTime());
        joinDetails.mutable_windowtype()->PackFrom(tumblingWindowDetails);
    }
    else if (Util::instanceOf<Windowing::SlidingWindow>(windowType))
    {
        auto slidingWindow = Util::as<Windowing::SlidingWindow>(windowType);
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
    joinDetails.set_windowstartfieldname(joinOperator.getWindowStartFieldName());
    joinDetails.set_windowendfieldname(joinOperator.getWindowEndFieldName());
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

LogicalJoinOperatorPtr
OperatorSerializationUtil::deserializeJoinOperator(const SerializableOperator_JoinDetails& joinDetails, OperatorId operatorId)
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

    Windowing::WindowTypePtr window;
    if (serializedWindowType.Is<SerializableOperator_TumblingWindow>())
    {
        auto serializedTumblingWindow = SerializableOperator_TumblingWindow();
        serializedWindowType.UnpackTo(&serializedTumblingWindow);
        const auto& serializedTimeCharacteristic = serializedTumblingWindow.timecharacteristic();
        auto multiplier = serializedTimeCharacteristic.multiplier();
        if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_EventTime)
        {
            auto field = NodeFunctionFieldAccess::create(serializedTimeCharacteristic.field());
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
        auto multiplier = serializedTimeCharacteristic.multiplier();
        if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_EventTime)
        {
            auto field = NodeFunctionFieldAccess::create(serializedTimeCharacteristic.field());
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

    LogicalOperatorPtr ptr;
    auto serializedJoinFunction = joinDetails.joinfunction();
    auto joinFunction = Util::as<NodeFunctionBinary>(FunctionSerializationUtil::deserializeFunction(serializedJoinFunction));

    auto joinDefinition = Join::LogicalJoinDescriptor::create(
        joinFunction, window, joinDetails.numberofinputedgesleft(), joinDetails.numberofinputedgesright(), joinType);
    auto joinOperator = NES::Util::as<LogicalJoinOperator>(LogicalOperatorFactory::createJoinOperator(joinDefinition, operatorId));
    joinOperator->setWindowStartEndKeyFieldName(joinDetails.windowstartfieldname(), joinDetails.windowendfieldname());
    joinOperator->setOriginId(OriginId(joinDetails.origin()));
    return joinOperator;
}

void OperatorSerializationUtil::serializeBatchJoinOperator(
    const Experimental::LogicalBatchJoinOperator& joinOperator, SerializableOperator& serializedOperator)
{
    NES_TRACE("OperatorSerializationUtil:: serialize to LogicalBatchJoinOperator");

    auto joinDetails = SerializableOperator_BatchJoinDetails();
    auto joinDefinition = joinOperator.getBatchJoinDefinition();

    FunctionSerializationUtil::serializeFunction(joinDefinition->getBuildJoinKey(), joinDetails.mutable_onbuildkey());
    FunctionSerializationUtil::serializeFunction(joinDefinition->getProbeJoinKey(), joinDetails.mutable_onprobekey());

    joinDetails.set_numberofinputedgesbuild(joinDefinition->getNumberOfInputEdgesBuild());
    joinDetails.set_numberofinputedgesprobe(joinDefinition->getNumberOfInputEdgesProbe());

    serializedOperator.mutable_details()->PackFrom(joinDetails);
}

Experimental::LogicalBatchJoinOperatorPtr
OperatorSerializationUtil::deserializeBatchJoinOperator(const SerializableOperator_BatchJoinDetails& joinDetails, OperatorId operatorId)
{
    auto buildKeyAccessFunction
        = NES::Util::as<NodeFunctionFieldAccess>(FunctionSerializationUtil::deserializeFunction(joinDetails.onbuildkey()));
    auto probeKeyAccessFunction
        = NES::Util::as<NodeFunctionFieldAccess>(FunctionSerializationUtil::deserializeFunction(joinDetails.onprobekey()));
    auto joinDefinition = Join::Experimental::LogicalBatchJoinDescriptor::create(
        buildKeyAccessFunction, probeKeyAccessFunction, joinDetails.numberofinputedgesprobe(), joinDetails.numberofinputedgesbuild());
    auto retValue = NES::Util::as<Experimental::LogicalBatchJoinOperator>(
        LogicalOperatorFactory::createBatchJoinOperator(joinDefinition, operatorId));
    return retValue;
}

SerializableOperator_SourceDescriptorLogicalOperator_VariantSourceDescriptor
SourceDescriptorConfigTypeToProto(const Configurations::DescriptorConfig::ConfigType& var)
{
    SerializableOperator_SourceDescriptorLogicalOperator_VariantSourceDescriptor proto_var;
    std::visit(
        [&proto_var]<typename T>(T&& arg)
        {
            /// Remove const, volatile, and reference to simplify type matching
            using U = std::remove_cvref_t<T>;
            if constexpr (std::is_same_v<U, int32_t>)
                proto_var.set_int_value(arg);
            else if constexpr (std::is_same_v<U, uint32_t>)
                proto_var.set_uint_value(arg);
            else if constexpr (std::is_same_v<U, bool>)
                proto_var.set_bool_value(arg);
            else if constexpr (std::is_same_v<U, char>)
                proto_var.set_char_value(arg);
            else if constexpr (std::is_same_v<U, float>)
                proto_var.set_float_value(arg);
            else if constexpr (std::is_same_v<U, double>)
                proto_var.set_double_value(arg);
            else if constexpr (std::is_same_v<U, std::string>)
                proto_var.set_string_value(arg);
            else if constexpr (std::is_same_v<U, Configurations::EnumWrapper>)
            {
                auto enumWrapper = SerializableOperator_SourceDescriptorLogicalOperator_EnumWrapper().New();
                enumWrapper->set_value(arg.getValue());
                proto_var.set_allocated_enum_value(enumWrapper);
            }
            else
                static_assert(!std::is_same_v<U, U>, "Unsupported type in SourceDescriptorConfigTypeToProto"); /// is_same_v for logging T
        },
        var);
    return proto_var;
}
void OperatorSerializationUtil::serializeSourceDescriptor(
    const Sources::SourceDescriptor& sourceDescriptor, SerializableOperator_SourceDescriptorLogicalOperator& sourceDetails)
{
    auto serializedSourceDescriptor
        = SerializableOperator_SourceDescriptorLogicalOperator_SourceDescriptor().New(); /// cleaned up by protobuf

    SchemaSerializationUtil::serializeSchema(sourceDescriptor.schema, serializedSourceDescriptor->mutable_sourceschema());
    serializedSourceDescriptor->set_logicalsourcename(sourceDescriptor.logicalSourceName);
    serializedSourceDescriptor->set_sourcetype(sourceDescriptor.sourceType);
    /// Convert from Configurations::InputFormat to protobuf InputFormat.
    switch (sourceDescriptor.inputFormat)
    {
        case Configurations::InputFormat::CSV:
            serializedSourceDescriptor->set_inputformat(InputFormat::CSV);
            break;
        default:
            throw InvalidConfigParameter(
                fmt::format("Serialization of inputFormat: {} is not supported.", magic_enum::enum_name(sourceDescriptor.inputFormat)));
    }
    /// Iterate over SourceDescriptor config and serialize all key-value pairs.
    for (const auto& [key, value] : sourceDescriptor.config)
    {
        auto* kv = serializedSourceDescriptor->mutable_config();
        kv->emplace(key, SourceDescriptorConfigTypeToProto(value));
    }
    sourceDetails.set_allocated_sourcedescriptor(serializedSourceDescriptor);
}

Configurations::DescriptorConfig::ConfigType
protoToSourceDescriptorConfigType(const SerializableOperator_SourceDescriptorLogicalOperator_VariantSourceDescriptor& proto_var)
{
    switch (proto_var.value_case())
    {
        case SerializableOperator_SourceDescriptorLogicalOperator_VariantSourceDescriptor::kIntValue:
            return proto_var.int_value();
        case SerializableOperator_SourceDescriptorLogicalOperator_VariantSourceDescriptor::kUintValue:
            return proto_var.uint_value();
        case SerializableOperator_SourceDescriptorLogicalOperator_VariantSourceDescriptor::kBoolValue:
            return proto_var.bool_value();
        case SerializableOperator_SourceDescriptorLogicalOperator_VariantSourceDescriptor::kCharValue:
            return static_cast<char>(proto_var.char_value()); /// Convert (fixed32) ascii number to char.
        case SerializableOperator_SourceDescriptorLogicalOperator_VariantSourceDescriptor::kFloatValue:
            return proto_var.float_value();
        case SerializableOperator_SourceDescriptorLogicalOperator_VariantSourceDescriptor::kDoubleValue:
            return proto_var.double_value();
        case SerializableOperator_SourceDescriptorLogicalOperator_VariantSourceDescriptor::kStringValue:
            return proto_var.string_value();
        case SerializableOperator_SourceDescriptorLogicalOperator_VariantSourceDescriptor::kEnumValue:
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
std::unique_ptr<Sources::SourceDescriptor> OperatorSerializationUtil::deserializeSourceDescriptor(
    const SerializableOperator_SourceDescriptorLogicalOperator_SourceDescriptor& sourceDescriptor)
{
    /// Declaring variables outside of SourceDescriptor for readability/debuggability.
    auto schema = SchemaSerializationUtil::deserializeSchema(sourceDescriptor.sourceschema());
    auto logicalSourceName = sourceDescriptor.logicalsourcename();
    auto sourceType = sourceDescriptor.sourcetype();
    auto inputFormat = sourceDescriptor.inputformat();

    /// Deserialize SourceDescriptor config. Convert from protobuf variant to SourceDescriptor::ConfigType.
    Configurations::DescriptorConfig::Config SourceDescriptorConfig{};
    for (const auto& kv : sourceDescriptor.config())
    {
        SourceDescriptorConfig[kv.first] = protoToSourceDescriptorConfigType(kv.second);
    }

    return std::make_unique<Sources::SourceDescriptor>(
        std::move(schema),
        std::move(logicalSourceName),
        std::move(sourceType),
        std::move(static_cast<Configurations::InputFormat>(inputFormat)),
        std::move(SourceDescriptorConfig));
}

void OperatorSerializationUtil::serializeSinkDescriptor(
    const SinkDescriptor& sinkDescriptor, SerializableOperator_SinkDetails& sinkDetails, uint64_t numberOfOrigins)
{
    /// serialize a sink descriptor and all its properties depending on its type
    NES_DEBUG("OperatorSerializationUtil:: serialized SinkDescriptor ");
    if (Util::instanceOf<const PrintSinkDescriptor>(sinkDescriptor))
    {
        /// serialize print sink descriptor
        auto printSinkDescriptor = Util::as<const PrintSinkDescriptor>(sinkDescriptor);
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as "
                  "SerializableOperator_SinkDetails_SerializablePrintSinkDescriptor");
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializablePrintSinkDescriptor();
        sinkDetails.mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
        sinkDetails.set_numberoforiginids(numberOfOrigins);
    }
    else if (Util::instanceOf<const FileSinkDescriptor>(sinkDescriptor))
    {
        /// serialize file sink descriptor. The file sink has different types which have to be set correctly
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as "
                  "SerializableOperator_SinkDetails_SerializableFileSinkDescriptor");
        auto fileSinkDescriptor = Util::as<const FileSinkDescriptor>(sinkDescriptor);
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableFileSinkDescriptor();

        serializedSinkDescriptor.set_filepath(fileSinkDescriptor.getFileName());
        serializedSinkDescriptor.set_append(fileSinkDescriptor.getAppend());
        serializedSinkDescriptor.set_addtimestamp(fileSinkDescriptor.getAddTimestamp());

        auto format = fileSinkDescriptor.getSinkFormatAsString();
        if (format == "JSON_FORMAT")
        {
            serializedSinkDescriptor.set_sinkformat("JSON_FORMAT");
        }
        else if (format == "CSV_FORMAT")
        {
            serializedSinkDescriptor.set_sinkformat("CSV_FORMAT");
        }
        else if (format == "NES_FORMAT")
        {
            serializedSinkDescriptor.set_sinkformat("NES_FORMAT");
        }
        else if (format == "TEXT_FORMAT")
        {
            serializedSinkDescriptor.set_sinkformat("TEXT_FORMAT");
        }
        else
        {
            NES_ERROR("serializeSinkDescriptor: format not supported");
        }
        sinkDetails.mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
        sinkDetails.set_numberoforiginids(numberOfOrigins);
    }
    else
    {
        NES_ERROR("OperatorSerializationUtil: Unknown Sink Descriptor Type - {}", sinkDescriptor.toString());
        throw std::invalid_argument("Unknown Sink Descriptor Type");
    }
}

SinkDescriptorPtr OperatorSerializationUtil::deserializeSinkDescriptor(const SerializableOperator_SinkDetails& sinkDetails)
{
    /// de-serialize a sink descriptor and all its properties to a SinkDescriptor.
    NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor {}", sinkDetails.DebugString());
    const auto& deserializedSinkDescriptor = sinkDetails.sinkdescriptor();
    const auto deserializedNumberOfOrigins = sinkDetails.numberoforiginids();
    if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializablePrintSinkDescriptor>())
    {
        /// de-serialize print sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as PrintSinkDescriptor");
        return PrintSinkDescriptor::create(deserializedNumberOfOrigins);
    }
    else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableFileSinkDescriptor>())
    {
        /// de-serialize file sink descriptor
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableFileSinkDescriptor();
        deserializedSinkDescriptor.UnpackTo(&serializedSinkDescriptor);
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as FileSinkDescriptor");
        return FileSinkDescriptor::create(
            serializedSinkDescriptor.filepath(),
            serializedSinkDescriptor.sinkformat(),
            serializedSinkDescriptor.append() ? "APPEND" : "OVERWRITE",
            serializedSinkDescriptor.addtimestamp(),
            deserializedNumberOfOrigins);
    }
    else
    {
        NES_ERROR("OperatorSerializationUtil: Unknown sink Descriptor Type {}", sinkDetails.DebugString());
        throw std::invalid_argument("Unknown Sink Descriptor Type");
    }
}

void OperatorSerializationUtil::serializeLimitOperator(const LogicalLimitOperator& limitOperator, SerializableOperator& serializedOperator)
{
    NES_TRACE("OperatorSerializationUtil:: serialize limit operator ");

    auto limitDetails = SerializableOperator_LimitDetails();
    limitDetails.set_limit(limitOperator.getLimit());
    serializedOperator.mutable_details()->PackFrom(limitDetails);
}

LogicalUnaryOperatorPtr OperatorSerializationUtil::deserializeLimitOperator(const SerializableOperator_LimitDetails& limitDetails)
{
    return LogicalOperatorFactory::createLimitOperator(limitDetails.limit(), getNextOperatorId());
}

void OperatorSerializationUtil::serializeWatermarkAssignerOperator(
    const WatermarkAssignerLogicalOperator& watermarkAssignerOperator, SerializableOperator& serializedOperator)
{
    NES_TRACE("OperatorSerializationUtil:: serialize watermark assigner operator ");

    auto watermarkStrategyDetails = SerializableOperator_WatermarkStrategyDetails();
    auto watermarkStrategyDescriptor = watermarkAssignerOperator.getWatermarkStrategyDescriptor();
    serializeWatermarkStrategyDescriptor(*watermarkStrategyDescriptor, watermarkStrategyDetails);
    serializedOperator.mutable_details()->PackFrom(watermarkStrategyDetails);
}

LogicalUnaryOperatorPtr OperatorSerializationUtil::deserializeWatermarkAssignerOperator(
    const SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails)
{
    auto watermarkStrategyDescriptor = deserializeWatermarkStrategyDescriptor(watermarkStrategyDetails);
    return LogicalOperatorFactory::createWatermarkAssignerOperator(watermarkStrategyDescriptor, getNextOperatorId());
}

void OperatorSerializationUtil::serializeWatermarkStrategyDescriptor(
    const Windowing::WatermarkStrategyDescriptor& watermarkStrategyDescriptor,
    SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails)
{
    NES_TRACE("OperatorSerializationUtil:: serialize watermark strategy ");

    if (Util::instanceOf<const Windowing::EventTimeWatermarkStrategyDescriptor>(watermarkStrategyDescriptor))
    {
        auto eventTimeWatermarkStrategyDescriptor
            = Util::as<const Windowing::EventTimeWatermarkStrategyDescriptor>(watermarkStrategyDescriptor);
        auto serializedWatermarkStrategyDescriptor
            = SerializableOperator_WatermarkStrategyDetails_SerializableEventTimeWatermarkStrategyDescriptor();
        FunctionSerializationUtil::serializeFunction(
            eventTimeWatermarkStrategyDescriptor.getOnField(), serializedWatermarkStrategyDescriptor.mutable_onfield());
        serializedWatermarkStrategyDescriptor.set_allowedlateness(eventTimeWatermarkStrategyDescriptor.getAllowedLateness().getTime());
        serializedWatermarkStrategyDescriptor.set_multiplier(
            eventTimeWatermarkStrategyDescriptor.getTimeUnit().getMillisecondsConversionMultiplier());
        watermarkStrategyDetails.mutable_strategy()->PackFrom(serializedWatermarkStrategyDescriptor);
    }
    else if (Util::instanceOf<const Windowing::IngestionTimeWatermarkStrategyDescriptor>(watermarkStrategyDescriptor))
    {
        auto serializedWatermarkStrategyDescriptor
            = SerializableOperator_WatermarkStrategyDetails_SerializableIngestionTimeWatermarkStrategyDescriptor();
        watermarkStrategyDetails.mutable_strategy()->PackFrom(serializedWatermarkStrategyDescriptor);
    }
    else
    {
        NES_ERROR("OperatorSerializationUtil: Unknown Watermark Strategy Descriptor Type");
        throw std::invalid_argument("Unknown Watermark Strategy Descriptor Type");
    }
}

Windowing::WatermarkStrategyDescriptorPtr OperatorSerializationUtil::deserializeWatermarkStrategyDescriptor(
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

        auto onField = Util::as<NodeFunctionFieldAccess>(
            FunctionSerializationUtil::deserializeFunction(serializedEventTimeWatermarkStrategyDescriptor.onfield()));
        NES_DEBUG("OperatorSerializationUtil:: deserialized field name {}", onField->getFieldName());
        auto eventTimeWatermarkStrategyDescriptor = Windowing::EventTimeWatermarkStrategyDescriptor::create(
            NodeFunctionFieldAccess::create(onField->getFieldName()),
            Windowing::TimeMeasure(serializedEventTimeWatermarkStrategyDescriptor.allowedlateness()),
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

void OperatorSerializationUtil::serializeInputSchema(const OperatorPtr& operatorNode, SerializableOperator& serializedOperator)
{
    NES_TRACE("OperatorSerializationUtil:: serialize input schema");
    if (!NES::Util::instanceOf<BinaryOperator>(operatorNode))
    {
        SchemaSerializationUtil::serializeSchema(
            NES::Util::as<UnaryOperator>(operatorNode)->getInputSchema(), serializedOperator.mutable_inputschema());
    }
    else
    {
        auto binaryOperator = NES::Util::as<BinaryOperator>(operatorNode);
        SchemaSerializationUtil::serializeSchema(binaryOperator->getLeftInputSchema(), serializedOperator.mutable_leftinputschema());
        SchemaSerializationUtil::serializeSchema(binaryOperator->getRightInputSchema(), serializedOperator.mutable_rightinputschema());
    }
}

void OperatorSerializationUtil::deserializeInputSchema(LogicalOperatorPtr operatorNode, const SerializableOperator& serializedOperator)
{
    /// de-serialize operator input schema
    if (!NES::Util::instanceOf<BinaryOperator>(operatorNode))
    {
        NES::Util::as<UnaryOperator>(operatorNode)
            ->setInputSchema(SchemaSerializationUtil::deserializeSchema(serializedOperator.inputschema()));
    }
    else
    {
        auto binaryOperator = NES::Util::as<BinaryOperator>(operatorNode);
        binaryOperator->setLeftInputSchema(SchemaSerializationUtil::deserializeSchema(serializedOperator.leftinputschema()));
        binaryOperator->setRightInputSchema(SchemaSerializationUtil::deserializeSchema(serializedOperator.rightinputschema()));
    }
}

void OperatorSerializationUtil::serializeInferModelOperator(
    const InferModel::LogicalInferModelOperator& inferModelOperator, SerializableOperator& serializedOperator)
{
    /// serialize infer model operator
    NES_TRACE("OperatorSerializationUtil:: serialize to LogicalInferModelOperator");
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

LogicalUnaryOperatorPtr
OperatorSerializationUtil::deserializeInferModelOperator(const SerializableOperator_InferModelDetails& inferModelDetails)
{
    std::vector<NodeFunctionPtr> inputFields;
    std::vector<NodeFunctionPtr> outputFields;

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

    return LogicalOperatorFactory::createInferModelOperator(inferModelDetails.mlfilename(), inputFields, outputFields, getNextOperatorId());
}

} /// namespace NES
