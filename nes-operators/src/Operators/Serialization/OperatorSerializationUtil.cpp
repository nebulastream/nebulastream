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
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Expressions/ExpressionSerializationUtil.hpp>
#include <Expressions/FieldAssignmentExpressionNode.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinDescriptor.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinOperator.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/LogicalIntervalJoinDescriptor.hpp>
#include <Operators/LogicalOperators/LogicalIntervalJoinOperator.hpp>
#include <Operators/LogicalOperators/LogicalLimitOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalOpenCLOperator.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/RenameSourceOperator.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/MonitoringSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/StatisticSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MonitoringSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/TCPSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Operators/LogicalOperators/StatisticCollection/LogicalStatisticWindowOperator.hpp>
#include <Operators/LogicalOperators/UDFs/MapUDF/MapUDFLogicalOperator.hpp>
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
#include <Operators/Serialization/StatisticSerializationUtil.hpp>
#include <Operators/Serialization/UDFSerializationUtil.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Types/SlidingWindow.hpp>
#include <Types/ThresholdWindow.hpp>
#include <Types/TumblingWindow.hpp>
#include <Types/WindowType.hpp>
#include <fstream>
#ifdef ENABLE_OPC_BUILD
#include <Operators/LogicalOperators/Sinks/OPCSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/OPCSourceDescriptor.hpp>
#endif
#ifdef ENABLE_MQTT_BUILD
#include <Operators/LogicalOperators/Sinks/MQTTSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MQTTSourceDescriptor.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <fstream>
#endif
#ifdef ENABLE_KAFKA_BUILD
#include <Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#endif

namespace NES {

SerializableOperator OperatorSerializationUtil::serializeOperator(const OperatorPtr& operatorNode, bool isClientOriginated) {
    NES_TRACE("OperatorSerializationUtil:: serialize operator {}", operatorNode->toString());
    auto serializedOperator = SerializableOperator();
    if (operatorNode->instanceOf<SourceLogicalOperator>()) {
        // serialize source operator
        serializeSourceOperator(*operatorNode->as<SourceLogicalOperator>(), serializedOperator, isClientOriginated);

    } else if (operatorNode->instanceOf<SinkLogicalOperator>()) {
        // serialize sink operator
        serializeSinkOperator(*operatorNode->as<SinkLogicalOperator>(), serializedOperator);

    } else if (operatorNode->instanceOf<LogicalFilterOperator>()) {
        // serialize filter operator
        serializeFilterOperator(*operatorNode->as<LogicalFilterOperator>(), serializedOperator);

    } else if (operatorNode->instanceOf<LogicalProjectionOperator>()) {
        // serialize projection operator
        serializeProjectionOperator(*operatorNode->as<LogicalProjectionOperator>(), serializedOperator);

    } else if (operatorNode->instanceOf<LogicalUnionOperator>()) {
        // serialize union operator
        NES_TRACE("OperatorSerializationUtil:: serialize to LogicalUnionOperator");
        auto unionDetails = SerializableOperator_UnionDetails();
        serializedOperator.mutable_details()->PackFrom(unionDetails);

    } else if (operatorNode->instanceOf<LogicalMapOperator>()) {
        // serialize map operator
        serializeMapOperator(*operatorNode->as<LogicalMapOperator>(), serializedOperator);

    } else if (operatorNode->instanceOf<InferModel::LogicalInferModelOperator>()) {
        // serialize infer model
        serializeInferModelOperator(*operatorNode->as<InferModel::LogicalInferModelOperator>(), serializedOperator);
    } else if (operatorNode->instanceOf<LogicalWindowOperator>()) {
        // serialize window operator
        serializeWindowOperator(*operatorNode->as<LogicalWindowOperator>(), serializedOperator);
    } else if (operatorNode->instanceOf<LogicalJoinOperator>()) {
        // serialize streaming join operator
        serializeJoinOperator(*operatorNode->as<LogicalJoinOperator>(), serializedOperator);
    } else if (operatorNode->instanceOf<Experimental::LogicalBatchJoinOperator>()) {
        // serialize batch join operator
        serializeBatchJoinOperator(*operatorNode->as<Experimental::LogicalBatchJoinOperator>(), serializedOperator);
    } else if (operatorNode->instanceOf<LogicalIntervalJoinOperator>()) {
        //serialize interval join operator
        serializeIntervalJoinOperator(*operatorNode->as<LogicalIntervalJoinOperator>(), serializedOperator);
    } else if (operatorNode->instanceOf<LogicalLimitOperator>()) {
        // serialize limit operator
        serializeLimitOperator(*operatorNode->as<LogicalLimitOperator>(), serializedOperator);

    } else if (operatorNode->instanceOf<WatermarkAssignerLogicalOperator>()) {
        // serialize watermarkAssigner operator
        serializeWatermarkAssignerOperator(*operatorNode->as<WatermarkAssignerLogicalOperator>(), serializedOperator);

    } else if (operatorNode->instanceOf<RenameSourceOperator>()) {
        // Serialize rename source operator
        NES_TRACE("OperatorSerializationUtil:: serialize to RenameSourceOperator");
        auto renameDetails = SerializableOperator_RenameDetails();
        renameDetails.set_newsourcename(operatorNode->as<RenameSourceOperator>()->getNewSourceName());
        serializedOperator.mutable_details()->PackFrom(renameDetails);

    } else if (operatorNode->instanceOf<MapUDFLogicalOperator>()) {
        // Serialize Map Java UDF operator
        serializeJavaUDFOperator<MapUDFLogicalOperator, SerializableOperator_MapJavaUdfDetails>(
            *operatorNode->as<MapUDFLogicalOperator>(),
            serializedOperator);

    } else if (operatorNode->instanceOf<FlatMapUDFLogicalOperator>()) {
        // Serialize FlatMap Java UDF operator
        serializeJavaUDFOperator<FlatMapUDFLogicalOperator, SerializableOperator_FlatMapJavaUdfDetails>(
            *operatorNode->as<FlatMapUDFLogicalOperator>(),
            serializedOperator);

    } else if (operatorNode->instanceOf<LogicalOpenCLOperator>()) {
        // Serialize map udf operator
        serializeOpenCLOperator(*operatorNode->as<LogicalOpenCLOperator>(), serializedOperator);
    } else if (operatorNode->instanceOf<Statistic::LogicalStatisticWindowOperator>()) {
        // Serialize map udf operator
        serializeStatisticWindowOperator(*operatorNode->as<Statistic::LogicalStatisticWindowOperator>(), serializedOperator);
    } else {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not serialize this operator: {}", operatorNode->toString());
    }

    // serialize input schema
    serializeInputSchema(operatorNode, serializedOperator);

    // serialize output schema
    SchemaSerializationUtil::serializeSchema(operatorNode->getOutputSchema(), serializedOperator.mutable_outputschema());

    // serialize operator id
    serializedOperator.set_operatorid(operatorNode->getId().getRawValue());

    // serialize statistic id
    serializedOperator.set_statisticid(operatorNode->getStatisticId());

    // serialize and append children if the node has any
    for (const auto& child : operatorNode->getChildren()) {
        serializedOperator.add_childrenids(child->as<Operator>()->getId().getRawValue());
    }

    // serialize and append origin id
    if (operatorNode->instanceOf<BinaryOperator>()) {
        auto binaryOperator = operatorNode->as<BinaryOperator>();
        for (const auto& originId : binaryOperator->getLeftInputOriginIds()) {
            serializedOperator.add_leftoriginids(originId.getRawValue());
        }
        for (const auto& originId : binaryOperator->getRightInputOriginIds()) {
            serializedOperator.add_rightoriginids(originId.getRawValue());
        }
    } else {
        auto unaryOperator = operatorNode->as<UnaryOperator>();
        for (const auto& originId : unaryOperator->getInputOriginIds()) {
            serializedOperator.add_originids(originId.getRawValue());
        }
    }

    NES_TRACE("OperatorSerializationUtil:: serialize {} to {}",
              operatorNode->toString(),
              serializedOperator.details().type_url());
    return serializedOperator;
}

OperatorPtr OperatorSerializationUtil::deserializeOperator(SerializableOperator serializedOperator) {
    NES_TRACE("OperatorSerializationUtil:: de-serialize {}", serializedOperator.DebugString());
    auto details = serializedOperator.details();
    LogicalOperatorPtr operatorNode;
    if (details.Is<SerializableOperator_SourceDetails>()) {
        // de-serialize source operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to SourceLogicalOperator");
        auto serializedSourceDescriptor = SerializableOperator_SourceDetails();
        details.UnpackTo(&serializedSourceDescriptor);
        operatorNode = deserializeSourceOperator(serializedSourceDescriptor);

    } else if (details.Is<SerializableOperator_SinkDetails>()) {
        // de-serialize sink operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to SinkLogicalOperator");
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails();
        details.UnpackTo(&serializedSinkDescriptor);
        operatorNode = deserializeSinkOperator(serializedSinkDescriptor);

    } else if (details.Is<SerializableOperator_FilterDetails>()) {
        // de-serialize filter operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to FilterLogicalOperator");
        auto serializedFilterOperator = SerializableOperator_FilterDetails();
        details.UnpackTo(&serializedFilterOperator);
        operatorNode = deserializeFilterOperator(serializedFilterOperator);

    } else if (details.Is<SerializableOperator_ProjectionDetails>()) {
        // de-serialize projection operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to ProjectionLogicalOperator");
        auto serializedProjectionOperator = SerializableOperator_ProjectionDetails();
        details.UnpackTo(&serializedProjectionOperator);
        operatorNode = deserializeProjectionOperator(serializedProjectionOperator);

    } else if (details.Is<SerializableOperator_UnionDetails>()) {
        // de-serialize union operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to UnionLogicalOperator");
        auto serializedUnionDescriptor = SerializableOperator_UnionDetails();
        details.UnpackTo(&serializedUnionDescriptor);
        operatorNode = LogicalOperatorFactory::createUnionOperator(getNextOperatorId());

    } else if (details.Is<SerializableOperator_MapDetails>()) {
        // de-serialize map operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to MapLogicalOperator");
        auto serializedMapOperator = SerializableOperator_MapDetails();
        details.UnpackTo(&serializedMapOperator);
        operatorNode = deserializeMapOperator(serializedMapOperator);

    } else if (details.Is<SerializableOperator_InferModelDetails>()) {
        // de-serialize infer model operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to InferModelLogicalOperator");
        auto serializedInferModelOperator = SerializableOperator_InferModelDetails();
        details.UnpackTo(&serializedInferModelOperator);
        operatorNode = deserializeInferModelOperator(serializedInferModelOperator);
    } else if (details.Is<SerializableOperator_WindowDetails>()) {
        // de-serialize window operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to WindowLogicalOperator");
        auto serializedWindowOperator = SerializableOperator_WindowDetails();
        details.UnpackTo(&serializedWindowOperator);
        auto windowNode = deserializeWindowOperator(serializedWindowOperator, getNextOperatorId());
        operatorNode = windowNode;

    } else if (details.Is<SerializableOperator_JoinDetails>()) {
        // de-serialize streaming join operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to JoinLogicalOperator");
        auto serializedJoinOperator = SerializableOperator_JoinDetails();
        details.UnpackTo(&serializedJoinOperator);
        operatorNode = deserializeJoinOperator(serializedJoinOperator, getNextOperatorId());

    } else if (details.Is<SerializableOperator_IntervalJoinDetails>()) {
        // de-serialize streaming interval join operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to IntervalJoinLogicalOperator");
        auto serializedIntervalJoinOperator = SerializableOperator_IntervalJoinDetails();
        details.UnpackTo(&serializedIntervalJoinOperator);
        operatorNode = deserializeIntervalJoinOperator(serializedIntervalJoinOperator, getNextOperatorId());

    } else if (details.Is<SerializableOperator_BatchJoinDetails>()) {
        // de-serialize batch join operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to BatchJoinLogicalOperator");
        auto serializedBatchJoinOperator = SerializableOperator_BatchJoinDetails();
        details.UnpackTo(&serializedBatchJoinOperator);
        operatorNode = deserializeBatchJoinOperator(serializedBatchJoinOperator, getNextOperatorId());

    } else if (details.Is<SerializableOperator_WatermarkStrategyDetails>()) {
        // de-serialize watermark assigner operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to watermarkassigner operator");
        auto serializedWatermarkStrategyDetails = SerializableOperator_WatermarkStrategyDetails();
        details.UnpackTo(&serializedWatermarkStrategyDetails);
        operatorNode = deserializeWatermarkAssignerOperator(serializedWatermarkStrategyDetails);

    } else if (details.Is<SerializableOperator_LimitDetails>()) {
        // de-serialize limit operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to limit operator");
        auto serializedLimitDetails = SerializableOperator_LimitDetails();
        details.UnpackTo(&serializedLimitDetails);
        operatorNode = deserializeLimitOperator(serializedLimitDetails);

    } else if (details.Is<SerializableOperator_RenameDetails>()) {
        // Deserialize rename source operator.
        NES_TRACE("OperatorSerializationUtil:: deserialize to rename source operator");
        auto renameDetails = SerializableOperator_RenameDetails();
        details.UnpackTo(&renameDetails);
        operatorNode = LogicalOperatorFactory::createRenameSourceOperator(renameDetails.newsourcename());

    } else if (details.Is<SerializableOperator_MapJavaUdfDetails>()) {
        NES_TRACE("Deserialize Map Java UDF operator.");
        auto mapJavaUDFDetails = SerializableOperator_MapJavaUdfDetails();
        details.UnpackTo(&mapJavaUDFDetails);
        operatorNode = deserializeMapJavaUDFOperator(mapJavaUDFDetails);

    } else if (details.Is<SerializableOperator_FlatMapJavaUdfDetails>()) {
        NES_TRACE("Deserialize FlatMap Java UDF operator.");
        auto flatMapJavaUDFDetails = SerializableOperator_FlatMapJavaUdfDetails();
        details.UnpackTo(&flatMapJavaUDFDetails);
        operatorNode = deserializeFlatMapJavaUDFOperator(flatMapJavaUDFDetails);

    } else if (details.Is<SerializableOperator_OpenCLOperatorDetails>()) {
        NES_TRACE("Deserialize Open CL operator.");
        auto openCLDetails = SerializableOperator_OpenCLOperatorDetails();
        details.UnpackTo(&openCLDetails);
        operatorNode = deserializeOpenCLOperator(openCLDetails);

    } else if (details.Is<SerializableOperator_StatisticWindowDetails>()) {
        NES_TRACE("Deserialize Statistic Window Operator.");
        SerializableOperator_StatisticWindowDetails statisticWindowDetails;
        details.UnpackTo(&statisticWindowDetails);
        operatorNode = deserializeStatisticWindowOperator(statisticWindowDetails);
    } else {
        NES_THROW_RUNTIME_ERROR("OperatorSerializationUtil: could not de-serialize this serialized operator: ");
    }

    // de-serialize operator output schema
    operatorNode->setOutputSchema(SchemaSerializationUtil::deserializeSchema(serializedOperator.outputschema()));
    // deserialize input schema
    deserializeInputSchema(operatorNode, serializedOperator);

    if (details.Is<SerializableOperator_JoinDetails>()) {
        auto joinOp = operatorNode->as<LogicalJoinOperator>();
        joinOp->getJoinDefinition()->updateSourceTypes(joinOp->getLeftInputSchema(), joinOp->getRightInputSchema());
        joinOp->getJoinDefinition()->updateOutputDefinition(joinOp->getOutputSchema());
    }

    if (details.Is<SerializableOperator_BatchJoinDetails>()) {
        auto joinOp = operatorNode->as<Experimental::LogicalBatchJoinOperator>();
        joinOp->getBatchJoinDefinition()->updateInputSchemas(joinOp->getLeftInputSchema(), joinOp->getRightInputSchema());
        joinOp->getBatchJoinDefinition()->updateOutputDefinition(joinOp->getOutputSchema());
    }

    operatorNode->setStatisticId(serializedOperator.statisticid());

    // de-serialize and append origin id
    if (operatorNode->instanceOf<BinaryOperator>()) {
        auto binaryOperator = operatorNode->as<BinaryOperator>();
        std::vector<OriginId> leftOriginIds;
        for (const auto& originId : serializedOperator.leftoriginids()) {
            leftOriginIds.emplace_back(originId);
        }
        binaryOperator->setLeftInputOriginIds(leftOriginIds);
        std::vector<OriginId> rightOriginIds;
        for (const auto& originId : serializedOperator.rightoriginids()) {
            rightOriginIds.emplace_back(originId);
        }
        binaryOperator->setRightInputOriginIds(rightOriginIds);
    } else {
        auto unaryOperator = operatorNode->as<UnaryOperator>();
        std::vector<OriginId> originIds;
        for (const auto& originId : serializedOperator.originids()) {
            originIds.emplace_back(originId);
        }
        unaryOperator->setInputOriginIds(originIds);
    }
    NES_TRACE("OperatorSerializationUtil:: de-serialize {} to {}", serializedOperator.DebugString(), operatorNode->toString());
    return operatorNode;
}

void OperatorSerializationUtil::serializeSourceOperator(const SourceLogicalOperator& sourceOperator,
                                                        SerializableOperator& serializedOperator,
                                                        bool isClientOriginated) {

    NES_TRACE("OperatorSerializationUtil:: serialize to SourceLogicalOperator");

    auto sourceDetails = SerializableOperator_SourceDetails();
    auto sourceDescriptor = sourceOperator.getSourceDescriptor();
    serializeSourceDescriptor(*sourceDescriptor, sourceDetails, isClientOriginated);
    sourceDetails.set_sourceoriginid(sourceOperator.getOriginId().getRawValue());

    serializedOperator.mutable_details()->PackFrom(sourceDetails);
}

LogicalUnaryOperatorPtr
OperatorSerializationUtil::deserializeSourceOperator(const SerializableOperator_SourceDetails& sourceDetails) {
    auto sourceDescriptor = deserializeSourceDescriptor(sourceDetails);
    return LogicalOperatorFactory::createSourceOperator(sourceDescriptor,
                                                        getNextOperatorId(),
                                                        OriginId(sourceDetails.sourceoriginid()));
}

void OperatorSerializationUtil::serializeFilterOperator(const LogicalFilterOperator& filterOperator,
                                                        SerializableOperator& serializedOperator) {
    NES_TRACE("OperatorSerializationUtil:: serialize to LogicalFilterOperator");
    auto filterDetails = SerializableOperator_FilterDetails();
    ExpressionSerializationUtil::serializeExpression(filterOperator.getPredicate(), filterDetails.mutable_predicate());
    serializedOperator.mutable_details()->PackFrom(filterDetails);
}

LogicalUnaryOperatorPtr
OperatorSerializationUtil::deserializeFilterOperator(const SerializableOperator_FilterDetails& filterDetails) {
    auto filterExpression = ExpressionSerializationUtil::deserializeExpression(filterDetails.predicate());
    return LogicalOperatorFactory::createFilterOperator(filterExpression, getNextOperatorId());
}

void OperatorSerializationUtil::serializeProjectionOperator(const LogicalProjectionOperator& projectionOperator,
                                                            SerializableOperator& serializedOperator) {
    NES_TRACE("OperatorSerializationUtil:: serialize to LogicalProjectionOperator");
    auto projectionDetail = SerializableOperator_ProjectionDetails();
    for (auto& exp : projectionOperator.getExpressions()) {
        auto* mutableExpression = projectionDetail.mutable_expression()->Add();
        ExpressionSerializationUtil::serializeExpression(exp, mutableExpression);
    }

    serializedOperator.mutable_details()->PackFrom(projectionDetail);
}

LogicalUnaryOperatorPtr
OperatorSerializationUtil::deserializeProjectionOperator(const SerializableOperator_ProjectionDetails& projectionDetails) {
    // serialize and append children if the node has any
    std::vector<ExpressionNodePtr> exps;
    for (const auto& serializedExpression : projectionDetails.expression()) {
        auto projectExpression = ExpressionSerializationUtil::deserializeExpression(serializedExpression);
        exps.push_back(projectExpression);
    }

    return LogicalOperatorFactory::createProjectionOperator(exps, getNextOperatorId());
}

void OperatorSerializationUtil::serializeSinkOperator(const SinkLogicalOperator& sinkOperator,
                                                      SerializableOperator& serializedOperator) {
    NES_TRACE("OperatorSerializationUtil:: serialize to SinkLogicalOperator");
    auto sinkDetails = SerializableOperator_SinkDetails();
    auto sinkDescriptor = sinkOperator.getSinkDescriptor();
    serializeSinkDescriptor(*sinkDescriptor, sinkDetails, sinkOperator.getInputOriginIds().size());
    serializedOperator.mutable_details()->PackFrom(sinkDetails);
}

LogicalUnaryOperatorPtr OperatorSerializationUtil::deserializeSinkOperator(const SerializableOperator_SinkDetails& sinkDetails) {
    auto sinkDescriptor = deserializeSinkDescriptor(sinkDetails);
    return LogicalOperatorFactory::createSinkOperator(sinkDescriptor, INVALID_WORKER_NODE_ID, getNextOperatorId());
}

void OperatorSerializationUtil::serializeMapOperator(const LogicalMapOperator& mapOperator,
                                                     SerializableOperator& serializedOperator) {
    NES_TRACE("OperatorSerializationUtil:: serialize to LogicalMapOperator");
    auto mapDetails = SerializableOperator_MapDetails();
    ExpressionSerializationUtil::serializeExpression(mapOperator.getMapExpression(), mapDetails.mutable_expression());
    serializedOperator.mutable_details()->PackFrom(mapDetails);
}

LogicalUnaryOperatorPtr OperatorSerializationUtil::deserializeMapOperator(const SerializableOperator_MapDetails& mapDetails) {
    auto fieldAssignmentExpression = ExpressionSerializationUtil::deserializeExpression(mapDetails.expression());
    return LogicalOperatorFactory::createMapOperator(fieldAssignmentExpression->as<FieldAssignmentExpressionNode>(),
                                                     getNextOperatorId());
}

void OperatorSerializationUtil::serializeWindowOperator(const WindowOperator& windowOperator,
                                                        SerializableOperator& serializedOperator) {
    NES_TRACE("OperatorSerializationUtil:: serialize to WindowOperator");
    auto windowDetails = SerializableOperator_WindowDetails();
    auto windowDefinition = windowOperator.getWindowDefinition();

    if (windowDefinition->isKeyed()) {
        for (const auto& key : windowDefinition->getKeys()) {
            auto expression = windowDetails.mutable_keys()->Add();
            ExpressionSerializationUtil::serializeExpression(key, expression);
        }
    }
    windowDetails.set_origin(windowOperator.getOriginId().getRawValue());
    windowDetails.set_allowedlateness(windowDefinition->getAllowedLateness());
    auto windowType = windowDefinition->getWindowType();

    if (windowType->instanceOf<Windowing::TimeBasedWindowType>()) {
        auto timeBasedWindowType = windowType->as<Windowing::TimeBasedWindowType>();
        auto timeCharacteristic = timeBasedWindowType->getTimeCharacteristic();
        auto timeCharacteristicDetails = SerializableOperator_TimeCharacteristic();
        if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::Type::EventTime) {
            timeCharacteristicDetails.set_type(SerializableOperator_TimeCharacteristic_Type_EventTime);
            timeCharacteristicDetails.set_field(timeCharacteristic->getField()->getName());
        } else if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::Type::IngestionTime) {
            timeCharacteristicDetails.set_type(SerializableOperator_TimeCharacteristic_Type_IngestionTime);
        } else {
            NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Characteristic");
        }
        timeCharacteristicDetails.set_multiplier(timeCharacteristic->getTimeUnit().getMillisecondsConversionMultiplier());

        if (windowType->instanceOf<Windowing::TumblingWindow>()) {
            auto tumblingWindow = windowType->as<Windowing::TumblingWindow>();
            auto tumblingWindowDetails = SerializableOperator_TumblingWindow();
            tumblingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
            tumblingWindowDetails.set_size(tumblingWindow->getSize().getTime());
            windowDetails.mutable_windowtype()->PackFrom(tumblingWindowDetails);
        } else if (windowType->instanceOf<Windowing::SlidingWindow>()) {
            auto slidingWindow = windowType->as<Windowing::SlidingWindow>();
            auto slidingWindowDetails = SerializableOperator_SlidingWindow();
            slidingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
            slidingWindowDetails.set_size(slidingWindow->getSize().getTime());
            slidingWindowDetails.set_slide(slidingWindow->getSlide().getTime());
            windowDetails.mutable_windowtype()->PackFrom(slidingWindowDetails);
        } else {
            NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Type");
        }
    } else if (windowType->instanceOf<Windowing::ContentBasedWindowType>()) {
        auto contentBasedWindowType = windowType->as<Windowing::ContentBasedWindowType>();
        if (contentBasedWindowType->getContentBasedSubWindowType()
            == Windowing::ContentBasedWindowType::ContentBasedSubWindowType::THRESHOLDWINDOW) {
            auto thresholdWindow = std::dynamic_pointer_cast<Windowing::ThresholdWindow>(windowType);
            auto thresholdWindowDetails = SerializableOperator_ThresholdWindow();
            ExpressionSerializationUtil::serializeExpression(thresholdWindow->getPredicate(),
                                                             thresholdWindowDetails.mutable_predicate());
            thresholdWindowDetails.set_minimumcount(thresholdWindow->getMinimumCount());
            windowDetails.mutable_windowtype()->PackFrom(thresholdWindowDetails);
        } else {
            NES_ERROR("OperatorSerializationUtil: Cant serialize this content based window Type");
        }
    }

    // serialize aggregation
    for (auto aggregation : windowDefinition->getWindowAggregation()) {
        auto* windowAggregation = windowDetails.mutable_windowaggregations()->Add();
        ExpressionSerializationUtil::serializeExpression(aggregation->as(), windowAggregation->mutable_asfield());
        ExpressionSerializationUtil::serializeExpression(aggregation->on(), windowAggregation->mutable_onfield());

        using enum Windowing::WindowAggregationDescriptor::Type;
        switch (aggregation->getType()) {
            case Count: windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_COUNT); break;
            case Max: windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_MAX); break;
            case Min: windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_MIN); break;
            case Sum: windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_SUM); break;
            case Avg: windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_AVG); break;
            case Median: windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_MEDIAN); break;
            default: NES_FATAL_ERROR("OperatorSerializationUtil: could not cast aggregation type");
        }
    }

    serializedOperator.mutable_details()->PackFrom(windowDetails);
}

LogicalUnaryOperatorPtr
OperatorSerializationUtil::deserializeWindowOperator(const SerializableOperator_WindowDetails& windowDetails,
                                                     OperatorId operatorId) {
    const auto& serializedWindowAggregations = windowDetails.windowaggregations();
    const auto& serializedWindowType = windowDetails.windowtype();

    std::vector<Windowing::WindowAggregationDescriptorPtr> aggregation;
    for (const auto& serializedWindowAggregation : serializedWindowAggregations) {
        auto onField = ExpressionSerializationUtil::deserializeExpression(serializedWindowAggregation.onfield())
                           ->as<FieldAccessExpressionNode>();
        auto asField = ExpressionSerializationUtil::deserializeExpression(serializedWindowAggregation.asfield())
                           ->as<FieldAccessExpressionNode>();
        if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_SUM) {
            aggregation.emplace_back(Windowing::SumAggregationDescriptor::create(onField, asField));
        } else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_MAX) {
            aggregation.emplace_back(Windowing::MaxAggregationDescriptor::create(onField, asField));
        } else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_MIN) {
            aggregation.emplace_back(Windowing::MinAggregationDescriptor::create(onField, asField));
        } else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_COUNT) {
            aggregation.emplace_back(Windowing::CountAggregationDescriptor::create(onField, asField));
        } else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_AVG) {
            aggregation.emplace_back(Windowing::AvgAggregationDescriptor::create(onField, asField));
        } else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_MEDIAN) {
            aggregation.emplace_back(Windowing::MedianAggregationDescriptor::create(onField, asField));
        } else {
            NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window aggregation: {}",
                            serializedWindowAggregation.DebugString());
        }
    }

    Windowing::WindowTypePtr window;
    if (serializedWindowType.Is<SerializableOperator_TumblingWindow>()) {
        auto serializedTumblingWindow = SerializableOperator_TumblingWindow();
        serializedWindowType.UnpackTo(&serializedTumblingWindow);
        auto serializedTimeCharacteristic = serializedTumblingWindow.timecharacteristic();
        auto multiplier = serializedTimeCharacteristic.multiplier();
        if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_EventTime) {
            auto field = FieldAccessExpressionNode::create(serializedTimeCharacteristic.field());
            window = Windowing::TumblingWindow::of(
                Windowing::TimeCharacteristic::createEventTime(field, Windowing::TimeUnit(multiplier)),
                Windowing::TimeMeasure(serializedTumblingWindow.size()));
        } else if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_IngestionTime) {
            window = Windowing::TumblingWindow::of(Windowing::TimeCharacteristic::createIngestionTime(),
                                                   Windowing::TimeMeasure(serializedTumblingWindow.size()));
        } else {
            NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window time characteristic: {}",
                            serializedTimeCharacteristic.DebugString());
        }
    } else if (serializedWindowType.Is<SerializableOperator_SlidingWindow>()) {
        auto serializedSlidingWindow = SerializableOperator_SlidingWindow();
        serializedWindowType.UnpackTo(&serializedSlidingWindow);
        auto serializedTimeCharacteristic = serializedSlidingWindow.timecharacteristic();
        auto multiplier = serializedTimeCharacteristic.multiplier();
        if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_EventTime) {
            auto field = FieldAccessExpressionNode::create(serializedTimeCharacteristic.field());
            window = Windowing::SlidingWindow::of(
                Windowing::TimeCharacteristic::createEventTime(field, Windowing::TimeUnit(multiplier)),
                Windowing::TimeMeasure(serializedSlidingWindow.size()),
                Windowing::TimeMeasure(serializedSlidingWindow.slide()));
        } else if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_IngestionTime) {
            window = Windowing::SlidingWindow::of(Windowing::TimeCharacteristic::createIngestionTime(),
                                                  Windowing::TimeMeasure(serializedSlidingWindow.size()),
                                                  Windowing::TimeMeasure(serializedSlidingWindow.slide()));
        } else {
            NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window time characteristic: {}",
                            serializedTimeCharacteristic.DebugString());
        }
    } else if (serializedWindowType.Is<SerializableOperator_ThresholdWindow>()) {
        auto serializedThresholdWindow = SerializableOperator_ThresholdWindow();
        serializedWindowType.UnpackTo(&serializedThresholdWindow);
        auto thresholdExpression = ExpressionSerializationUtil::deserializeExpression(serializedThresholdWindow.predicate());
        auto thresholdMinimumCount = serializedThresholdWindow.minimumcount();
        window = Windowing::ThresholdWindow::of(thresholdExpression, thresholdMinimumCount);
    } else {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window type: {}", serializedWindowType.DebugString());
    }

    auto allowedLateness = windowDetails.allowedlateness();
    std::vector<FieldAccessExpressionNodePtr> keyAccessExpression;
    for (auto& key : windowDetails.keys()) {
        keyAccessExpression.emplace_back(
            ExpressionSerializationUtil::deserializeExpression(key)->as<FieldAccessExpressionNode>());
    }
    auto windowDef = Windowing::LogicalWindowDescriptor::create(keyAccessExpression, aggregation, window, allowedLateness);
    windowDef->setOriginId(OriginId(windowDetails.origin()));
    return LogicalOperatorFactory::createWindowOperator(windowDef, operatorId);
}

void OperatorSerializationUtil::serializeJoinOperator(const LogicalJoinOperator& joinOperator,
                                                      SerializableOperator& serializedOperator) {
    NES_TRACE("OperatorSerializationUtil:: serialize to LogicalJoinOperator");
    auto joinDetails = SerializableOperator_JoinDetails();
    auto joinDefinition = joinOperator.getJoinDefinition();

    ExpressionSerializationUtil::serializeExpression(joinDefinition->getJoinExpression(), joinDetails.mutable_joinexpression());

    auto windowType = joinDefinition->getWindowType();
    auto timeBasedWindowType = windowType->as<Windowing::TimeBasedWindowType>();
    auto timeCharacteristic = timeBasedWindowType->getTimeCharacteristic();
    auto timeCharacteristicDetails = SerializableOperator_TimeCharacteristic();
    timeCharacteristicDetails.set_multiplier(timeCharacteristic->getTimeUnit().getMillisecondsConversionMultiplier());
    if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::Type::EventTime) {
        timeCharacteristicDetails.set_type(SerializableOperator_TimeCharacteristic_Type_EventTime);
        timeCharacteristicDetails.set_field(timeCharacteristic->getField()->getName());
    } else if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::Type::IngestionTime) {
        timeCharacteristicDetails.set_type(SerializableOperator_TimeCharacteristic_Type_IngestionTime);
    } else {
        NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Characteristic");
    }
    if (windowType->instanceOf<Windowing::TumblingWindow>()) {
        auto tumblingWindow = windowType->as<Windowing::TumblingWindow>();
        auto tumblingWindowDetails = SerializableOperator_TumblingWindow();
        tumblingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
        tumblingWindowDetails.set_size(tumblingWindow->getSize().getTime());
        joinDetails.mutable_windowtype()->PackFrom(tumblingWindowDetails);
    } else if (windowType->instanceOf<Windowing::SlidingWindow>()) {
        auto slidingWindow = windowType->as<Windowing::SlidingWindow>();
        auto slidingWindowDetails = SerializableOperator_SlidingWindow();
        slidingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
        slidingWindowDetails.set_size(slidingWindow->getSize().getTime());
        slidingWindowDetails.set_slide(slidingWindow->getSlide().getTime());
        joinDetails.mutable_windowtype()->PackFrom(slidingWindowDetails);
    } else {
        NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Type");
    }

    joinDetails.set_numberofinputedgesleft(joinDefinition->getNumberOfInputEdgesLeft());
    joinDetails.set_numberofinputedgesright(joinDefinition->getNumberOfInputEdgesRight());
    joinDetails.set_windowstartfieldname(joinOperator.getWindowStartFieldName());
    joinDetails.set_windowendfieldname(joinOperator.getWindowEndFieldName());
    joinDetails.set_origin(joinOperator.getOutputOriginIds()[0].getRawValue());

    if (joinDefinition->getJoinType() == Join::LogicalJoinDescriptor::JoinType::INNER_JOIN) {
        joinDetails.mutable_jointype()->set_jointype(SerializableOperator_JoinDetails_JoinTypeCharacteristic_JoinType_INNER_JOIN);
    } else if (joinDefinition->getJoinType() == Join::LogicalJoinDescriptor::JoinType::CARTESIAN_PRODUCT) {
        joinDetails.mutable_jointype()->set_jointype(
            SerializableOperator_JoinDetails_JoinTypeCharacteristic_JoinType_CARTESIAN_PRODUCT);
    }

    serializedOperator.mutable_details()->PackFrom(joinDetails);
}

LogicalJoinOperatorPtr OperatorSerializationUtil::deserializeJoinOperator(const SerializableOperator_JoinDetails& joinDetails,
                                                                          OperatorId operatorId) {
    const auto& serializedWindowType = joinDetails.windowtype();
    const auto& serializedJoinType = joinDetails.jointype();
    // check which jointype is set
    // default: JoinType::INNER_JOIN
    Join::LogicalJoinDescriptor::JoinType joinType = Join::LogicalJoinDescriptor::JoinType::INNER_JOIN;
    // with Cartesian Product is set, change join type
    if (serializedJoinType.jointype() == SerializableOperator_JoinDetails_JoinTypeCharacteristic_JoinType_CARTESIAN_PRODUCT) {
        joinType = Join::LogicalJoinDescriptor::JoinType::CARTESIAN_PRODUCT;
    }

    Windowing::WindowTypePtr window;
    if (serializedWindowType.Is<SerializableOperator_TumblingWindow>()) {
        auto serializedTumblingWindow = SerializableOperator_TumblingWindow();
        serializedWindowType.UnpackTo(&serializedTumblingWindow);
        const auto& serializedTimeCharacteristic = serializedTumblingWindow.timecharacteristic();
        auto multiplier = serializedTimeCharacteristic.multiplier();
        if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_EventTime) {
            auto field = FieldAccessExpressionNode::create(serializedTimeCharacteristic.field());
            window = Windowing::TumblingWindow::of(
                Windowing::TimeCharacteristic::createEventTime(field, Windowing::TimeUnit(multiplier)),
                Windowing::TimeMeasure(serializedTumblingWindow.size()));
        } else if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_IngestionTime) {
            window = Windowing::TumblingWindow::of(Windowing::TimeCharacteristic::createIngestionTime(),
                                                   Windowing::TimeMeasure(serializedTumblingWindow.size()));
        } else {
            NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window time characteristic: {}",
                            serializedTimeCharacteristic.DebugString());
        }
    } else if (serializedWindowType.Is<SerializableOperator_SlidingWindow>()) {
        auto serializedSlidingWindow = SerializableOperator_SlidingWindow();
        serializedWindowType.UnpackTo(&serializedSlidingWindow);
        const auto& serializedTimeCharacteristic = serializedSlidingWindow.timecharacteristic();
        auto multiplier = serializedTimeCharacteristic.multiplier();
        if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_EventTime) {
            auto field = FieldAccessExpressionNode::create(serializedTimeCharacteristic.field());
            window = Windowing::SlidingWindow::of(
                Windowing::TimeCharacteristic::createEventTime(field, Windowing::TimeUnit(multiplier)),
                Windowing::TimeMeasure(serializedSlidingWindow.size()),
                Windowing::TimeMeasure(serializedSlidingWindow.slide()));
        } else if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_IngestionTime) {
            window = Windowing::SlidingWindow::of(Windowing::TimeCharacteristic::createIngestionTime(),
                                                  Windowing::TimeMeasure(serializedSlidingWindow.size()),
                                                  Windowing::TimeMeasure(serializedSlidingWindow.slide()));
        } else {
            NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window time characteristic: {}",
                            serializedTimeCharacteristic.DebugString());
        }
    } else {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window type: {}", serializedWindowType.DebugString());
    }

    LogicalOperatorPtr ptr;
    auto serializedJoinExpression = joinDetails.joinexpression();
    auto joinExpression = ExpressionSerializationUtil::deserializeExpression(serializedJoinExpression);

    auto joinDefinition = Join::LogicalJoinDescriptor::create(joinExpression,
                                                              window,
                                                              joinDetails.numberofinputedgesleft(),
                                                              joinDetails.numberofinputedgesright(),
                                                              joinType);
    auto joinOperator = LogicalOperatorFactory::createJoinOperator(joinDefinition, operatorId)->as<LogicalJoinOperator>();
    joinOperator->setWindowStartEndKeyFieldName(joinDetails.windowstartfieldname(), joinDetails.windowendfieldname());
    joinOperator->setOriginId(OriginId(joinDetails.origin()));
    return joinOperator;

    //TODO: enable distrChar for distributed joins
    //    if (distrChar.distr() == SerializableOperator_JoinDetails_DistributionCharacteristic_Distribution_Complete) {
    //        return LogicalOperatorFactory::createCentralWindowSpecializedOperator(windowDef, operatorId)->as<CentralWindowOperator>();
    //    } else if (distrChar.distr() == SerializableOperator_JoinDetails_DistributionCharacteristic_Distribution_Combining) {
    //        return LogicalOperatorFactory::createWindowComputationSpecializedOperator(windowDef, operatorId)->as<WindowComputationOperator>();
    //    } else if (distrChar.distr() == SerializableOperator_JoinDetails_DistributionCharacteristic_Distribution_Slicing) {
    //        return LogicalOperatorFactory::createSliceCreationSpecializedOperator(windowDef, operatorId)->as<SliceCreationOperator>();
    //    } else {
    //        NES_NOT_IMPLEMENTED();
    //    }
}

void OperatorSerializationUtil::serializeBatchJoinOperator(const Experimental::LogicalBatchJoinOperator& joinOperator,
                                                           SerializableOperator& serializedOperator) {
    NES_TRACE("OperatorSerializationUtil:: serialize to LogicalBatchJoinOperator");

    auto joinDetails = SerializableOperator_BatchJoinDetails();
    auto joinDefinition = joinOperator.getBatchJoinDefinition();

    ExpressionSerializationUtil::serializeExpression(joinDefinition->getBuildJoinKey(), joinDetails.mutable_onbuildkey());
    ExpressionSerializationUtil::serializeExpression(joinDefinition->getProbeJoinKey(), joinDetails.mutable_onprobekey());

    joinDetails.set_numberofinputedgesbuild(joinDefinition->getNumberOfInputEdgesBuild());
    joinDetails.set_numberofinputedgesprobe(joinDefinition->getNumberOfInputEdgesProbe());

    serializedOperator.mutable_details()->PackFrom(joinDetails);
}

Experimental::LogicalBatchJoinOperatorPtr
OperatorSerializationUtil::deserializeBatchJoinOperator(const SerializableOperator_BatchJoinDetails& joinDetails,
                                                        OperatorId operatorId) {
    auto buildKeyAccessExpression =
        ExpressionSerializationUtil::deserializeExpression(joinDetails.onbuildkey())->as<FieldAccessExpressionNode>();
    auto probeKeyAccessExpression =
        ExpressionSerializationUtil::deserializeExpression(joinDetails.onprobekey())->as<FieldAccessExpressionNode>();
    auto joinDefinition = Join::Experimental::LogicalBatchJoinDescriptor::create(buildKeyAccessExpression,
                                                                                 probeKeyAccessExpression,
                                                                                 joinDetails.numberofinputedgesprobe(),
                                                                                 joinDetails.numberofinputedgesbuild());
    auto retValue =
        LogicalOperatorFactory::createBatchJoinOperator(joinDefinition, operatorId)->as<Experimental::LogicalBatchJoinOperator>();
    return retValue;
}

void OperatorSerializationUtil::serializeIntervalJoinOperator(const LogicalIntervalJoinOperator& intervalJoinOperator,
                                                              SerializableOperator& serializedOperator) {
    NES_TRACE("OperatorSerializationUtil:: serialize to LogicalIntervalJoinOperator");
    auto joinDetails = SerializableOperator_IntervalJoinDetails();
    auto joinDefinition = intervalJoinOperator.getIntervalJoinDefinition();

    ExpressionSerializationUtil::serializeExpression(joinDefinition->getJoinExpression(), joinDetails.mutable_joinexpression());

    auto timeCharacteristic = joinDefinition->getTimeCharacteristic();
    auto timeCharacteristicDetails = SerializableOperator_TimeCharacteristic();
    timeCharacteristicDetails.set_multiplier(timeCharacteristic->getTimeUnit().getMillisecondsConversionMultiplier());
    if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::Type::EventTime) {
        timeCharacteristicDetails.set_type(SerializableOperator_TimeCharacteristic_Type_EventTime);
        timeCharacteristicDetails.set_field(timeCharacteristic->getField()->getName());
    } else {
        NES_ERROR("OperatorSerializationUtil: Can't serialize TimeCharacteristic");
    }
    joinDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);

    joinDetails.set_lowerbound(joinDefinition->getLowerBound());
    joinDetails.set_upperbound(joinDefinition->getUpperBound());

    serializedOperator.mutable_details()->PackFrom(joinDetails);
}

LogicalIntervalJoinOperatorPtr
OperatorSerializationUtil::deserializeIntervalJoinOperator(const SerializableOperator_IntervalJoinDetails& joinDetails,
                                                           OperatorId operatorId) {

    const auto& serializedTimeCharacteristic = joinDetails.timecharacteristic();
    auto multiplier = serializedTimeCharacteristic.multiplier();

    LogicalIntervalJoinOperatorPtr joinOperator;

    if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_EventTime) {
        auto field = FieldAccessExpressionNode::create(serializedTimeCharacteristic.field());
        auto timeCharacteristic = Windowing::TimeCharacteristic::createEventTime(field, Windowing::TimeUnit(multiplier));

        LogicalOperatorPtr ptr;
        auto serializedJoinExpression = joinDetails.joinexpression();
        auto joinExpression =
            ExpressionSerializationUtil::deserializeExpression(serializedJoinExpression)->as<BinaryExpressionNode>();

        auto joinDefinition = Join::LogicalIntervalJoinDescriptor::create(joinExpression,
                                                                          timeCharacteristic,
                                                                          joinDetails.lowerbound(),
                                                                          joinDetails.upperbound());
        joinOperator =
            LogicalOperatorFactory::createIntervalJoinOperator(joinDefinition, operatorId)->as<LogicalIntervalJoinOperator>();

    } else {
        NES_THROW_RUNTIME_ERROR("The IntervalJoin implementation only supports event time queries, please check your query. ");
    }
    return joinOperator;
}

void OperatorSerializationUtil::serializeSourceDescriptor(const SourceDescriptor& sourceDescriptor,
                                                          SerializableOperator_SourceDetails& sourceDetails,
                                                          bool isClientOriginated) {
    NES_TRACE("OperatorSerializationUtil:: serialize to SourceDescriptor");
    NES_DEBUG("OperatorSerializationUtil:: serialize to SourceDescriptor with ={}", sourceDescriptor.toString());
    if (sourceDescriptor.instanceOf<const ZmqSourceDescriptor>()) {
        // serialize zmq source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableZMQSourceDescriptor");
        auto zmqSourceDescriptor = sourceDescriptor.as<const ZmqSourceDescriptor>();
        auto zmqSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableZMQSourceDescriptor();
        zmqSerializedSourceDescriptor.set_host(zmqSourceDescriptor->getHost());
        zmqSerializedSourceDescriptor.set_port(zmqSourceDescriptor->getPort());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(zmqSourceDescriptor->getSchema(),
                                                 zmqSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails.mutable_sourcedescriptor()->PackFrom(zmqSerializedSourceDescriptor);
    }
#ifdef ENABLE_MQTT_BUILD
    else if (sourceDescriptor.instanceOf<const MQTTSourceDescriptor>()) {
        // serialize MQTT source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableMQTTSourceDescriptor");
        auto mqttSourceDescriptor = sourceDescriptor.as<const MQTTSourceDescriptor>();
        //init serializable source config
        auto serializedPhysicalSourceType = new SerializablePhysicalSourceType();
        serializedPhysicalSourceType->set_sourcetype(mqttSourceDescriptor->getSourceConfigPtr()->getSourceTypeAsString());
        //init serializable mqtt source config
        auto mqttSerializedSourceConfig = SerializablePhysicalSourceType_SerializableMQTTSourceType();
        mqttSerializedSourceConfig.set_clientid(mqttSourceDescriptor->getSourceConfigPtr()->getClientId()->getValue());
        mqttSerializedSourceConfig.set_url(mqttSourceDescriptor->getSourceConfigPtr()->getUrl()->getValue());
        mqttSerializedSourceConfig.set_username(mqttSourceDescriptor->getSourceConfigPtr()->getUserName()->getValue());
        mqttSerializedSourceConfig.set_topic(mqttSourceDescriptor->getSourceConfigPtr()->getTopic()->getValue());
        mqttSerializedSourceConfig.set_qos(mqttSourceDescriptor->getSourceConfigPtr()->getQos()->getValue());
        mqttSerializedSourceConfig.set_cleansession(mqttSourceDescriptor->getSourceConfigPtr()->getCleanSession()->getValue());
        mqttSerializedSourceConfig.set_flushintervalms(
            mqttSourceDescriptor->getSourceConfigPtr()->getFlushIntervalMS()->getValue());
        switch (mqttSourceDescriptor->getSourceConfigPtr()->getInputFormat()->getValue()) {
            case Configurations::InputFormat::JSON:
                mqttSerializedSourceConfig.set_inputformat(SerializablePhysicalSourceType_InputFormat_JSON);
                break;
            case Configurations::InputFormat::CSV:
                mqttSerializedSourceConfig.set_inputformat(SerializablePhysicalSourceType_InputFormat_CSV);
                break;
            case Configurations::InputFormat::NES_BINARY: NES_NOT_IMPLEMENTED();
        }
        serializedPhysicalSourceType->mutable_specificphysicalsourcetype()->PackFrom(mqttSerializedSourceConfig);
        //init serializable mqtt source descriptor
        auto mqttSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableMQTTSourceDescriptor();
        mqttSerializedSourceDescriptor.set_allocated_physicalsourcetype(serializedPhysicalSourceType);
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(mqttSourceDescriptor->getSchema(),
                                                 mqttSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails.mutable_sourcedescriptor()->PackFrom(mqttSerializedSourceDescriptor);
    }
#endif
#ifdef ENABLE_OPC_BUILD
    else if (sourceDescriptor.instanceOf<OPCSourceDescriptor>()) {
        // serialize opc source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableOPCSourceDescriptor");
        auto opcSourceDescriptor = sourceDescriptor.as<const OPCSourceDescriptor>();
        auto opcSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableOPCSourceDescriptor();
        char* ident = (char*) UA_malloc(sizeof(char) * opcSourceDescriptor->getNodeId().identifier.string.length + 1);
        memcpy(ident,
               opcSourceDescriptor->getNodeId().identifier.string.data,
               opcSourceDescriptor->getNodeId().identifier.string.length);
        ident[opcSourceDescriptor->getNodeId().identifier.string.length] = '\0';
        opcSerializedSourceDescriptor.set_identifier(ident);
        opcSerializedSourceDescriptor.set_url(opcSourceDescriptor->getUrl());
        opcSerializedSourceDescriptor.set_namespaceindex(opcSourceDescriptor->getNodeId().namespaceIndex);
        opcSerializedSourceDescriptor.set_identifiertype(opcSourceDescriptor->getNodeId().identifierType);
        opcSerializedSourceDescriptor.set_user(opcSourceDescriptor->getUser());
        opcSerializedSourceDescriptor.set_password(opcSourceDescriptor->getPassword());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(opcSourceDescriptor->getSchema(),
                                                 opcSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails.mutable_sourcedescriptor()->PackFrom(opcSerializedSourceDescriptor);
    }
#endif
    else if (sourceDescriptor.instanceOf<const TCPSourceDescriptor>()) {
        // serialize TCP source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableTCPSourceDescriptor");
        auto tcpSourceDescriptor = sourceDescriptor.as<const TCPSourceDescriptor>();
        //init serializable source config
        auto serializedPhysicalSourceType = new SerializablePhysicalSourceType();
        serializedPhysicalSourceType->set_sourcetype(tcpSourceDescriptor->getSourceConfig()->getSourceTypeAsString());
        //init serializable tcp source config
        auto tcpSerializedSourceConfig = SerializablePhysicalSourceType_SerializableTCPSourceType();
        tcpSerializedSourceConfig.set_sockethost(tcpSourceDescriptor->getSourceConfig()->getSocketHost()->getValue());
        tcpSerializedSourceConfig.set_socketport(tcpSourceDescriptor->getSourceConfig()->getSocketPort()->getValue());
        tcpSerializedSourceConfig.set_socketdomain(tcpSourceDescriptor->getSourceConfig()->getSocketDomain()->getValue());
        tcpSerializedSourceConfig.set_sockettype(tcpSourceDescriptor->getSourceConfig()->getSocketType()->getValue());
        std::string tupleSeparator;
        tupleSeparator = tcpSourceDescriptor->getSourceConfig()->getTupleSeparator()->getValue();
        tcpSerializedSourceConfig.set_tupleseparator(tupleSeparator);
        tcpSerializedSourceConfig.set_flushintervalms(tcpSourceDescriptor->getSourceConfig()->getFlushIntervalMS()->getValue());
        switch (tcpSourceDescriptor->getSourceConfig()->getInputFormat()->getValue()) {
            case Configurations::InputFormat::JSON:
                tcpSerializedSourceConfig.set_inputformat(SerializablePhysicalSourceType_InputFormat_JSON);
                break;
            case Configurations::InputFormat::CSV:
                tcpSerializedSourceConfig.set_inputformat(SerializablePhysicalSourceType_InputFormat_CSV);
                break;
            case Configurations::InputFormat::NES_BINARY:
                tcpSerializedSourceConfig.set_inputformat(SerializablePhysicalSourceType_InputFormat_NES_BINARY);
                break;
        }

        using enum Configurations::TCPDecideMessageSize;
        switch (tcpSourceDescriptor->getSourceConfig()->getDecideMessageSize()->getValue()) {
            case TUPLE_SEPARATOR:
                tcpSerializedSourceConfig.set_tcpdecidemessagesize(
                    SerializablePhysicalSourceType_TCPDecideMessageSize_TUPLE_SEPARATOR);
                break;
            case USER_SPECIFIED_BUFFER_SIZE:
                tcpSerializedSourceConfig.set_tcpdecidemessagesize(
                    SerializablePhysicalSourceType_TCPDecideMessageSize_USER_SPECIFIED_BUFFER_SIZE);
                break;
            case BUFFER_SIZE_FROM_SOCKET:
                tcpSerializedSourceConfig.set_tcpdecidemessagesize(
                    SerializablePhysicalSourceType_TCPDecideMessageSize_BUFFER_SIZE_FROM_SOCKET);
                break;
        }
        tcpSerializedSourceConfig.set_socketbuffersize(tcpSourceDescriptor->getSourceConfig()->getSocketBufferSize()->getValue());
        tcpSerializedSourceConfig.set_bytesusedforsocketbuffersizetransfer(
            tcpSourceDescriptor->getSourceConfig()->getBytesUsedForSocketBufferSizeTransfer()->getValue());
        serializedPhysicalSourceType->mutable_specificphysicalsourcetype()->PackFrom(tcpSerializedSourceConfig);
        //init serializable tcp source descriptor
        auto tcpSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableTCPSourceDescriptor();
        tcpSerializedSourceDescriptor.set_allocated_physicalsourcetype(serializedPhysicalSourceType);

        // serialize source schema
        SchemaSerializationUtil::serializeSchema(tcpSourceDescriptor->getSchema(),
                                                 tcpSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails.mutable_sourcedescriptor()->PackFrom(tcpSerializedSourceDescriptor);
    } else if (sourceDescriptor.instanceOf<const MonitoringSourceDescriptor>()) {
        // serialize network source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableNetworkSourceDescriptor");
        auto monitoringSourceDescriptor = sourceDescriptor.as<const MonitoringSourceDescriptor>();
        auto monitoringSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableMonitoringSourceDescriptor();
        auto metricCollectorType = monitoringSourceDescriptor->getMetricCollectorType();
        auto waitTime = monitoringSourceDescriptor->getWaitTime();
        // serialize source schema
        monitoringSerializedSourceDescriptor.set_metriccollectortype(magic_enum::enum_integer(metricCollectorType));
        monitoringSerializedSourceDescriptor.set_waittime(waitTime.count());
        sourceDetails.mutable_sourcedescriptor()->PackFrom(monitoringSerializedSourceDescriptor);
    } else if (sourceDescriptor.instanceOf<const Network::NetworkSourceDescriptor>()) {
        // serialize network source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableNetworkSourceDescriptor");
        auto networkSourceDescriptor = sourceDescriptor.as<const Network::NetworkSourceDescriptor>();
        auto networkSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableNetworkSourceDescriptor();
        const auto nodeLocation = networkSourceDescriptor->getNodeLocation();
        const auto nesPartition = networkSourceDescriptor->getNesPartition();
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(networkSourceDescriptor->getSchema(),
                                                 networkSerializedSourceDescriptor.mutable_sourceschema());
        networkSerializedSourceDescriptor.mutable_nespartition()->set_operatorid(nesPartition.getOperatorId().getRawValue());
        networkSerializedSourceDescriptor.mutable_nespartition()->set_partitionid(nesPartition.getPartitionId().getRawValue());
        networkSerializedSourceDescriptor.mutable_nespartition()->set_queryid(nesPartition.getQueryId().getRawValue());
        networkSerializedSourceDescriptor.mutable_nespartition()->set_subpartitionid(
            nesPartition.getSubpartitionId().getRawValue());
        networkSerializedSourceDescriptor.mutable_nodelocation()->set_port(nodeLocation.getPort());
        networkSerializedSourceDescriptor.mutable_nodelocation()->set_hostname(nodeLocation.getHostname());
        networkSerializedSourceDescriptor.mutable_nodelocation()->set_nodeid(nodeLocation.getNodeId().getRawValue());
        auto s = std::chrono::duration_cast<std::chrono::milliseconds>(networkSourceDescriptor->getWaitTime());
        networkSerializedSourceDescriptor.set_waittime(s.count());
        networkSerializedSourceDescriptor.set_version(networkSourceDescriptor->getVersion());
        networkSerializedSourceDescriptor.set_uniqueid(networkSourceDescriptor->getUniqueId().getRawValue());
        networkSerializedSourceDescriptor.set_retrytimes(networkSourceDescriptor->getRetryTimes());
        sourceDetails.mutable_sourcedescriptor()->PackFrom(networkSerializedSourceDescriptor);
    } else if (sourceDescriptor.instanceOf<const DefaultSourceDescriptor>()) {
        // serialize default source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableDefaultSourceDescriptor");
        auto defaultSourceDescriptor = sourceDescriptor.as<const DefaultSourceDescriptor>();
        auto defaultSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableDefaultSourceDescriptor();
        defaultSerializedSourceDescriptor.set_sourcegatheringinterval(defaultSourceDescriptor->getSourceGatheringIntervalCount());
        defaultSerializedSourceDescriptor.set_numberofbufferstoproduce(defaultSourceDescriptor->getNumbersOfBufferToProduce());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(defaultSourceDescriptor->getSchema(),
                                                 defaultSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails.mutable_sourcedescriptor()->PackFrom(defaultSerializedSourceDescriptor);
    } else if (sourceDescriptor.instanceOf<const BinarySourceDescriptor>()) {
        // serialize binary source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableBinarySourceDescriptor");
        auto binarySourceDescriptor = sourceDescriptor.as<const BinarySourceDescriptor>();
        auto binarySerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableBinarySourceDescriptor();
        binarySerializedSourceDescriptor.set_filepath(binarySourceDescriptor->getFilePath());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(binarySourceDescriptor->getSchema(),
                                                 binarySerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails.mutable_sourcedescriptor()->PackFrom(binarySerializedSourceDescriptor);
    } else if (sourceDescriptor.instanceOf<const CsvSourceDescriptor>()) {
        // serialize csv source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableCsvSourceDescriptor");
        auto csvSourceDescriptor = sourceDescriptor.as<const CsvSourceDescriptor>();
        // init serializable source config
        auto serializedSourceConfig = new SerializablePhysicalSourceType();
        serializedSourceConfig->set_sourcetype(csvSourceDescriptor->getSourceConfig()->getSourceTypeAsString());
        // init serializable csv source config
        auto csvSerializedSourceConfig = SerializablePhysicalSourceType_SerializableCSVSourceType();
        csvSerializedSourceConfig.set_numberofbufferstoproduce(
            csvSourceDescriptor->getSourceConfig()->getNumberOfBuffersToProduce()->getValue());
        csvSerializedSourceConfig.set_numberoftuplestoproduceperbuffer(
            csvSourceDescriptor->getSourceConfig()->getNumberOfTuplesToProducePerBuffer()->getValue());
        csvSerializedSourceConfig.set_sourcegatheringinterval(
            csvSourceDescriptor->getSourceConfig()->getGatheringInterval()->getValue());
        csvSerializedSourceConfig.set_filepath(csvSourceDescriptor->getSourceConfig()->getFilePath()->getValue());
        csvSerializedSourceConfig.set_skipheader(csvSourceDescriptor->getSourceConfig()->getSkipHeader()->getValue());
        csvSerializedSourceConfig.set_delimiter(csvSourceDescriptor->getSourceConfig()->getDelimiter()->getValue());
        serializedSourceConfig->mutable_specificphysicalsourcetype()->PackFrom(csvSerializedSourceConfig);
        // init serializable csv source descriptor
        auto csvSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableCsvSourceDescriptor();
        csvSerializedSourceDescriptor.set_allocated_physicalsourcetype(serializedSourceConfig);
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(csvSourceDescriptor->getSchema(),
                                                 csvSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails.mutable_sourcedescriptor()->PackFrom(csvSerializedSourceDescriptor);
    } else if (sourceDescriptor.instanceOf<const SenseSourceDescriptor>()) {
        // serialize sense source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableSenseSourceDescriptor");
        auto senseSourceDescriptor = sourceDescriptor.as<const SenseSourceDescriptor>();
        auto senseSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableSenseSourceDescriptor();
        senseSerializedSourceDescriptor.set_udfs(senseSourceDescriptor->getUdfs());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(senseSourceDescriptor->getSchema(),
                                                 senseSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails.mutable_sourcedescriptor()->PackFrom(senseSerializedSourceDescriptor);
    } else if (sourceDescriptor.instanceOf<const LogicalSourceDescriptor>()) {
        // serialize logical source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableLogicalSourceDescriptor");
        auto logicalSourceDescriptor = sourceDescriptor.as<const LogicalSourceDescriptor>();
        auto logicalSourceSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableLogicalSourceDescriptor();
        logicalSourceSerializedSourceDescriptor.set_logicalsourcename(logicalSourceDescriptor->getLogicalSourceName());
        logicalSourceSerializedSourceDescriptor.set_physicalsourcename(logicalSourceDescriptor->getPhysicalSourceName());

        if (!isClientOriginated) {
            // serialize source schema
            SchemaSerializationUtil::serializeSchema(logicalSourceDescriptor->getSchema(),
                                                     logicalSourceSerializedSourceDescriptor.mutable_sourceschema());
        }
        sourceDetails.mutable_sourcedescriptor()->PackFrom(logicalSourceSerializedSourceDescriptor);
    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown Source Descriptor Type {}", sourceDescriptor.toString());
        throw std::invalid_argument("Unknown Source Descriptor Type");
    }
}

SourceDescriptorPtr
OperatorSerializationUtil::deserializeSourceDescriptor(const SerializableOperator_SourceDetails& sourceDetails) {
    NES_TRACE("OperatorSerializationUtil:: de-serialized SourceDescriptor id={}", sourceDetails.DebugString());
    const auto& serializedSourceDescriptor = sourceDetails.sourcedescriptor();

    if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableZMQSourceDescriptor>()) {
        // de-serialize zmq source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as ZmqSourceDescriptor");
        auto zmqSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableZMQSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&zmqSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(zmqSerializedSourceDescriptor.sourceschema());
        auto ret =
            ZmqSourceDescriptor::create(schema, zmqSerializedSourceDescriptor.host(), zmqSerializedSourceDescriptor.port());
        return ret;
    }
#ifdef ENABLE_MQTT_BUILD
    if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableMQTTSourceDescriptor>()) {
        // de-serialize mqtt source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as MQTTSourceDescriptor");
        auto* mqttSerializedSourceDescriptor = new SerializableOperator_SourceDetails_SerializableMQTTSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(mqttSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(mqttSerializedSourceDescriptor->sourceschema());
        auto mqttSourceConfig = new SerializablePhysicalSourceType_SerializableMQTTSourceType();
        SerializablePhysicalSourceType physicalSourceType = mqttSerializedSourceDescriptor->physicalsourcetype();
        auto sourceConfig =
            MQTTSourceType::create(physicalSourceType.logicalsourcename(), physicalSourceType.physicalsourcename());
        physicalSourceType.specificphysicalsourcetype().UnpackTo(mqttSourceConfig);
        sourceConfig->setUrl(mqttSourceConfig->url());
        sourceConfig->setClientId(mqttSourceConfig->clientid());
        sourceConfig->setUserName(mqttSourceConfig->username());
        sourceConfig->setTopic(mqttSourceConfig->topic());
        sourceConfig->setQos(mqttSourceConfig->qos());
        sourceConfig->setCleanSession(mqttSourceConfig->cleansession());
        sourceConfig->setFlushIntervalMS(mqttSourceConfig->flushintervalms());
        sourceConfig->setInputFormat(static_cast<Configurations::InputFormat>(mqttSourceConfig->inputformat()));
        auto ret = MQTTSourceDescriptor::create(schema, sourceConfig);
        return ret;
    }

#endif
#ifdef ENABLE_OPC_BUILD
    else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableOPCSourceDescriptor>()) {
        // de-serialize opc source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as OPCSourceDescriptor");
        auto opcSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableOPCSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&opcSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(opcSerializedSourceDescriptor.sourceschema());
        char* ident = (char*) UA_malloc(sizeof(char) * opcSerializedSourceDescriptor.identifier().length() + 1);
        memcpy(ident, opcSerializedSourceDescriptor.identifier().data(), opcSerializedSourceDescriptor.identifier().length());
        ident[opcSerializedSourceDescriptor.identifier().length()] = '\0';
        UA_NodeId nodeId = UA_NODEID_STRING(opcSerializedSourceDescriptor.namespaceindex(), ident);
        auto ret = OPCSourceDescriptor::create(schema,
                                               opcSerializedSourceDescriptor.url(),
                                               nodeId,
                                               opcSerializedSourceDescriptor.user(),
                                               opcSerializedSourceDescriptor.password());
        return ret;
    }
#endif
    else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableTCPSourceDescriptor>()) {
        // de-serialize tcp source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as TCPSourceDescriptor");
        auto* tcpSerializedSourceDescriptor = new SerializableOperator_SourceDetails_SerializableTCPSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(tcpSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(tcpSerializedSourceDescriptor->sourceschema());
        SerializablePhysicalSourceType physicalSourceType = tcpSerializedSourceDescriptor->physicalsourcetype();
        auto sourceConfig =
            TCPSourceType::create(physicalSourceType.logicalsourcename(), physicalSourceType.physicalsourcename());
        auto tcpSourceConfig = new SerializablePhysicalSourceType_SerializableTCPSourceType();
        tcpSerializedSourceDescriptor->physicalsourcetype().specificphysicalsourcetype().UnpackTo(tcpSourceConfig);
        sourceConfig->setSocketHost(tcpSourceConfig->sockethost());
        sourceConfig->setSocketPort(tcpSourceConfig->socketport());
        sourceConfig->setSocketDomain(tcpSourceConfig->socketdomain());
        sourceConfig->setSocketType(tcpSourceConfig->sockettype());
        sourceConfig->setFlushIntervalMS(tcpSourceConfig->flushintervalms());
        sourceConfig->setInputFormat(static_cast<Configurations::InputFormat>(tcpSourceConfig->inputformat()));
        sourceConfig->setDecideMessageSize(
            static_cast<Configurations::TCPDecideMessageSize>(tcpSourceConfig->tcpdecidemessagesize()));
        sourceConfig->setTupleSeparator(tcpSourceConfig->tupleseparator().at(0));
        sourceConfig->setSocketBufferSize(tcpSourceConfig->socketbuffersize());
        sourceConfig->setBytesUsedForSocketBufferSizeTransfer(tcpSourceConfig->bytesusedforsocketbuffersizetransfer());
        auto ret = TCPSourceDescriptor::create(schema, sourceConfig);
        return ret;
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableMonitoringSourceDescriptor>()) {
        // de-serialize zmq source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as monitoringSourceDescriptor");
        auto monitoringSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableMonitoringSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&monitoringSerializedSourceDescriptor);
        // de-serialize source schema
        auto waitTime = std::chrono::milliseconds(monitoringSerializedSourceDescriptor.waittime());
        auto metricCollectorType = monitoringSerializedSourceDescriptor.metriccollectortype();
        auto ret = MonitoringSourceDescriptor::create(waitTime, Monitoring::MetricCollectorType(metricCollectorType));
        return ret;
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableNetworkSourceDescriptor>()) {
        // de-serialize zmq source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as NetworkSourceDescriptor");
        auto networkSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableNetworkSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&networkSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(networkSerializedSourceDescriptor.sourceschema());
        Network::NesPartition nesPartition{SharedQueryId(networkSerializedSourceDescriptor.nespartition().queryid()),
                                           OperatorId(networkSerializedSourceDescriptor.nespartition().operatorid()),
                                           PartitionId(networkSerializedSourceDescriptor.nespartition().partitionid()),
                                           SubpartitionId(networkSerializedSourceDescriptor.nespartition().subpartitionid())};
        NES::Network::NodeLocation nodeLocation(WorkerId(networkSerializedSourceDescriptor.nodelocation().nodeid()),
                                                networkSerializedSourceDescriptor.nodelocation().hostname(),
                                                networkSerializedSourceDescriptor.nodelocation().port());
        auto waitTime = std::chrono::milliseconds(networkSerializedSourceDescriptor.waittime());
        auto ret = Network::NetworkSourceDescriptor::create(schema,
                                                            nesPartition,
                                                            nodeLocation,
                                                            waitTime,
                                                            networkSerializedSourceDescriptor.retrytimes(),
                                                            networkSerializedSourceDescriptor.version(),
                                                            OperatorId(networkSerializedSourceDescriptor.uniqueid()));
        return ret;
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableDefaultSourceDescriptor>()) {
        // de-serialize default source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as DefaultSourceDescriptor");
        auto defaultSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableDefaultSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&defaultSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(defaultSerializedSourceDescriptor.sourceschema());
        auto ret = DefaultSourceDescriptor::create(schema,
                                                   defaultSerializedSourceDescriptor.numberofbufferstoproduce(),
                                                   defaultSerializedSourceDescriptor.sourcegatheringinterval());
        return ret;
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableBinarySourceDescriptor>()) {
        // de-serialize binary source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as BinarySourceDescriptor");
        auto binarySerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableBinarySourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&binarySerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(binarySerializedSourceDescriptor.sourceschema());
        auto ret = BinarySourceDescriptor::create(schema, binarySerializedSourceDescriptor.filepath());
        return ret;
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableCsvSourceDescriptor>()) {
        // de-serialize csv source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as CsvSourceDescriptor");
        auto csvSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableCsvSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&csvSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(csvSerializedSourceDescriptor.sourceschema());
        auto physicalSourceType = csvSerializedSourceDescriptor.physicalsourcetype();
        auto sourceConfig =
            CSVSourceType::create(physicalSourceType.logicalsourcename(), physicalSourceType.physicalsourcename());
        auto csvSourceConfig = new SerializablePhysicalSourceType_SerializableCSVSourceType();
        physicalSourceType.specificphysicalsourcetype().UnpackTo(csvSourceConfig);
        sourceConfig->setFilePath(csvSourceConfig->filepath());
        sourceConfig->setSkipHeader(csvSourceConfig->skipheader());
        sourceConfig->setDelimiter(csvSourceConfig->delimiter());
        sourceConfig->setGatheringInterval(csvSourceConfig->sourcegatheringinterval());
        sourceConfig->setNumberOfBuffersToProduce(csvSourceConfig->numberofbufferstoproduce());
        sourceConfig->setNumberOfTuplesToProducePerBuffer(csvSourceConfig->numberoftuplestoproduceperbuffer());
        auto ret = CsvSourceDescriptor::create(schema, sourceConfig);
        return ret;
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableSenseSourceDescriptor>()) {
        // de-serialize sense source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as SenseSourceDescriptor");
        auto senseSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableSenseSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&senseSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(senseSerializedSourceDescriptor.sourceschema());
        return SenseSourceDescriptor::create(schema, senseSerializedSourceDescriptor.udfs());
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableLogicalSourceDescriptor>()) {
        // de-serialize logical source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as LogicalSourceDescriptor");
        auto logicalSourceSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableLogicalSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&logicalSourceSerializedSourceDescriptor);

        // de-serialize source schema
        SourceDescriptorPtr logicalSourceDescriptor =
            LogicalSourceDescriptor::create(logicalSourceSerializedSourceDescriptor.logicalsourcename());
        logicalSourceDescriptor->setPhysicalSourceName(logicalSourceSerializedSourceDescriptor.physicalsourcename());
        // check if the schema is set
        if (logicalSourceSerializedSourceDescriptor.has_sourceschema()) {
            auto schema = SchemaSerializationUtil::deserializeSchema(logicalSourceSerializedSourceDescriptor.sourceschema());
            logicalSourceDescriptor->setSchema(schema);
        }

        return logicalSourceDescriptor;
    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown Source Descriptor Type {}", serializedSourceDescriptor.type_url());
        throw std::invalid_argument("Unknown Source Descriptor Type");
    }
}

void OperatorSerializationUtil::serializeSinkDescriptor(const SinkDescriptor& sinkDescriptor,
                                                        SerializableOperator_SinkDetails& sinkDetails,
                                                        uint64_t numberOfOrigins) {
    // serialize a sink descriptor and all its properties depending on its type
    NES_DEBUG("OperatorSerializationUtil:: serialized SinkDescriptor ");
    if (sinkDescriptor.instanceOf<const PrintSinkDescriptor>()) {
        // serialize print sink descriptor
        auto printSinkDescriptor = sinkDescriptor.as<const PrintSinkDescriptor>();
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as "
                  "SerializableOperator_SinkDetails_SerializablePrintSinkDescriptor");
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializablePrintSinkDescriptor();
        sinkDetails.mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
        sinkDetails.set_numberoforiginids(numberOfOrigins);
    } else if (sinkDescriptor.instanceOf<const NullOutputSinkDescriptor>()) {
        auto nullSinkDescriptor = sinkDescriptor.as<const NullOutputSinkDescriptor>();
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as "
                  "SerializableOperator_SinkDetails_SerializableNullOutputSinkDescriptor");
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableNullOutputSinkDescriptor();
        sinkDetails.mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
        sinkDetails.set_numberoforiginids(numberOfOrigins);
    } else if (sinkDescriptor.instanceOf<const ZmqSinkDescriptor>()) {
        // serialize zmq sink descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as "
                  "SerializableOperator_SinkDetails_SerializableZMQSinkDescriptor");
        auto zmqSinkDescriptor = sinkDescriptor.as<const ZmqSinkDescriptor>();
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableZMQSinkDescriptor();
        serializedSinkDescriptor.set_port(zmqSinkDescriptor->getPort());
        serializedSinkDescriptor.set_isinternal(zmqSinkDescriptor->isInternal());
        serializedSinkDescriptor.set_host(zmqSinkDescriptor->getHost());
        sinkDetails.mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
        sinkDetails.set_numberoforiginids(numberOfOrigins);
    } else if (sinkDescriptor.instanceOf<const MonitoringSinkDescriptor>()) {
        // serialize Monitoring sink descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as "
                  "SerializableOperator_SinkDetails_SerializableMonitoringSinkDescriptor");
        auto monitoringSinkDescriptor = sinkDescriptor.as<const MonitoringSinkDescriptor>();
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableMonitoringSinkDescriptor();
        serializedSinkDescriptor.set_collectortype(magic_enum::enum_integer(monitoringSinkDescriptor->getCollectorType()));
        sinkDetails.mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
        sinkDetails.set_numberoforiginids(numberOfOrigins);
    }

#ifdef ENABLE_OPC_BUILD
    else if (sinkDescriptor.instanceOf<const OPCSinkDescriptor>()) {
        // serialize opc sink descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as "
                  "SerializableOperator_SinkDetails_SerializableOPCSinkDescriptor");
        auto opcSinkDescriptor = sinkDescriptor.as<const OPCSinkDescriptor>();
        auto opcSerializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableOPCSinkDescriptor();
        char* ident = (char*) UA_malloc(sizeof(char) * opcSinkDescriptor->getNodeId().identifier.string.length + 1);
        memcpy(ident,
               opcSinkDescriptor->getNodeId().identifier.string.data,
               opcSinkDescriptor->getNodeId().identifier.string.length);
        ident[opcSinkDescriptor->getNodeId().identifier.string.length] = '\0';
        opcSerializedSinkDescriptor.set_identifier(ident);
        free(ident);
        opcSerializedSinkDescriptor.set_url(opcSinkDescriptor->getUrl());
        opcSerializedSinkDescriptor.set_namespaceindex(opcSinkDescriptor->getNodeId().namespaceIndex);
        opcSerializedSinkDescriptor.set_identifiertype(opcSinkDescriptor->getNodeId().identifierType);
        opcSerializedSinkDescriptor.set_user(opcSinkDescriptor->getUser());
        opcSerializedSinkDescriptor.set_password(opcSinkDescriptor->getPassword());
        sinkDetails.mutable_sinkdescriptor()->PackFrom(opcSerializedSinkDescriptor);
    }
#endif
#ifdef ENABLE_MQTT_BUILD
    else if (sinkDescriptor.instanceOf<const MQTTSinkDescriptor>()) {
        // serialize MQTT sink descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as "
                  "SerializableOperator_SourceDetails_SerializableMQTTSourceDescriptor");
        auto mqttSinkDescriptor = sinkDescriptor.as<const MQTTSinkDescriptor>();
        auto mqttSerializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableMQTTSinkDescriptor();
        mqttSerializedSinkDescriptor.set_address(mqttSinkDescriptor->getAddress());
        mqttSerializedSinkDescriptor.set_clientid(mqttSinkDescriptor->getClientId());
        mqttSerializedSinkDescriptor.set_topic(mqttSinkDescriptor->getTopic());
        mqttSerializedSinkDescriptor.set_user(mqttSinkDescriptor->getUser());
        mqttSerializedSinkDescriptor.set_maxbufferedmessages(mqttSinkDescriptor->getMaxBufferedMSGs());
        mqttSerializedSinkDescriptor.set_timeunit(
            (SerializableOperator_SinkDetails_SerializableMQTTSinkDescriptor_TimeUnits) mqttSinkDescriptor->getTimeUnit());
        mqttSerializedSinkDescriptor.set_msgdelay(mqttSinkDescriptor->getMsgDelay());
        mqttSerializedSinkDescriptor.set_asynchronousclient(mqttSinkDescriptor->getAsynchronousClient());

        sinkDetails.mutable_sinkdescriptor()->PackFrom(mqttSerializedSinkDescriptor);
        sinkDetails.set_numberoforiginids(numberOfOrigins);
    }
#endif
#ifdef ENABLE_KAFKA_BUILD
    else if (sinkDescriptor.instanceOf<const KafkaSinkDescriptor>()) {
        NES_TRACE("Serializing KafkaSinkDescriptor");
        const auto kafkaSinkDescriptor = sinkDescriptor.as<const KafkaSinkDescriptor>();
        SerializableOperator_SinkDetails_SerializableKafkaSinkDescriptor serializedDescriptor;
        serializedDescriptor.set_brokers(kafkaSinkDescriptor->getBrokers());
        serializedDescriptor.set_topic(kafkaSinkDescriptor->getTopic());
        serializedDescriptor.set_kafkaconnecttimeout(kafkaSinkDescriptor->getTimeout());
        sinkDetails.mutable_sinkdescriptor()->PackFrom(serializedDescriptor);
        sinkDetails.set_numberoforiginids(numberOfOrigins);
    }
#endif
    else if (sinkDescriptor.instanceOf<const Network::NetworkSinkDescriptor>()) {
        // serialize zmq sink descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as "
                  "SerializableOperator_SinkDetails_SerializableNetworkSinkDescriptor");
        auto networkSinkDescriptor = sinkDescriptor.as<const Network::NetworkSinkDescriptor>();
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableNetworkSinkDescriptor();
        //set details of NesPartition
        auto* serializedNesPartition = serializedSinkDescriptor.mutable_nespartition();
        auto nesPartition = networkSinkDescriptor->getNesPartition();
        serializedNesPartition->set_queryid(nesPartition.getQueryId().getRawValue());
        serializedNesPartition->set_operatorid(nesPartition.getOperatorId().getRawValue());
        serializedNesPartition->set_partitionid(nesPartition.getPartitionId().getRawValue());
        serializedNesPartition->set_subpartitionid(nesPartition.getSubpartitionId().getRawValue());
        //set details of NodeLocation
        auto* serializedNodeLocation = serializedSinkDescriptor.mutable_nodelocation();
        auto nodeLocation = networkSinkDescriptor->getNodeLocation();
        serializedNodeLocation->set_nodeid(nodeLocation.getNodeId().getRawValue());
        serializedNodeLocation->set_hostname(nodeLocation.getHostname());
        serializedNodeLocation->set_port(nodeLocation.getPort());
        // set reconnection details
        auto s = std::chrono::duration_cast<std::chrono::milliseconds>(networkSinkDescriptor->getWaitTime());
        serializedSinkDescriptor.set_waittime(s.count());
        serializedSinkDescriptor.set_retrytimes(networkSinkDescriptor->getRetryTimes());
        serializedSinkDescriptor.set_version(networkSinkDescriptor->getVersion());
        serializedSinkDescriptor.set_uniquenetworksinkdescriptorid(networkSinkDescriptor->getUniqueId().getRawValue());
        //pack to output
        sinkDetails.mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
        sinkDetails.set_numberoforiginids(numberOfOrigins);
    } else if (sinkDescriptor.instanceOf<const FileSinkDescriptor>()) {
        // serialize file sink descriptor. The file sink has different types which have to be set correctly
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as "
                  "SerializableOperator_SinkDetails_SerializableFileSinkDescriptor");
        auto fileSinkDescriptor = sinkDescriptor.as<const FileSinkDescriptor>();
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableFileSinkDescriptor();

        serializedSinkDescriptor.set_filepath(fileSinkDescriptor->getFileName());
        serializedSinkDescriptor.set_append(fileSinkDescriptor->getAppend());
        serializedSinkDescriptor.set_addtimestamp(fileSinkDescriptor->getAddTimestamp());

        auto format = fileSinkDescriptor->getSinkFormatAsString();
        if (format == "JSON_FORMAT") {
            serializedSinkDescriptor.set_sinkformat("JSON_FORMAT");
        } else if (format == "CSV_FORMAT") {
            serializedSinkDescriptor.set_sinkformat("CSV_FORMAT");
        } else if (format == "NES_FORMAT") {
            serializedSinkDescriptor.set_sinkformat("NES_FORMAT");
        } else if (format == "TEXT_FORMAT") {
            serializedSinkDescriptor.set_sinkformat("TEXT_FORMAT");
        } else if (format == "MIGRATION_FORMAT") {
            serializedSinkDescriptor.set_sinkformat("MIGRATION_FORMAT");
        } else {
            NES_ERROR("serializeSinkDescriptor: format not supported");
        }
        sinkDetails.mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
        sinkDetails.set_numberoforiginids(numberOfOrigins);
    } else if (sinkDescriptor.instanceOf<Statistic::StatisticSinkDescriptor>()) {
        auto statisticSinkDescriptor = sinkDescriptor.as<const Statistic::StatisticSinkDescriptor>();
        SerializableOperator_SinkDetails_StatisticSinkDescriptor sinkDescriptorMessage;
        sinkDescriptorMessage.set_sinkformattype(
            (SerializableOperator_SinkDetails_StatisticSinkDescriptor_StatisticSinkFormatType)
                statisticSinkDescriptor->getSinkFormatType());
        sinkDescriptorMessage.set_sinkdatacodec((SerializableOperator_SinkDetails_StatisticSinkDescriptor_StatisticDataCodec)
                                                    statisticSinkDescriptor->getSinkDataCodec());
        sinkDetails.mutable_sinkdescriptor()->PackFrom(sinkDescriptorMessage);
        sinkDetails.set_numberoforiginids(numberOfOrigins);
    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown Sink Descriptor Type - {}", sinkDescriptor.toString());
        throw std::invalid_argument("Unknown Sink Descriptor Type");
    }
}

SinkDescriptorPtr OperatorSerializationUtil::deserializeSinkDescriptor(const SerializableOperator_SinkDetails& sinkDetails) {
    // de-serialize a sink descriptor and all its properties to a SinkDescriptor.
    NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor {}", sinkDetails.DebugString());
    const auto& deserializedSinkDescriptor = sinkDetails.sinkdescriptor();
    const auto deserializedNumberOfOrigins = sinkDetails.numberoforiginids();
    if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializablePrintSinkDescriptor>()) {
        // de-serialize print sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as PrintSinkDescriptor");
        return PrintSinkDescriptor::create(deserializedNumberOfOrigins);
    }
    if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableNullOutputSinkDescriptor>()) {
        // de-serialize print sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as PrintSinkDescriptor");
        return NullOutputSinkDescriptor::create(deserializedNumberOfOrigins);
    } else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableZMQSinkDescriptor>()) {
        // de-serialize zmq sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as ZmqSinkDescriptor");
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableZMQSinkDescriptor();
        deserializedSinkDescriptor.UnpackTo(&serializedSinkDescriptor);
        return ZmqSinkDescriptor::create(serializedSinkDescriptor.host(),
                                         serializedSinkDescriptor.port(),
                                         serializedSinkDescriptor.isinternal(),
                                         deserializedNumberOfOrigins);
    } else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableMonitoringSinkDescriptor>()) {
        // de-serialize zmq sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as MonitoringSinkDescriptor");
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableMonitoringSinkDescriptor();
        deserializedSinkDescriptor.UnpackTo(&serializedSinkDescriptor);
        return MonitoringSinkDescriptor::create(Monitoring::MetricCollectorType(serializedSinkDescriptor.collectortype()),
                                                deserializedNumberOfOrigins);
    }
#ifdef ENABLE_OPC_BUILD
    else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableOPCSinkDescriptor>()) {
        // de-serialize opc sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as OPCSinkDescriptor");
        auto opcSerializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableOPCSinkDescriptor();
        deserializedSinkDescriptor.UnpackTo(&opcSerializedSinkDescriptor);
        char* ident = (char*) UA_malloc(sizeof(char) * opcSerializedSinkDescriptor.identifier().length() + 1);
        memcpy(ident, opcSerializedSinkDescriptor.identifier().data(), opcSerializedSinkDescriptor.identifier().length());
        ident[opcSerializedSinkDescriptor.identifier().length()] = '\0';
        UA_NodeId nodeId = UA_NODEID_STRING(opcSerializedSinkDescriptor.namespaceindex(), ident);
        return OPCSinkDescriptor::create(opcSerializedSinkDescriptor.url(),
                                         nodeId,
                                         opcSerializedSinkDescriptor.user(),
                                         opcSerializedSinkDescriptor.password());
    }
#endif
#ifdef ENABLE_MQTT_BUILD
    else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableMQTTSinkDescriptor>()) {
        // de-serialize MQTT sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as MQTTSinkDescriptor");
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableMQTTSinkDescriptor();
        deserializedSinkDescriptor.UnpackTo(&serializedSinkDescriptor);
        return MQTTSinkDescriptor::create(std::string{serializedSinkDescriptor.address()},
                                          std::string{serializedSinkDescriptor.topic()},
                                          std::string{serializedSinkDescriptor.user()},
                                          serializedSinkDescriptor.maxbufferedmessages(),
                                          (MQTTSinkDescriptor::TimeUnits) serializedSinkDescriptor.timeunit(),
                                          serializedSinkDescriptor.msgdelay(),
                                          (MQTTSinkDescriptor::ServiceQualities) serializedSinkDescriptor.qualityofservice(),
                                          serializedSinkDescriptor.asynchronousclient(),
                                          std::string{serializedSinkDescriptor.clientid()},
                                          deserializedNumberOfOrigins);
    }
#endif
#ifdef ENABLE_KAFKA_BUILD
    else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableKafkaSinkDescriptor>()) {
        NES_TRACE("Deserializing SinkDescriptor as KafkaSinkDescriptor");
        SerializableOperator_SinkDetails_SerializableKafkaSinkDescriptor serializedSinkDescriptor;
        deserializedSinkDescriptor.UnpackTo(&serializedSinkDescriptor);
        return KafkaSinkDescriptor::create("CSV_FORMAT",
                                           std::string{serializedSinkDescriptor.topic()},
                                           std::string{serializedSinkDescriptor.brokers()},
                                           serializedSinkDescriptor.kafkaconnecttimeout());

    }
#endif
    else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableNetworkSinkDescriptor>()) {
        // de-serialize zmq sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as NetworkSinkDescriptor");
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableNetworkSinkDescriptor();
        deserializedSinkDescriptor.UnpackTo(&serializedSinkDescriptor);
        Network::NesPartition nesPartition{SharedQueryId(serializedSinkDescriptor.nespartition().queryid()),
                                           OperatorId(serializedSinkDescriptor.nespartition().operatorid()),
                                           PartitionId(serializedSinkDescriptor.nespartition().partitionid()),
                                           SubpartitionId(serializedSinkDescriptor.nespartition().subpartitionid())};
        Network::NodeLocation nodeLocation{WorkerId(serializedSinkDescriptor.nodelocation().nodeid()),
                                           serializedSinkDescriptor.nodelocation().hostname(),
                                           serializedSinkDescriptor.nodelocation().port()};
        auto waitTime = std::chrono::milliseconds(serializedSinkDescriptor.waittime());
        return Network::NetworkSinkDescriptor::create(nodeLocation,
                                                      nesPartition,
                                                      waitTime,
                                                      serializedSinkDescriptor.retrytimes(),
                                                      serializedSinkDescriptor.version(),
                                                      deserializedNumberOfOrigins,
                                                      OperatorId(serializedSinkDescriptor.uniquenetworksinkdescriptorid()));
    } else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableFileSinkDescriptor>()) {
        // de-serialize file sink descriptor
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableFileSinkDescriptor();
        deserializedSinkDescriptor.UnpackTo(&serializedSinkDescriptor);
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as FileSinkDescriptor");
        return FileSinkDescriptor::create(serializedSinkDescriptor.filepath(),
                                          serializedSinkDescriptor.sinkformat(),
                                          serializedSinkDescriptor.append() ? "APPEND" : "OVERWRITE",
                                          serializedSinkDescriptor.addtimestamp(),
                                          deserializedNumberOfOrigins);
    } else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_StatisticSinkDescriptor>()) {
        SerializableOperator_SinkDetails_StatisticSinkDescriptor serializedSinkDescriptor;
        deserializedSinkDescriptor.UnpackTo(&serializedSinkDescriptor);
        return Statistic::StatisticSinkDescriptor::create(
            // Be careful changing the order of the enum values, as this works as long as the order is the same in the
            // cpp and the proto file
            (Statistic::StatisticSynopsisType) serializedSinkDescriptor.sinkformattype(),
            (Statistic::StatisticDataCodec) serializedSinkDescriptor.sinkdatacodec(),
            deserializedNumberOfOrigins);

    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown sink Descriptor Type {}", sinkDetails.DebugString());
        throw std::invalid_argument("Unknown Sink Descriptor Type");
    }
}

void OperatorSerializationUtil::serializeLimitOperator(const LogicalLimitOperator& limitOperator,
                                                       SerializableOperator& serializedOperator) {

    NES_TRACE("OperatorSerializationUtil:: serialize limit operator ");

    auto limitDetails = SerializableOperator_LimitDetails();
    limitDetails.set_limit(limitOperator.getLimit());
    serializedOperator.mutable_details()->PackFrom(limitDetails);
}

LogicalUnaryOperatorPtr
OperatorSerializationUtil::deserializeLimitOperator(const SerializableOperator_LimitDetails& limitDetails) {
    return LogicalOperatorFactory::createLimitOperator(limitDetails.limit(), getNextOperatorId());
}

void OperatorSerializationUtil::serializeWatermarkAssignerOperator(
    const WatermarkAssignerLogicalOperator& watermarkAssignerOperator,
    SerializableOperator& serializedOperator) {

    NES_TRACE("OperatorSerializationUtil:: serialize watermark assigner operator ");

    auto watermarkStrategyDetails = SerializableOperator_WatermarkStrategyDetails();
    auto watermarkStrategyDescriptor = watermarkAssignerOperator.getWatermarkStrategyDescriptor();
    serializeWatermarkStrategyDescriptor(*watermarkStrategyDescriptor, watermarkStrategyDetails);
    serializedOperator.mutable_details()->PackFrom(watermarkStrategyDetails);
}

LogicalUnaryOperatorPtr OperatorSerializationUtil::deserializeWatermarkAssignerOperator(
    const SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails) {

    auto watermarkStrategyDescriptor = deserializeWatermarkStrategyDescriptor(watermarkStrategyDetails);
    return LogicalOperatorFactory::createWatermarkAssignerOperator(watermarkStrategyDescriptor, getNextOperatorId());
}

void OperatorSerializationUtil::serializeWatermarkStrategyDescriptor(
    const Windowing::WatermarkStrategyDescriptor& watermarkStrategyDescriptor,
    SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails) {
    NES_TRACE("OperatorSerializationUtil:: serialize watermark strategy ");

    if (watermarkStrategyDescriptor.instanceOf<const Windowing::EventTimeWatermarkStrategyDescriptor>()) {
        auto eventTimeWatermarkStrategyDescriptor =
            watermarkStrategyDescriptor.as<const Windowing::EventTimeWatermarkStrategyDescriptor>();
        auto serializedWatermarkStrategyDescriptor =
            SerializableOperator_WatermarkStrategyDetails_SerializableEventTimeWatermarkStrategyDescriptor();
        ExpressionSerializationUtil::serializeExpression(eventTimeWatermarkStrategyDescriptor->getOnField(),
                                                         serializedWatermarkStrategyDescriptor.mutable_onfield());
        serializedWatermarkStrategyDescriptor.set_allowedlateness(
            eventTimeWatermarkStrategyDescriptor->getAllowedLateness().getTime());
        serializedWatermarkStrategyDescriptor.set_multiplier(
            eventTimeWatermarkStrategyDescriptor->getTimeUnit().getMillisecondsConversionMultiplier());
        watermarkStrategyDetails.mutable_strategy()->PackFrom(serializedWatermarkStrategyDescriptor);
    } else if (watermarkStrategyDescriptor.instanceOf<const Windowing::IngestionTimeWatermarkStrategyDescriptor>()) {
        auto serializedWatermarkStrategyDescriptor =
            SerializableOperator_WatermarkStrategyDetails_SerializableIngestionTimeWatermarkStrategyDescriptor();
        watermarkStrategyDetails.mutable_strategy()->PackFrom(serializedWatermarkStrategyDescriptor);
    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown Watermark Strategy Descriptor Type");
        throw std::invalid_argument("Unknown Watermark Strategy Descriptor Type");
    }
}

Windowing::WatermarkStrategyDescriptorPtr OperatorSerializationUtil::deserializeWatermarkStrategyDescriptor(
    const SerializableOperator_WatermarkStrategyDetails& watermarkStrategyDetails) {

    NES_TRACE("OperatorSerializationUtil:: de-serialize watermark strategy ");
    const auto& deserializedWatermarkStrategyDescriptor = watermarkStrategyDetails.strategy();
    if (deserializedWatermarkStrategyDescriptor
            .Is<SerializableOperator_WatermarkStrategyDetails_SerializableEventTimeWatermarkStrategyDescriptor>()) {
        // de-serialize print sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized WatermarkStrategy as EventTimeWatermarkStrategyDescriptor");
        auto serializedEventTimeWatermarkStrategyDescriptor =
            SerializableOperator_WatermarkStrategyDetails_SerializableEventTimeWatermarkStrategyDescriptor();
        deserializedWatermarkStrategyDescriptor.UnpackTo(&serializedEventTimeWatermarkStrategyDescriptor);

        auto onField =
            ExpressionSerializationUtil::deserializeExpression(serializedEventTimeWatermarkStrategyDescriptor.onfield())
                ->as<FieldAccessExpressionNode>();
        NES_DEBUG("OperatorSerializationUtil:: deserialized field name {}", onField->getFieldName());
        auto eventTimeWatermarkStrategyDescriptor = Windowing::EventTimeWatermarkStrategyDescriptor::create(
            FieldAccessExpressionNode::create(onField->getFieldName()),
            Windowing::TimeMeasure(serializedEventTimeWatermarkStrategyDescriptor.allowedlateness()),
            Windowing::TimeUnit(serializedEventTimeWatermarkStrategyDescriptor.multiplier()));
        return eventTimeWatermarkStrategyDescriptor;
    }
    if (deserializedWatermarkStrategyDescriptor
            .Is<SerializableOperator_WatermarkStrategyDetails_SerializableIngestionTimeWatermarkStrategyDescriptor>()) {
        return Windowing::IngestionTimeWatermarkStrategyDescriptor::create();
    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown Serialized Watermark Strategy Descriptor Type");
        throw std::invalid_argument("Unknown Serialized Watermark Strategy Descriptor Type");
    }
}

void OperatorSerializationUtil::serializeInputSchema(const OperatorPtr& operatorNode, SerializableOperator& serializedOperator) {

    NES_TRACE("OperatorSerializationUtil:: serialize input schema");
    if (!operatorNode->instanceOf<BinaryOperator>()) {
        SchemaSerializationUtil::serializeSchema(operatorNode->as<UnaryOperator>()->getInputSchema(),
                                                 serializedOperator.mutable_inputschema());
    } else {
        auto binaryOperator = operatorNode->as<BinaryOperator>();
        SchemaSerializationUtil::serializeSchema(binaryOperator->getLeftInputSchema(),
                                                 serializedOperator.mutable_leftinputschema());
        SchemaSerializationUtil::serializeSchema(binaryOperator->getRightInputSchema(),
                                                 serializedOperator.mutable_rightinputschema());
    }
}

void OperatorSerializationUtil::deserializeInputSchema(LogicalOperatorPtr operatorNode,
                                                       const SerializableOperator& serializedOperator) {
    // de-serialize operator input schema
    if (!operatorNode->instanceOf<BinaryOperator>()) {
        operatorNode->as<UnaryOperator>()->setInputSchema(
            SchemaSerializationUtil::deserializeSchema(serializedOperator.inputschema()));
    } else {
        auto binaryOperator = operatorNode->as<BinaryOperator>();
        binaryOperator->setLeftInputSchema(SchemaSerializationUtil::deserializeSchema(serializedOperator.leftinputschema()));
        binaryOperator->setRightInputSchema(SchemaSerializationUtil::deserializeSchema(serializedOperator.rightinputschema()));
    }
}

void OperatorSerializationUtil::serializeInferModelOperator(const InferModel::LogicalInferModelOperator& inferModelOperator,
                                                            SerializableOperator& serializedOperator) {

    // serialize infer model operator
    NES_TRACE("OperatorSerializationUtil:: serialize to LogicalInferModelOperator");
    auto inferModelDetails = SerializableOperator_InferModelDetails();

    for (auto& exp : inferModelOperator.getInputFields()) {
        auto* mutableInputFields = inferModelDetails.mutable_inputfields()->Add();
        ExpressionSerializationUtil::serializeExpression(exp, mutableInputFields);
    }
    for (auto& exp : inferModelOperator.getOutputFields()) {
        auto* mutableOutputFields = inferModelDetails.mutable_outputfields()->Add();
        ExpressionSerializationUtil::serializeExpression(exp, mutableOutputFields);
    }

    inferModelDetails.set_mlfilename(inferModelOperator.getDeployedModelPath());
    std::ifstream input(inferModelOperator.getModel(), std::ios::binary);

    std::string bytes((std::istreambuf_iterator<char>(input)), (std::istreambuf_iterator<char>()));
    input.close();
    inferModelDetails.set_mlfilecontent(bytes);

    serializedOperator.mutable_details()->PackFrom(inferModelDetails);
}

LogicalUnaryOperatorPtr
OperatorSerializationUtil::deserializeInferModelOperator(const SerializableOperator_InferModelDetails& inferModelDetails) {
    std::vector<ExpressionNodePtr> inputFields;
    std::vector<ExpressionNodePtr> outputFields;

    for (const auto& serializedInputField : inferModelDetails.inputfields()) {
        auto inputField = ExpressionSerializationUtil::deserializeExpression(serializedInputField);
        inputFields.push_back(inputField);
    }
    for (const auto& serializedOutputField : inferModelDetails.outputfields()) {
        auto outputField = ExpressionSerializationUtil::deserializeExpression(serializedOutputField);
        outputFields.push_back(outputField);
    }

    const auto& content = inferModelDetails.mlfilecontent();
    std::ofstream output(inferModelDetails.mlfilename(), std::ios::binary);
    output << content;
    output.close();

    return LogicalOperatorFactory::createInferModelOperator(inferModelDetails.mlfilename(),
                                                            inputFields,
                                                            outputFields,
                                                            getNextOperatorId());
}

template<typename T, typename D>
void OperatorSerializationUtil::serializeJavaUDFOperator(const T& javaUdfOperator, SerializableOperator& serializedOperator) {
    NES_TRACE("OperatorSerializationUtil:: serialize {}", javaUdfOperator.toString());
    auto javaUdfDetails = D();
    UDFSerializationUtil::serializeJavaUDFDescriptor(javaUdfOperator.getUDFDescriptor(),
                                                     *javaUdfDetails.mutable_javaudfdescriptor());
    serializedOperator.mutable_details()->PackFrom(javaUdfDetails);
}

LogicalUnaryOperatorPtr
OperatorSerializationUtil::deserializeMapJavaUDFOperator(const SerializableOperator_MapJavaUdfDetails& mapJavaUdfDetails) {
    auto javaUDFDescriptor = UDFSerializationUtil::deserializeJavaUDFDescriptor(mapJavaUdfDetails.javaudfdescriptor());
    return LogicalOperatorFactory::createMapUDFLogicalOperator(javaUDFDescriptor);
}

LogicalUnaryOperatorPtr OperatorSerializationUtil::deserializeFlatMapJavaUDFOperator(
    const SerializableOperator_FlatMapJavaUdfDetails& flatMapJavaUdfDetails) {
    auto javaUDFDescriptor = UDFSerializationUtil::deserializeJavaUDFDescriptor(flatMapJavaUdfDetails.javaudfdescriptor());
    return LogicalOperatorFactory::createFlatMapUDFLogicalOperator(javaUDFDescriptor);
}

void OperatorSerializationUtil::serializeOpenCLOperator(const LogicalOpenCLOperator& openCLLogicalOperator,
                                                        SerializableOperator& serializedOperator) {
    NES_TRACE("OperatorSerializationUtil:: serialize to LogicalOpenCLOperator");
    auto openCLDetails = SerializableOperator_OpenCLOperatorDetails();
    UDFSerializationUtil::serializeJavaUDFDescriptor(openCLLogicalOperator.getJavaUDFDescriptor(),
                                                     *openCLDetails.mutable_javaudfdescriptor());
    openCLDetails.set_deviceid(openCLLogicalOperator.getDeviceId());
    openCLDetails.set_openclcode(openCLLogicalOperator.getOpenClCode());
    serializedOperator.mutable_details()->PackFrom(openCLDetails);
}

LogicalUnaryOperatorPtr
OperatorSerializationUtil::deserializeOpenCLOperator(const SerializableOperator_OpenCLOperatorDetails& openCLDetails) {
    auto javaUDFDescriptor = UDFSerializationUtil::deserializeJavaUDFDescriptor(openCLDetails.javaudfdescriptor());
    auto openCLOperator = LogicalOperatorFactory::createOpenCLLogicalOperator(javaUDFDescriptor)->as<LogicalOpenCLOperator>();
    openCLOperator->setDeviceId(openCLDetails.deviceid());
    openCLOperator->setOpenClCode(openCLDetails.openclcode());
    return openCLOperator;
}

void OperatorSerializationUtil::serializeStatisticWindowOperator(
    const Statistic::LogicalStatisticWindowOperator& statisticWindowOperator,
    SerializableOperator& serializedOperator) {
    SerializableOperator_StatisticWindowDetails statisticWindowDetails;

    // 1. Serializing the windowType
    auto windowType = statisticWindowOperator.getWindowType();
    auto timeBasedWindowType = windowType->as<Windowing::TimeBasedWindowType>();
    auto timeCharacteristic = timeBasedWindowType->getTimeCharacteristic();
    auto timeCharacteristicDetails = SerializableOperator_TimeCharacteristic();
    if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::Type::EventTime) {
        timeCharacteristicDetails.set_type(SerializableOperator_TimeCharacteristic_Type_EventTime);
        timeCharacteristicDetails.set_field(timeCharacteristic->getField()->getName());
    } else if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::Type::IngestionTime) {
        timeCharacteristicDetails.set_type(SerializableOperator_TimeCharacteristic_Type_IngestionTime);
    } else {
        NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Characteristic");
    }
    if (windowType->instanceOf<Windowing::TumblingWindow>()) {
        auto tumblingWindow = windowType->as<Windowing::TumblingWindow>();
        auto tumblingWindowDetails = SerializableOperator_TumblingWindow();
        tumblingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
        tumblingWindowDetails.set_size(tumblingWindow->getSize().getTime());
        statisticWindowDetails.mutable_windowtype()->PackFrom(tumblingWindowDetails);
    } else if (windowType->instanceOf<Windowing::SlidingWindow>()) {
        auto slidingWindow = windowType->as<Windowing::SlidingWindow>();
        auto slidingWindowDetails = SerializableOperator_SlidingWindow();
        slidingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
        slidingWindowDetails.set_size(slidingWindow->getSize().getTime());
        slidingWindowDetails.set_slide(slidingWindow->getSlide().getTime());
        statisticWindowDetails.mutable_windowtype()->PackFrom(slidingWindowDetails);
    } else {
        NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Type");
    }

    // 2. Serializing the statistic window descriptor
    StatisticWindowDescriptorMessage statisticWindowDescriptorMessage;
    auto statisticWindowDescriptor = statisticWindowOperator.getWindowStatisticDescriptor();
    ExpressionSerializationUtil::serializeExpression(statisticWindowDescriptor->getField(),
                                                     statisticWindowDescriptorMessage.mutable_field());
    StatisticSerializationUtil::serializeDescriptorDetails(*statisticWindowDescriptor, statisticWindowDescriptorMessage);
    statisticWindowDescriptorMessage.set_width(statisticWindowDescriptor->getWidth());
    statisticWindowDetails.mutable_statisticwindowdescriptor()->CopyFrom(statisticWindowDescriptorMessage);

    // 3. Serializing sending policy and trigger condition
    StatisticSerializationUtil::serializeSendingPolicy(*statisticWindowOperator.getSendingPolicy(),
                                                       *statisticWindowDetails.mutable_sendingpolicy());
    StatisticSerializationUtil::serializeTriggerCondition(*statisticWindowOperator.getTriggerCondition(),
                                                          *statisticWindowDetails.mutable_triggercondition());

    // 4. Serializing the metric hash and then packing everything into serializedOperator
    statisticWindowDetails.set_metrichash(statisticWindowOperator.getMetricHash());
    serializedOperator.mutable_details()->PackFrom(statisticWindowDetails);
}

LogicalUnaryOperatorPtr OperatorSerializationUtil::deserializeStatisticWindowOperator(
    const SerializableOperator_StatisticWindowDetails& statisticWindowDetails) {

    // 1. Deserializing the window type
    const auto serializedWindowType = statisticWindowDetails.windowtype();
    Windowing::WindowTypePtr window;
    if (serializedWindowType.Is<SerializableOperator_TumblingWindow>()) {
        auto serializedTumblingWindow = SerializableOperator_TumblingWindow();
        serializedWindowType.UnpackTo(&serializedTumblingWindow);
        const auto serializedTimeCharacteristic = serializedTumblingWindow.timecharacteristic();
        if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_EventTime) {
            auto field = FieldAccessExpressionNode::create(serializedTimeCharacteristic.field());
            window = Windowing::TumblingWindow::of(Windowing::TimeCharacteristic::createEventTime(field),
                                                   Windowing::TimeMeasure(serializedTumblingWindow.size()));
        } else if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_IngestionTime) {
            window = Windowing::TumblingWindow::of(Windowing::TimeCharacteristic::createIngestionTime(),
                                                   Windowing::TimeMeasure(serializedTumblingWindow.size()));
        } else {
            NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window time characteristic: {}",
                            serializedTimeCharacteristic.DebugString());
        }
    } else if (serializedWindowType.Is<SerializableOperator_SlidingWindow>()) {
        auto serializedSlidingWindow = SerializableOperator_SlidingWindow();
        serializedWindowType.UnpackTo(&serializedSlidingWindow);
        const auto serializedTimeCharacteristic = serializedSlidingWindow.timecharacteristic();
        if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_EventTime) {
            auto field = FieldAccessExpressionNode::create(serializedTimeCharacteristic.field());
            window = Windowing::SlidingWindow::of(Windowing::TimeCharacteristic::createEventTime(field),
                                                  Windowing::TimeMeasure(serializedSlidingWindow.size()),
                                                  Windowing::TimeMeasure(serializedSlidingWindow.slide()));
        } else if (serializedTimeCharacteristic.type() == SerializableOperator_TimeCharacteristic_Type_IngestionTime) {
            window = Windowing::SlidingWindow::of(Windowing::TimeCharacteristic::createIngestionTime(),
                                                  Windowing::TimeMeasure(serializedSlidingWindow.size()),
                                                  Windowing::TimeMeasure(serializedSlidingWindow.slide()));
        } else {
            NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window time characteristic: {}",
                            serializedTimeCharacteristic.DebugString());
        }
    } else {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window type: {}", serializedWindowType.DebugString());
    }

    // 2. Deserializing sending policy and trigger condition
    auto sendingPolicy = StatisticSerializationUtil::deserializeSendingPolicy(statisticWindowDetails.sendingpolicy());
    auto triggerCondition = StatisticSerializationUtil::deserializeTriggerCondition(statisticWindowDetails.triggercondition());

    // 3. Deserializing the statistic descriptor, the metric hash, and then creating the operator
    auto statisticDescriptor =
        StatisticSerializationUtil::deserializeDescriptor(statisticWindowDetails.statisticwindowdescriptor());
    auto metricHash = statisticWindowDetails.metrichash();
    auto statisticWindowOperator = LogicalOperatorFactory::createStatisticBuildOperator(window,
                                                                                        statisticDescriptor,
                                                                                        metricHash,
                                                                                        sendingPolicy,
                                                                                        triggerCondition);

    return statisticWindowOperator;
}

}// namespace NES
