#include <API/Schema.hpp>
#include <GRPC/Serialization/DataTypeSerializationUtil.hpp>
#include <GRPC/Serialization/ExpressionSerializationUtil.hpp>
#include <GRPC/Serialization/OperatorSerializationUtil.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/BroadcastLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Operators/LogicalOperators/SpecializedWindowOperators/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/SpecializedWindowOperators/SliceCreationOperator.hpp>
#include <Operators/LogicalOperators/SpecializedWindowOperators/WindowComputationOperator.hpp>
#include <Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <SerializableOperator.pb.h>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowAggregations/Count.hpp>
#include <Windowing/WindowAggregations/Max.hpp>
#include <Windowing/WindowAggregations/Min.hpp>
#include <Windowing/WindowAggregations/Sum.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>

#ifdef ENABLE_OPC_BUILD
#include "../../../build/SerializableOperator.pb.h"
#include <Nodes/Operators/LogicalOperators/Sinks/OPCSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/OPCSourceDescriptor.hpp>
#endif

namespace NES {

SerializableOperator* OperatorSerializationUtil::serializeOperator(OperatorNodePtr operatorNode, SerializableOperator* serializedOperator) {
    NES_TRACE("OperatorSerializationUtil:: serialize operator " << operatorNode->toString());
    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        // serialize source operator
        NES_TRACE("OperatorSerializationUtil:: serialize to SourceLogicalOperatorNode");
        auto sourceDetails = serializeSourceOperator(operatorNode->as<SourceLogicalOperatorNode>());
        serializedOperator->mutable_details()->PackFrom(sourceDetails);
    } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        // serialize sink operator
        NES_TRACE("OperatorSerializationUtil:: serialize to SinkLogicalOperatorNode");
        auto sinkDetails = serializeSinkOperator(operatorNode->as<SinkLogicalOperatorNode>());
        serializedOperator->mutable_details()->PackFrom(sinkDetails);
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        // serialize filter operator
        NES_TRACE("OperatorSerializationUtil:: serialize to FilterLogicalOperatorNode");
        auto filterDetails = SerializableOperator_FilterDetails();
        auto filterOperator = operatorNode->as<FilterLogicalOperatorNode>();
        // serialize filter expression
        ExpressionSerializationUtil::serializeExpression(filterOperator->getPredicate(), filterDetails.mutable_predicate());
        serializedOperator->mutable_details()->PackFrom(filterDetails);
    } else if (operatorNode->instanceOf<MergeLogicalOperatorNode>()) {
        // serialize merge operator
        NES_TRACE("OperatorSerializationUtil:: serialize to MergeLogicalOperatorNode");
        auto mergeDetails = SerializableOperator_MergeDetails();
        auto mergeOperator = operatorNode->as<MergeLogicalOperatorNode>();
        serializedOperator->mutable_details()->PackFrom(mergeDetails);
    } else if (operatorNode->instanceOf<BroadcastLogicalOperatorNode>()) {
        // serialize merge operator
        NES_TRACE("OperatorSerializationUtil:: serialize to BroadcastLogicalOperatorNode");
        auto broadcastDetails = SerializableOperator_BroadcastDetails();
        auto broadcastOperator = operatorNode->as<BroadcastLogicalOperatorNode>();
        serializedOperator->mutable_details()->PackFrom(broadcastDetails);
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        // serialize map operator
        NES_TRACE("OperatorSerializationUtil:: serialize to MapLogicalOperatorNode");
        auto mapDetails = SerializableOperator_MapDetails();
        auto mapOperator = operatorNode->as<MapLogicalOperatorNode>();
        // serialize map expression
        ExpressionSerializationUtil::serializeExpression(mapOperator->getMapExpression(), mapDetails.mutable_expression());
        serializedOperator->mutable_details()->PackFrom(mapDetails);
    } else if (operatorNode->instanceOf<CentralWindowOperator>()) {
        // serialize window operator
        NES_TRACE("OperatorSerializationUtil:: serialize to CentralWindowOperator");
        auto windowDetails = serializeWindowOperator(operatorNode->as<CentralWindowOperator>());
        serializedOperator->mutable_details()->PackFrom(windowDetails);
    } else if (operatorNode->instanceOf<SliceCreationOperator>()) {
        // serialize window operator
        NES_TRACE("OperatorSerializationUtil:: serialize to SliceCreationOperator");
        auto windowDetails = serializeWindowOperator(operatorNode->as<SliceCreationOperator>());
        serializedOperator->mutable_details()->PackFrom(windowDetails);
    } else if (operatorNode->instanceOf<WindowComputationOperator>()) {
        // serialize window operator
        NES_TRACE("OperatorSerializationUtil:: serialize to WindowComputationOperator");
        auto windowDetails = serializeWindowOperator(operatorNode->as<WindowComputationOperator>());
        serializedOperator->mutable_details()->PackFrom(windowDetails);
    } else {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not serialize this operator: " << operatorNode->toString());
    }
    // serialize input schema
    SchemaSerializationUtil::serializeSchema(operatorNode->getInputSchema(), serializedOperator->mutable_inputschema());
    // serialize output schema
    SchemaSerializationUtil::serializeSchema(operatorNode->getOutputSchema(), serializedOperator->mutable_outputschema());

    // serialize operator id
    serializedOperator->set_operatorid(operatorNode->getId());

    // serialize and append children if the node has any
    for (const auto& child : operatorNode->getChildren()) {
        auto serializedChild = serializedOperator->add_children();
        // serialize this child
        serializeOperator(child->as<OperatorNode>(), serializedChild);
    }

    NES_DEBUG("OperatorSerializationUtil:: serialize " << operatorNode->toString() << " to " << serializedOperator->details().type_url());
    return serializedOperator;
}

OperatorNodePtr OperatorSerializationUtil::deserializeOperator(SerializableOperator* serializedOperator) {
    NES_TRACE("OperatorSerializationUtil:: de-serialize " << serializedOperator->DebugString());
    auto details = serializedOperator->details();
    LogicalOperatorNodePtr operatorNode;
    if (details.Is<SerializableOperator_SourceDetails>()) {
        // de-serialize source operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to SourceLogicalOperator");
        auto serializedSourceDescriptor = SerializableOperator_SourceDetails();
        details.UnpackTo(&serializedSourceDescriptor);
        // de-serialize source descriptor
        auto sourceDescriptor = deserializeSourceDescriptor(&serializedSourceDescriptor);
        operatorNode = LogicalOperatorFactory::createSourceOperator(sourceDescriptor);
    } else if (details.Is<SerializableOperator_SinkDetails>()) {
        // de-serialize sink operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to SinkLogicalOperator");
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails();
        details.UnpackTo(&serializedSinkDescriptor);
        // de-serialize sink descriptor
        auto sinkDescriptor = deserializeSinkDescriptor(&serializedSinkDescriptor);
        operatorNode = LogicalOperatorFactory::createSinkOperator(sinkDescriptor);
    } else if (details.Is<SerializableOperator_FilterDetails>()) {
        // de-serialize filter operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to FilterLogicalOperator");
        auto serializedFilterOperator = SerializableOperator_FilterDetails();
        details.UnpackTo(&serializedFilterOperator);
        // de-serialize filter expression
        auto filterExpression = ExpressionSerializationUtil::deserializeExpression(serializedFilterOperator.mutable_predicate());
        operatorNode = LogicalOperatorFactory::createFilterOperator(filterExpression);
    } else if (details.Is<SerializableOperator_MergeDetails>()) {
        // de-serialize merge operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to MergeLogicalOperator");
        auto serializedMergeDescriptor = SerializableOperator_MergeDetails();
        details.UnpackTo(&serializedMergeDescriptor);
        // de-serialize merge descriptor
        operatorNode = LogicalOperatorFactory::createMergeOperator();
    } else if (details.Is<SerializableOperator_BroadcastDetails>()) {
        // de-serialize broadcast operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to BroadcastLogicalOperator");
        auto serializedBroadcastDescriptor = SerializableOperator_BroadcastDetails();
        details.UnpackTo(&serializedBroadcastDescriptor);
        // de-serialize broadcast descriptor
        operatorNode = LogicalOperatorFactory::createBroadcastOperator();
    } else if (details.Is<SerializableOperator_MapDetails>()) {
        // de-serialize map operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to MapLogicalOperator");
        auto serializedMapOperator = SerializableOperator_MapDetails();
        details.UnpackTo(&serializedMapOperator);
        // de-serialize map expression
        auto fieldAssignmentExpression = ExpressionSerializationUtil::deserializeExpression(serializedMapOperator.mutable_expression());
        operatorNode = LogicalOperatorFactory::createMapOperator(fieldAssignmentExpression->as<FieldAssignmentExpressionNode>());
    } else if (details.Is<SerializableOperator_WindowDetails>()) {
        // de-serialize window operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to WindowLogicalOperator");
        auto serializedWindowOperator = SerializableOperator_WindowDetails();
        details.UnpackTo(&serializedWindowOperator);
        operatorNode = deserializeWindowOperator(&serializedWindowOperator);
    } else {
        NES_THROW_RUNTIME_ERROR("OperatorSerializationUtil: could not de-serialize this serialized operator: ");
    }

    // de-serialize operator output schema
    operatorNode->setOutputSchema(SchemaSerializationUtil::deserializeSchema(serializedOperator->mutable_outputschema()));
    // de-serialize operator input schema
    operatorNode->setInputSchema(SchemaSerializationUtil::deserializeSchema(serializedOperator->mutable_inputschema()));
    // de-serialize operator id
    operatorNode->setId(serializedOperator->operatorid());
    // de-serialize child operators if it has any
    for (auto child : serializedOperator->children()) {
        //de-serialize child
        operatorNode->addChild(deserializeOperator(&child));
    }
    NES_TRACE("OperatorSerializationUtil:: de-serialize " << serializedOperator->DebugString() << " to " << operatorNode->toString());
    return operatorNode;
}

SerializableOperator_WindowDetails OperatorSerializationUtil::serializeWindowOperator(WindowLogicalOperatorNodePtr windowOperator) {
    auto windowDetails = SerializableOperator_WindowDetails();
    auto windowDefinition = windowOperator->getWindowDefinition();

    if (windowDefinition->isKeyed()) {
        windowDetails.set_onkey(windowDefinition->getOnKey()->name);
        windowDetails.set_numberofinputedges(windowDefinition->getNumberOfInputEdges());
    }

    auto windowType = windowDefinition->getWindowType();
    auto timeCharacteristic = windowType->getTimeCharacteristic();
    auto timeCharacteristicDetails = SerializableOperator_WindowDetails_TimeCharacteristic();
    if (timeCharacteristic->getType() == TimeCharacteristic::EventTime) {
        timeCharacteristicDetails.set_type(SerializableOperator_WindowDetails_TimeCharacteristic_Type_EventTime);
        timeCharacteristicDetails.set_field(timeCharacteristic->getField()->name);
    } else if (timeCharacteristic->getType() == TimeCharacteristic::ProcessingTime) {
        timeCharacteristicDetails.set_type(SerializableOperator_WindowDetails_TimeCharacteristic_Type_ProcessingTime);
    } else {
        NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Characteristic");
    }
    if (windowType->isTumblingWindow()) {
        auto tumblingWindow = std::dynamic_pointer_cast<TumblingWindow>(windowType);
        auto tumblingWindowDetails = SerializableOperator_WindowDetails_TumblingWindow();
        tumblingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
        tumblingWindowDetails.set_size(tumblingWindow->getSize().getTime());
        windowDetails.mutable_windowtype()->PackFrom(tumblingWindowDetails);
    } else if (windowType->isSlidingWindow()) {
        auto slidingWindow = std::dynamic_pointer_cast<SlidingWindow>(windowType);
        auto slidingWindowDetails = SerializableOperator_WindowDetails_SlidingWindow();
        slidingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
        slidingWindowDetails.set_size(slidingWindow->getSize().getTime());
        slidingWindowDetails.set_slide(slidingWindow->getSlide().getTime());
        windowDetails.mutable_windowtype()->PackFrom(slidingWindowDetails);
    } else {
        NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Type");
    }

    // serialize aggregation
    auto windowAggregation = windowDetails.mutable_windowaggregation();
    windowAggregation->set_asfield(windowDefinition->getWindowAggregation()->as()->name);
    windowAggregation->set_onfield(windowDefinition->getWindowAggregation()->on()->name);
    // check if SUM aggregation
    if (std::dynamic_pointer_cast<Sum>(windowDefinition->getWindowAggregation()) != nullptr) {
        windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_SUM);
    } else if (std::dynamic_pointer_cast<Max>(windowDefinition->getWindowAggregation()) != nullptr) {
        windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_MAX);
    } else if (std::dynamic_pointer_cast<Min>(windowDefinition->getWindowAggregation()) != nullptr) {
        windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_MIN);
    } else if (std::dynamic_pointer_cast<Count>(windowDefinition->getWindowAggregation()) != nullptr) {
        windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_COUNT);
    } else {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not cast aggregation type");
    }
    auto distributionCharacteristics = SerializableOperator_WindowDetails_DistributionCharacteristic();
    if (windowDefinition->getDistributionType()->getType() == DistributionCharacteristic::Complete) {
        windowDetails.mutable_distrchar()->set_distr(SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Complete);
    } else if (windowDefinition->getDistributionType()->getType() == DistributionCharacteristic::Combining) {
        windowDetails.mutable_distrchar()->set_distr(SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Combining);
    } else if (windowDefinition->getDistributionType()->getType() == DistributionCharacteristic::Slicing) {
        windowDetails.mutable_distrchar()->set_distr(SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Slicing);
    } else {
        NES_NOT_IMPLEMENTED();
    }
    return windowDetails;
}

WindowLogicalOperatorNodePtr OperatorSerializationUtil::deserializeWindowOperator(SerializableOperator_WindowDetails* sinkDetails) {

    auto serializedWindowAggregation = sinkDetails->windowaggregation();
    auto serializedWindowType = sinkDetails->windowtype();

    WindowAggregationPtr aggregation;
    if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_SUM) {
        aggregation = Sum::create(AttributeField::create(serializedWindowAggregation.asfield(), DataTypeFactory::createUndefined()),
                                  AttributeField::create(serializedWindowAggregation.onfield(), DataTypeFactory::createUndefined()));
    } else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_MAX) {
        aggregation = Max::create(AttributeField::create(serializedWindowAggregation.asfield(), DataTypeFactory::createUndefined()),
                                  AttributeField::create(serializedWindowAggregation.onfield(), DataTypeFactory::createUndefined()));
    } else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_MIN) {
        aggregation = Min::create(AttributeField::create(serializedWindowAggregation.asfield(), DataTypeFactory::createUndefined()),
                                  AttributeField::create(serializedWindowAggregation.onfield(), DataTypeFactory::createUndefined()));
    } else if (serializedWindowAggregation.type() == SerializableOperator_WindowDetails_Aggregation_Type_COUNT) {
        aggregation = Count::create(AttributeField::create(serializedWindowAggregation.asfield(), DataTypeFactory::createUndefined()),
                                    AttributeField::create(serializedWindowAggregation.onfield(), DataTypeFactory::createUndefined()));
    } else {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window aggregation: " << serializedWindowAggregation.DebugString());
    }

    WindowTypePtr window;
    if (serializedWindowType.Is<SerializableOperator_WindowDetails_TumblingWindow>()) {
        auto serializedTumblingWindow = SerializableOperator_WindowDetails_TumblingWindow();
        serializedWindowType.UnpackTo(&serializedTumblingWindow);
        auto serializedTimeCharacterisitc = serializedTumblingWindow.timecharacteristic();
        if (serializedTimeCharacterisitc.type() == SerializableOperator_WindowDetails_TimeCharacteristic_Type_EventTime) {
            auto eventTimeField = AttributeField::create(serializedTimeCharacterisitc.field(), DataTypeFactory::createUndefined());
            auto field = Attribute(serializedTimeCharacterisitc.field());
            window = TumblingWindow::of(TimeCharacteristic::createEventTime(field), Milliseconds(serializedTumblingWindow.size()));
        } else if (serializedTimeCharacterisitc.type() == SerializableOperator_WindowDetails_TimeCharacteristic_Type_ProcessingTime) {
            window = TumblingWindow::of(TimeCharacteristic::createProcessingTime(), Milliseconds(serializedTumblingWindow.size()));
        } else {
            NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window time characteristic: " << serializedTimeCharacterisitc.DebugString());
        }
    } else if (serializedWindowType.Is<SerializableOperator_WindowDetails_SlidingWindow>()) {
        auto serializedSlidingWindow = SerializableOperator_WindowDetails_SlidingWindow();
        serializedWindowType.UnpackTo(&serializedSlidingWindow);
        auto serializedTimeCharacterisitc = serializedSlidingWindow.timecharacteristic();
        if (serializedTimeCharacterisitc.type() == SerializableOperator_WindowDetails_TimeCharacteristic_Type_EventTime) {
            auto eventTimeField = AttributeField::create(serializedTimeCharacterisitc.field(), DataTypeFactory::createUndefined());
            auto field = Attribute(serializedTimeCharacterisitc.field());
            window = SlidingWindow::of(TimeCharacteristic::createEventTime(field), Milliseconds(serializedSlidingWindow.size()), Milliseconds(serializedSlidingWindow.slide()));
        } else if (serializedTimeCharacterisitc.type() == SerializableOperator_WindowDetails_TimeCharacteristic_Type_ProcessingTime) {
            window = SlidingWindow::of(TimeCharacteristic::createProcessingTime(), Milliseconds(serializedSlidingWindow.size()), Milliseconds(serializedSlidingWindow.slide()));
        } else {
            NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window time characteristic: " << serializedTimeCharacterisitc.DebugString());
        }
    } else {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize window type: " << serializedWindowType.DebugString());
    }

    auto distrChar = sinkDetails->distrchar();
    DistributionCharacteristicPtr distChar;
    if (distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Complete) {
        NES_DEBUG("OperatorSerializationUtil::deserializeWindowOperator: SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Complete");
        distChar = DistributionCharacteristic::createCompleteWindowType();
    } else if (distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Combining) {
        NES_DEBUG("OperatorSerializationUtil::deserializeWindowOperator: SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Combining");
        distChar = std::make_shared<DistributionCharacteristic>(DistributionCharacteristic::Combining);
    } else if (distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Slicing) {
        NES_DEBUG("OperatorSerializationUtil::deserializeWindowOperator: SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Slicing");
        distChar = std::make_shared<DistributionCharacteristic>(DistributionCharacteristic::Slicing);
    } else {
        NES_NOT_IMPLEMENTED();
    }

    LogicalOperatorNodePtr ptr;
    if (sinkDetails->onkey() == "") {
        auto windowDef = WindowDefinition::create(aggregation, window, distChar);
        if (distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Complete) {
            return LogicalOperatorFactory::createCentralWindowSpecializedOperator(windowDef)->as<CentralWindowOperator>();
        } else if (distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Combining) {
            return LogicalOperatorFactory::createWindowComputationSpecializedOperator(windowDef)->as<WindowComputationOperator>();
        } else if (distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Slicing) {
            return LogicalOperatorFactory::createSliceCreationSpecializedOperator(windowDef)->as<SliceCreationOperator>();
        } else {
            NES_NOT_IMPLEMENTED();
        }
    } else {
        if (distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Complete) {
            return LogicalOperatorFactory::createCentralWindowSpecializedOperator(WindowDefinition::create(AttributeField::create(sinkDetails->onkey(), DataTypeFactory::createUndefined()), aggregation, window, distChar, 1))->as<CentralWindowOperator>();
        } else if (distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Combining) {
            return LogicalOperatorFactory::createWindowComputationSpecializedOperator(WindowDefinition::create(AttributeField::create(sinkDetails->onkey(), DataTypeFactory::createUndefined()), aggregation, window, distChar, sinkDetails->numberofinputedges()))->as<WindowComputationOperator>();
        } else if (distrChar.distr() == SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Slicing) {
            return LogicalOperatorFactory::createSliceCreationSpecializedOperator(WindowDefinition::create(AttributeField::create(sinkDetails->onkey(), DataTypeFactory::createUndefined()), aggregation, window, distChar, 1))->as<SliceCreationOperator>();
        } else {
            NES_NOT_IMPLEMENTED();
        }
    }
}

SerializableOperator_SourceDetails OperatorSerializationUtil::serializeSourceOperator(SourceLogicalOperatorNodePtr sourceOperator) {
    auto sourceDetails = SerializableOperator_SourceDetails();
    auto sourceDescriptor = sourceOperator->getSourceDescriptor();
    serializeSourceSourceDescriptor(sourceDescriptor, &sourceDetails);
    return sourceDetails;
}

OperatorNodePtr OperatorSerializationUtil::deserializeSourceOperator(SerializableOperator_SourceDetails* serializedSourceDetails) {
    auto sourceDescriptor = deserializeSourceDescriptor(serializedSourceDetails);
    return LogicalOperatorFactory::createSourceOperator(sourceDescriptor);
}

SerializableOperator_SinkDetails OperatorSerializationUtil::serializeSinkOperator(SinkLogicalOperatorNodePtr sinkOperator) {
    auto sinkDetails = SerializableOperator_SinkDetails();
    auto sinkDescriptor = sinkOperator->getSinkDescriptor();
    serializeSinkDescriptor(sinkDescriptor, &sinkDetails);
    return sinkDetails;
}

OperatorNodePtr OperatorSerializationUtil::deserializeSinkOperator(SerializableOperator_SinkDetails* sinkDetails) {
    auto sinkDescriptor = deserializeSinkDescriptor(sinkDetails);
    return LogicalOperatorFactory::createSinkOperator(sinkDescriptor);
}

SerializableOperator_SourceDetails* OperatorSerializationUtil::serializeSourceSourceDescriptor(SourceDescriptorPtr sourceDescriptor, SerializableOperator_SourceDetails* sourceDetails) {
    // serialize a source descriptor and all its properties depending of its type
    NES_TRACE("OperatorSerializationUtil:: serialize to SourceDescriptor" << sourceDescriptor->toString());
    if (sourceDescriptor->instanceOf<ZmqSourceDescriptor>()) {
        // serialize zmq source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as SerializableOperator_SourceDetails_SerializableZMQSourceDescriptor");
        auto zmqSourceDescriptor = sourceDescriptor->as<ZmqSourceDescriptor>();
        auto zmqSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableZMQSourceDescriptor();
        zmqSerializedSourceDescriptor.set_host(zmqSourceDescriptor->getHost());
        zmqSerializedSourceDescriptor.set_port(zmqSourceDescriptor->getPort());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(zmqSourceDescriptor->getSchema(), zmqSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(zmqSerializedSourceDescriptor);
    }
#ifdef ENABLE_OPC_BUILD
    else if (sourceDescriptor->instanceOf<OPCSourceDescriptor>()) {
        // serialize opc source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as SerializableOperator_SourceDetails_SerializableOPCSourceDescriptor");
        auto opcSourceDescriptor = sourceDescriptor->as<OPCSourceDescriptor>();
        auto opcSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableOPCSourceDescriptor();
        char* ident = (char*)UA_malloc(sizeof(char)*opcSourceDescriptor->getNodeId()->identifier.string.length+1);
        memcpy(ident, opcSourceDescriptor->getNodeId()->identifier.string.data, opcSourceDescriptor->getNodeId()->identifier.string.length);
        ident[opcSourceDescriptor->getNodeId()->identifier.string.length] = '\0';
        opcSerializedSourceDescriptor.set_url(opcSourceDescriptor->getUrl());
        opcSerializedSourceDescriptor.set_namespaceindex(opcSourceDescriptor->getNodeId()->namespaceIndex);
        opcSerializedSourceDescriptor.set_identifier(ident);
        opcSerializedSourceDescriptor.set_user(opcSourceDescriptor->getUser());
        opcSerializedSourceDescriptor.set_password(opcSourceDescriptor->getPassword());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(opcSourceDescriptor->getSchema(), opcSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(opcSerializedSourceDescriptor);
    }
#endif
    else if (sourceDescriptor->instanceOf<Network::NetworkSourceDescriptor>()) {
        // serialize network source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as SerializableOperator_SourceDetails_SerializableNetworkSourceDescriptor");
        auto networkSourceDescriptor = sourceDescriptor->as<Network::NetworkSourceDescriptor>();
        auto networkSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableNetworkSourceDescriptor();
        networkSerializedSourceDescriptor.mutable_nespartition()->set_queryid(networkSourceDescriptor->getNesPartition().getQueryId());
        networkSerializedSourceDescriptor.mutable_nespartition()->set_operatorid(networkSourceDescriptor->getNesPartition().getOperatorId());
        networkSerializedSourceDescriptor.mutable_nespartition()->set_partitionid(networkSourceDescriptor->getNesPartition().getPartitionId());
        networkSerializedSourceDescriptor.mutable_nespartition()->set_subpartitionid(networkSourceDescriptor->getNesPartition().getSubpartitionId());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(networkSourceDescriptor->getSchema(), networkSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(networkSerializedSourceDescriptor);
    } else if (sourceDescriptor->instanceOf<DefaultSourceDescriptor>()) {
        // serialize default source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as SerializableOperator_SourceDetails_SerializableDefaultSourceDescriptor");
        auto defaultSourceDescriptor = sourceDescriptor->as<DefaultSourceDescriptor>();
        auto defaultSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableDefaultSourceDescriptor();
        defaultSerializedSourceDescriptor.set_frequency(defaultSourceDescriptor->getFrequency());
        defaultSerializedSourceDescriptor.set_numbufferstoprocess(defaultSourceDescriptor->getNumbersOfBufferToProduce());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(defaultSourceDescriptor->getSchema(), defaultSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(defaultSerializedSourceDescriptor);
    } else if (sourceDescriptor->instanceOf<BinarySourceDescriptor>()) {
        // serialize binary source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as SerializableOperator_SourceDetails_SerializableBinarySourceDescriptor");
        auto binarySourceDescriptor = sourceDescriptor->as<BinarySourceDescriptor>();
        auto binarySerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableBinarySourceDescriptor();
        binarySerializedSourceDescriptor.set_filepath(binarySourceDescriptor->getFilePath());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(binarySourceDescriptor->getSchema(), binarySerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(binarySerializedSourceDescriptor);
    } else if (sourceDescriptor->instanceOf<CsvSourceDescriptor>()) {
        // serialize csv source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as SerializableOperator_SourceDetails_SerializableCsvSourceDescriptor");
        auto csvSourceDescriptor = sourceDescriptor->as<CsvSourceDescriptor>();
        auto csvSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableCsvSourceDescriptor();
        csvSerializedSourceDescriptor.set_filepath(csvSourceDescriptor->getFilePath());
        csvSerializedSourceDescriptor.set_frequency(csvSourceDescriptor->getFrequency());
        csvSerializedSourceDescriptor.set_delimiter(csvSourceDescriptor->getDelimiter());
        csvSerializedSourceDescriptor.set_numberoftuplestoproduceperbuffer(csvSourceDescriptor->getNumberOfTuplesToProducePerBuffer());
        csvSerializedSourceDescriptor.set_numbufferstoprocess(csvSourceDescriptor->getNumBuffersToProcess());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(csvSourceDescriptor->getSchema(), csvSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(csvSerializedSourceDescriptor);
    } else if (sourceDescriptor->instanceOf<SenseSourceDescriptor>()) {
        // serialize sense source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as SerializableOperator_SourceDetails_SerializableSenseSourceDescriptor");
        auto senseSourceDescriptor = sourceDescriptor->as<SenseSourceDescriptor>();
        auto senseSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableSenseSourceDescriptor();
        senseSerializedSourceDescriptor.set_udfs(senseSourceDescriptor->getUdfs());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(senseSourceDescriptor->getSchema(), senseSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(senseSerializedSourceDescriptor);
    } else if (sourceDescriptor->instanceOf<LogicalStreamSourceDescriptor>()) {
        // serialize logical stream source descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as SerializableOperator_SourceDetails_SerializableLogicalStreamSourceDescriptor");
        auto logicalStreamSourceDescriptor = sourceDescriptor->as<LogicalStreamSourceDescriptor>();
        auto logicalStreamSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableLogicalStreamSourceDescriptor();
        logicalStreamSerializedSourceDescriptor.set_streamname(logicalStreamSourceDescriptor->getStreamName());
        // serialize source schema
        SchemaSerializationUtil::serializeSchema(logicalStreamSourceDescriptor->getSchema(), logicalStreamSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(logicalStreamSerializedSourceDescriptor);
    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown Source Descriptor Type " << sourceDescriptor->toString());
        throw std::invalid_argument("Unknown Source Descriptor Type");
    }
    return sourceDetails;
}

SourceDescriptorPtr OperatorSerializationUtil::deserializeSourceDescriptor(SerializableOperator_SourceDetails* serializedSourceDetails) {
    // de-serialize source details and all its properties to a SourceDescriptor
    NES_TRACE("OperatorSerializationUtil:: de-serialized SourceDescriptor " << serializedSourceDetails->DebugString());
    const auto& serializedSourceDescriptor = serializedSourceDetails->sourcedescriptor();
    if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableZMQSourceDescriptor>()) {
        // de-serialize zmq source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as ZmqSourceDescriptor");
        auto zmqSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableZMQSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&zmqSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(zmqSerializedSourceDescriptor.release_sourceschema());
        return ZmqSourceDescriptor::create(schema, zmqSerializedSourceDescriptor.host(), zmqSerializedSourceDescriptor.port());
    }
#ifdef ENABLE_OPC_BUILD
    else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableOPCSourceDescriptor>()) {
        // de-serialize opc source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as ZmqSourceDescriptor");
        auto opcSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableOPCSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&opcSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(opcSerializedSourceDescriptor.release_sourceschema());
        UA_NodeId nodeId = UA_NODEID_STRING_ALLOC(opcSerializedSourceDescriptor.namespaceindex(), opcSerializedSourceDescriptor.identifier().c_str());
        return OPCSourceDescriptor::create(schema, opcSerializedSourceDescriptor.url(), &nodeId, opcSerializedSourceDescriptor.user(), opcSerializedSourceDescriptor.password());
    }
#endif
    else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableNetworkSourceDescriptor>()) {
        // de-serialize zmq source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as NetworkSourceDescriptor");
        auto networkSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableNetworkSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&networkSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(networkSerializedSourceDescriptor.release_sourceschema());
        Network::NesPartition nesPartition{networkSerializedSourceDescriptor.nespartition().queryid(),
                                           networkSerializedSourceDescriptor.nespartition().operatorid(),
                                           networkSerializedSourceDescriptor.nespartition().partitionid(),
                                           networkSerializedSourceDescriptor.nespartition().subpartitionid()};
        return Network::NetworkSourceDescriptor::create(schema, nesPartition);
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableDefaultSourceDescriptor>()) {
        // de-serialize default stream source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as DefaultSourceDescriptor");
        auto defaultSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableDefaultSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&defaultSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(defaultSerializedSourceDescriptor.release_sourceschema());
        return DefaultSourceDescriptor::create(schema, defaultSerializedSourceDescriptor.numbufferstoprocess(), defaultSerializedSourceDescriptor.frequency());
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableBinarySourceDescriptor>()) {
        // de-serialize binary source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as BinarySourceDescriptor");
        auto binarySerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableBinarySourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&binarySerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(binarySerializedSourceDescriptor.release_sourceschema());
        return BinarySourceDescriptor::create(schema, binarySerializedSourceDescriptor.filepath());
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableCsvSourceDescriptor>()) {
        // de-serialize csv source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as CsvSourceDescriptor");
        auto csvSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableCsvSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&csvSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(csvSerializedSourceDescriptor.release_sourceschema());
        return CsvSourceDescriptor::create(schema, csvSerializedSourceDescriptor.filepath(), csvSerializedSourceDescriptor.delimiter(), csvSerializedSourceDescriptor.numberoftuplestoproduceperbuffer(), csvSerializedSourceDescriptor.numbufferstoprocess(), csvSerializedSourceDescriptor.frequency());
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableSenseSourceDescriptor>()) {
        // de-serialize sense source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as SenseSourceDescriptor");
        auto senseSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableSenseSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&senseSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(senseSerializedSourceDescriptor.release_sourceschema());
        return SenseSourceDescriptor::create(schema, senseSerializedSourceDescriptor.udfs());
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableLogicalStreamSourceDescriptor>()) {
        // de-serialize logical stream source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as LogicalStreamSourceDescriptor");
        auto logicalStreamSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableLogicalStreamSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&logicalStreamSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(logicalStreamSerializedSourceDescriptor.release_sourceschema());
        SourceDescriptorPtr logicalStreamSourceDescriptor = LogicalStreamSourceDescriptor::create(logicalStreamSerializedSourceDescriptor.streamname());
        logicalStreamSourceDescriptor->setSchema(schema);
        return logicalStreamSourceDescriptor;
    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown Source Descriptor Type " << serializedSourceDescriptor.type_url());
        throw std::invalid_argument("Unknown Source Descriptor Type");
    }
}
SerializableOperator_SinkDetails* OperatorSerializationUtil::serializeSinkDescriptor(SinkDescriptorPtr sinkDescriptor, SerializableOperator_SinkDetails* sinkDetails) {
    // serialize a sink descriptor and all its properties depending of its type
    NES_DEBUG("OperatorSerializationUtil:: serialized SinkDescriptor ");
    if (sinkDescriptor->instanceOf<PrintSinkDescriptor>()) {
        // serialize print sink descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as SerializableOperator_SinkDetails_SerializablePrintSinkDescriptor");
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializablePrintSinkDescriptor();
        sinkDetails->mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
    } else if (sinkDescriptor->instanceOf<ZmqSinkDescriptor>()) {
        // serialize zmq sink descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as SerializableOperator_SinkDetails_SerializableZMQSinkDescriptor");
        auto zmqSinkDescriptor = sinkDescriptor->as<ZmqSinkDescriptor>();
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableZMQSinkDescriptor();
        serializedSinkDescriptor.set_port(zmqSinkDescriptor->getPort());
        serializedSinkDescriptor.set_isinternal(zmqSinkDescriptor->isInternal());
        serializedSinkDescriptor.set_host(zmqSinkDescriptor->getHost());
        sinkDetails->mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
    }
#ifdef ENABLE_OPC_BUILD
    else if (sinkDescriptor->instanceOf<OPCSinkDescriptor>()) {
        // serialize opc sink descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as SerializableOperator_SinkDetails_SerializableOPCSinkDescriptor");
        auto opcSinkDescriptor = sinkDescriptor->as<OPCSinkDescriptor>();
        auto opcSerializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableOPCSinkDescriptor();
        char* ident = (char*)UA_malloc(sizeof(char)*opcSinkDescriptor->getNodeId()->identifier.string.length+1);
        memcpy(ident, opcSinkDescriptor->getNodeId()->identifier.string.data, opcSinkDescriptor->getNodeId()->identifier.string.length);
        ident[opcSinkDescriptor->getNodeId()->identifier.string.length] = '\0';
        opcSerializedSinkDescriptor.set_url(opcSinkDescriptor->getUrl());
        opcSerializedSinkDescriptor.set_namespaceindex(opcSinkDescriptor->getNodeId()->namespaceIndex);
        opcSerializedSinkDescriptor.set_identifier(ident);
        opcSerializedSinkDescriptor.set_user(opcSinkDescriptor->getUser());
        opcSerializedSinkDescriptor.set_password(opcSinkDescriptor->getPassword());
        sinkDetails->mutable_sinkdescriptor()->PackFrom(opcSerializedSinkDescriptor);
    }
#endif
    else if (sinkDescriptor->instanceOf<Network::NetworkSinkDescriptor>()) {
        // serialize zmq sink descriptor
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as SerializableOperator_SinkDetails_SerializableNetworkSinkDescriptor");
        auto networkSinkDescriptor = sinkDescriptor->as<Network::NetworkSinkDescriptor>();
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableNetworkSinkDescriptor();
        //set details of NesPartition
        auto serializedNesPartition = serializedSinkDescriptor.mutable_nespartition();
        auto nesPartition = networkSinkDescriptor->getNesPartition();
        serializedNesPartition->set_queryid(nesPartition.getQueryId());
        serializedNesPartition->set_operatorid(nesPartition.getOperatorId());
        serializedNesPartition->set_partitionid(nesPartition.getPartitionId());
        serializedNesPartition->set_subpartitionid(nesPartition.getSubpartitionId());
        //set details of NodeLocation
        auto serializedNodeLocation = serializedSinkDescriptor.mutable_nodelocation();
        auto nodeLocation = networkSinkDescriptor->getNodeLocation();
        serializedNodeLocation->set_nodeid(nodeLocation.getNodeId());
        serializedNodeLocation->set_hostname(nodeLocation.getHostname());
        serializedNodeLocation->set_port(nodeLocation.getPort());
        // set reconnection details
        auto s = std::chrono::duration_cast<std::chrono::seconds>(networkSinkDescriptor->getWaitTime());
        serializedSinkDescriptor.set_waittime(s.count());
        serializedSinkDescriptor.set_retrytimes(networkSinkDescriptor->getRetryTimes());
        //pack to output
        sinkDetails->mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
    } else if (sinkDescriptor->instanceOf<FileSinkDescriptor>()) {
        // serialize file sink descriptor. The file sink has different types which have to be set correctly
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as SerializableOperator_SinkDetails_SerializableFileSinkDescriptor");
        auto fileSinkDescriptor = sinkDescriptor->as<FileSinkDescriptor>();
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableFileSinkDescriptor();

        serializedSinkDescriptor.set_filepath(fileSinkDescriptor->getFileName());
        serializedSinkDescriptor.set_append(fileSinkDescriptor->getAppend());

        auto format = fileSinkDescriptor->getSinkFormatAsString();
        if (format == "JSON_FORMAT") {
            serializedSinkDescriptor.set_sinkformat("JSON_FORMAT");
        } else if (format == "CSV_FORMAT") {
            serializedSinkDescriptor.set_sinkformat("CSV_FORMAT");
        } else if (format == "NES_FORMAT") {
            serializedSinkDescriptor.set_sinkformat("NES_FORMAT");
        } else if (format == "TEXT_FORMAT") {
            serializedSinkDescriptor.set_sinkformat("TEXT_FORMAT");
        } else {
            NES_ERROR("serializeSinkDescriptor: format not supported");
        }
        sinkDetails->mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown Sink Descriptor Type - " << sinkDescriptor->toString());
        throw std::invalid_argument("Unknown Sink Descriptor Type");
    }
    return sinkDetails;
}

SinkDescriptorPtr OperatorSerializationUtil::deserializeSinkDescriptor(SerializableOperator_SinkDetails* sinkDetails) {
    // de-serialize a sink descriptor and all its properties to a SinkDescriptor.
    NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor " << sinkDetails->DebugString());
    const auto& deserializedSinkDescriptor = sinkDetails->sinkdescriptor();
    if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializablePrintSinkDescriptor>()) {
        // de-serialize print sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as PrintSinkDescriptor");
        return PrintSinkDescriptor::create();
    } else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableZMQSinkDescriptor>()) {
        // de-serialize zmq sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as ZmqSinkDescriptor");
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableZMQSinkDescriptor();
        deserializedSinkDescriptor.UnpackTo(&serializedSinkDescriptor);
        return ZmqSinkDescriptor::create(serializedSinkDescriptor.host(), serializedSinkDescriptor.port(), serializedSinkDescriptor.isinternal());
    }
#ifdef ENABLE_OPC_BUILD
    else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableOPCSinkDescriptor>()) {
        // de-serialize opc sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as OPCSinkDescriptor");
        auto opcSerializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableOPCSinkDescriptor();
        deserializedSinkDescriptor.UnpackTo(&opcSerializedSinkDescriptor);
        UA_NodeId nodeId = UA_NODEID_STRING_ALLOC(opcSerializedSinkDescriptor.namespaceindex(), opcSerializedSinkDescriptor.identifier().c_str());
        return OPCSinkDescriptor::create(opcSerializedSinkDescriptor.url(), &nodeId, opcSerializedSinkDescriptor.user(), opcSerializedSinkDescriptor.password());
    }
#endif
    else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableNetworkSinkDescriptor>()) {
        // de-serialize zmq sink descriptor
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as NetworkSinkDescriptor");
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableNetworkSinkDescriptor();
        deserializedSinkDescriptor.UnpackTo(&serializedSinkDescriptor);
        Network::NesPartition nesPartition{serializedSinkDescriptor.nespartition().queryid(),
                                           serializedSinkDescriptor.nespartition().operatorid(),
                                           serializedSinkDescriptor.nespartition().partitionid(),
                                           serializedSinkDescriptor.nespartition().subpartitionid()};
        Network::NodeLocation nodeLocation{serializedSinkDescriptor.nodelocation().nodeid(),
                                           serializedSinkDescriptor.nodelocation().hostname(),
                                           serializedSinkDescriptor.nodelocation().port()};
        auto waitTime = std::chrono::seconds(serializedSinkDescriptor.waittime());
        return Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, waitTime, serializedSinkDescriptor.retrytimes());
    } else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableFileSinkDescriptor>()) {
        // de-serialize file sink descriptor
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableFileSinkDescriptor();
        deserializedSinkDescriptor.UnpackTo(&serializedSinkDescriptor);
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as FileSinkDescriptor");
        return FileSinkDescriptor::create(serializedSinkDescriptor.filepath(), serializedSinkDescriptor.sinkformat(), serializedSinkDescriptor.append() == true ? "APPEND" : "OVERWRITE");
    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown sink Descriptor Type " << sinkDetails->DebugString());
        throw std::invalid_argument("Unknown Sink Descriptor Type");
    }
}
}// namespace NES