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
#include <Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/Windowing/SliceCreationOperator.hpp>
#include <Operators/LogicalOperators/Windowing/WindowComputationOperator.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/Utils/ExpressionToFOLUtil.hpp>
#include <Optimizer/Utils/OperatorToFOLUtil.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowAggregations/CountAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/MaxAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/MinAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <z3++.h>

#ifdef ENABLE_OPC_BUILD
#include <Operators/LogicalOperators/Sinks/OPCSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/OPCSourceDescriptor.hpp>
#endif

namespace NES {

z3::expr OperatorToFOLUtil::serializeOperator(OperatorNodePtr operatorNode, z3::context& context) {
    NES_TRACE("OperatorSerializationUtil:: serialize operator " << operatorNode->toString());
    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        // serialize source operator
        NES_TRACE("OperatorSerializationUtil:: serialize to SourceLogicalOperatorNode");
        SourceLogicalOperatorNodePtr sourceOperator = operatorNode->as<SourceLogicalOperatorNode>();
        z3::expr exprVar = context.constant(context.str_symbol("streamName"), context.string_sort());
        std::string streamName = sourceOperator->getSourceDescriptor()->getStreamName();
        z3::expr exprVal = context.string_val(streamName);
        return to_expr(context, Z3_mk_eq(context, exprVar, exprVal));
    } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        // serialize sink operator
        NES_TRACE("OperatorSerializationUtil:: serialize to SinkLogicalOperatorNode");
        return context.string_val("sink");
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        // serialize filter operator
        NES_TRACE("OperatorSerializationUtil:: serialize to FilterLogicalOperatorNode");
        auto filterOperator = operatorNode->as<FilterLogicalOperatorNode>();
        return ExpressionToFOLUtil::serializeExpression(filterOperator->getPredicate(), context);
    } else if (operatorNode->instanceOf<MergeLogicalOperatorNode>()) {
        // serialize merge operator
        NES_TRACE("OperatorSerializationUtil:: serialize to MergeLogicalOperatorNode");
        return context.string_val("merge");
    } else if (operatorNode->instanceOf<BroadcastLogicalOperatorNode>()) {
        // serialize merge operator
        NES_TRACE("OperatorSerializationUtil:: serialize to BroadcastLogicalOperatorNode");
        return context.string_val("broadcast");
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        // serialize map operator
        NES_TRACE("OperatorSerializationUtil:: serialize to MapLogicalOperatorNode");
        auto mapOperator = operatorNode->as<MapLogicalOperatorNode>();
        // serialize map expression
        return ExpressionToFOLUtil::serializeExpression(mapOperator->getMapExpression(), context);
    } else if (operatorNode->instanceOf<WindowOperatorNode>()) {
        // serialize window operator
        NES_TRACE("OperatorSerializationUtil:: serialize to CentralWindowOperator");
        return context.string_val("window");
    }
    NES_THROW_RUNTIME_ERROR("OperatorSerializationUtil: could not serialize this operator: " + operatorNode->toString());

    //    // serialize input schema
    //    SchemaSerializationUtil::serializeSchema(operatorNode->getInputSchema(), serializedOperator->mutable_inputschema());
    //    // serialize output schema
    //    SchemaSerializationUtil::serializeSchema(operatorNode->getOutputSchema(), serializedOperator->mutable_outputschema());
    //
    //    // serialize operator id
    //    serializedOperator->set_operatorid(operatorNode->getId());
    //
    //    // serialize and append children if the node has any
    //    for (const auto& child : operatorNode->getChildren()) {
    //        auto serializedChild = serializedOperator->add_children();
    //        // serialize this child
    //        serializeOperator(child->as<OperatorNode>(), serializedChild);
    //    }
    //
    //    NES_DEBUG("OperatorSerializationUtil:: serialize " << operatorNode->toString() << " to " << serializedOperator->details().type_url());
    //    return serializedOperator;
}

//z3::expr OperatorToFOLUtil::serializeWindowOperator(WindowOperatorNodePtr windowOperator, z3::context& context) {
//    auto windowDetails = SerializableOperator_WindowDetails();
//    auto windowDefinition = windowOperator->getWindowDefinition();
//
//    if (windowDefinition->isKeyed()) {
//        ExpressionSerializationUtil::serializeExpression(windowDefinition->getOnKey(), windowDetails.mutable_onkey());
//        windowDetails.set_numberofinputedges(windowDefinition->getNumberOfInputEdges());
//    }
//
//    auto windowType = windowDefinition->getWindowType();
//    auto timeCharacteristic = windowType->getTimeCharacteristic();
//    auto timeCharacteristicDetails = SerializableOperator_WindowDetails_TimeCharacteristic();
//    if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::EventTime) {
//        timeCharacteristicDetails.set_type(SerializableOperator_WindowDetails_TimeCharacteristic_Type_EventTime);
//        timeCharacteristicDetails.set_field(timeCharacteristic->getField()->name);
//    } else if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::ProcessingTime) {
//        timeCharacteristicDetails.set_type(SerializableOperator_WindowDetails_TimeCharacteristic_Type_ProcessingTime);
//    } else {
//        NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Characteristic");
//    }
//    if (windowType->isTumblingWindow()) {
//        auto tumblingWindow = std::dynamic_pointer_cast<Windowing::TumblingWindow>(windowType);
//        auto tumblingWindowDetails = SerializableOperator_WindowDetails_TumblingWindow();
//        tumblingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
//        tumblingWindowDetails.set_size(tumblingWindow->getSize().getTime());
//        windowDetails.mutable_windowtype()->PackFrom(tumblingWindowDetails);
//    } else if (windowType->isSlidingWindow()) {
//        auto slidingWindow = std::dynamic_pointer_cast<Windowing::SlidingWindow>(windowType);
//        auto slidingWindowDetails = SerializableOperator_WindowDetails_SlidingWindow();
//        slidingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
//        slidingWindowDetails.set_size(slidingWindow->getSize().getTime());
//        slidingWindowDetails.set_slide(slidingWindow->getSlide().getTime());
//        windowDetails.mutable_windowtype()->PackFrom(slidingWindowDetails);
//    } else {
//        NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Type");
//    }
//
//    // serialize aggregation
//    auto windowAggregation = windowDetails.mutable_windowaggregation();
//    ExpressionSerializationUtil::serializeExpression(windowDefinition->getWindowAggregation()->as(), windowAggregation->mutable_asfield());
//    ExpressionSerializationUtil::serializeExpression(windowDefinition->getWindowAggregation()->on(), windowAggregation->mutable_onfield());
//
//    switch (windowDefinition->getWindowAggregation()->getType()) {
//        case Windowing::WindowAggregationDescriptor::Count:
//            windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_COUNT);
//            break;
//        case Windowing::WindowAggregationDescriptor::Max:
//            windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_MAX);
//            break;
//        case Windowing::WindowAggregationDescriptor::Min:
//            windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_MIN);
//            break;
//        case Windowing::WindowAggregationDescriptor::Sum:
//            windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_SUM);
//            break;
//        default:
//            NES_FATAL_ERROR("OperatorSerializationUtil: could not cast aggregation type");
//    }
//
//    auto distributionCharacteristics = SerializableOperator_WindowDetails_DistributionCharacteristic();
//    if (windowDefinition->getDistributionType()->getType() == Windowing::DistributionCharacteristic::Complete) {
//        windowDetails.mutable_distrchar()->set_distr(SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Complete);
//    } else if (windowDefinition->getDistributionType()->getType() == Windowing::DistributionCharacteristic::Combining) {
//        windowDetails.mutable_distrchar()->set_distr(SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Combining);
//    } else if (windowDefinition->getDistributionType()->getType() == Windowing::DistributionCharacteristic::Slicing) {
//        windowDetails.mutable_distrchar()->set_distr(SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Slicing);
//    } else {
//        NES_NOT_IMPLEMENTED();
//    }
//    return windowDetails;
//}

//z3::expr OperatorToFOLUtil::serializeSourceOperator(SourceLogicalOperatorNodePtr sourceOperator, z3::context& context) {
//    auto sourceDetails = SerializableOperator_SourceDetails();
//    auto sourceDescriptor = sourceOperator->getSourceDescriptor();
//    serializeSourceSourceDescriptor(sourceDescriptor, &sourceDetails);
//    return sourceDetails;
//}
//
//z3::expr OperatorToFOLUtil::serializeSinkOperator(SinkLogicalOperatorNodePtr sinkOperator, z3::context& context) {
//    auto sinkDetails = SerializableOperator_SinkDetails();
//    auto sinkDescriptor = sinkOperator->getSinkDescriptor();
//    serializeSinkDescriptor(sinkDescriptor, &sinkDetails);
//    return sinkDetails;
//}

//z3::expr OperatorToFOLUtil::serializeSourceSourceDescriptor(SourceDescriptorPtr sourceDescriptor, z3::context& context) {
//    // serialize a source descriptor and all its properties depending of its type
//    NES_TRACE("OperatorSerializationUtil:: serialize to SourceDescriptor" << sourceDescriptor->toString());
//    if (sourceDescriptor->instanceOf<ZmqSourceDescriptor>()) {
//        // serialize zmq source descriptor
//        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as SerializableOperator_SourceDetails_SerializableZMQSourceDescriptor");
//        auSerializableOperator_SourceDetailsto zmqSourceDescriptor = sourceDescriptor->as<ZmqSourceDescriptor>();
//        auto zmqSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableZMQSourceDescriptor();
//        zmqSerializedSourceDescriptor.set_host(zmqSourceDescriptor->getHost());
//        zmqSerializedSourceDescriptor.set_port(zmqSourceDescriptor->getPort());
//        // serialize source schema
//        SchemaSerializationUtil::serializeSchema(zmqSourceDescriptor->getSchema(), zmqSerializedSourceDescriptor.mutable_sourceschema());
//        sourceDetails->mutable_sourcedescriptor()->PackFrom(zmqSerializedSourceDescriptor);
//    }
//#ifdef ENABLE_OPC_BUILD
//    else if (sourceDescriptor->instanceOf<OPCSourceDescriptor>()) {
//        // serialize opc source descriptor
//        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as SerializableOperator_SourceDetails_SerializableOPCSourceDescriptor");
//        auto opcSourceDescriptor = sourceDescriptor->as<OPCSourceDescriptor>();
//        auto opcSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableOPCSourceDescriptor();
//        char* ident = (char*) UA_malloc(sizeof(char) * opcSourceDescriptor->getNodeId().identifier.string.length + 1);
//        memcpy(ident, opcSourceDescriptor->getNodeId().identifier.string.data, opcSourceDescriptor->getNodeId().identifier.string.length);
//        ident[opcSourceDescriptor->getNodeId().identifier.string.length] = '\0';
//        opcSerializedSourceDescriptor.set_identifier(ident);
//        opcSerializedSourceDescriptor.set_url(opcSourceDescriptor->getUrl());
//        opcSerializedSourceDescriptor.set_namespaceindex(opcSourceDescriptor->getNodeId().namespaceIndex);
//        opcSerializedSourceDescriptor.set_identifiertype(opcSourceDescriptor->getNodeId().identifierType);
//        opcSerializedSourceDescriptor.set_user(opcSourceDescriptor->getUser());
//        opcSerializedSourceDescriptor.set_password(opcSourceDescriptor->getPassword());
//        // serialize source schema
//        SchemaSerializationUtil::serializeSchema(opcSourceDescriptor->getSchema(), opcSerializedSourceDescriptor.mutable_sourceschema());
//        sourceDetails->mutable_sourcedescriptor()->PackFrom(opcSerializedSourceDescriptor);
//    }
//#endif
//    else if (sourceDescriptor->instanceOf<Network::NetworkSourceDescriptor>()) {
//        // serialize network source descriptor
//        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as SerializableOperator_SourceDetails_SerializableNetworkSourceDescriptor");
//        auto networkSourceDescriptor = sourceDescriptor->as<Network::NetworkSourceDescriptor>();
//        auto networkSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableNetworkSourceDescriptor();
//        networkSerializedSourceDescriptor.mutable_nespartition()->set_queryid(networkSourceDescriptor->getNesPartition().getQueryId());
//        networkSerializedSourceDescriptor.mutable_nespartition()->set_operatorid(networkSourceDescriptor->getNesPartition().getOperatorId());
//        networkSerializedSourceDescriptor.mutable_nespartition()->set_partitionid(networkSourceDescriptor->getNesPartition().getPartitionId());
//        networkSerializedSourceDescriptor.mutable_nespartition()->set_subpartitionid(networkSourceDescriptor->getNesPartition().getSubpartitionId());
//        // serialize source schema
//        SchemaSerializationUtil::serializeSchema(networkSourceDescriptor->getSchema(), networkSerializedSourceDescriptor.mutable_sourceschema());
//        sourceDetails->mutable_sourcedescriptor()->PackFrom(networkSerializedSourceDescriptor);
//    } else if (sourceDescriptor->instanceOf<DefaultSourceDescriptor>()) {
//        // serialize default source descriptor
//        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as SerializableOperator_SourceDetails_SerializableDefaultSourceDescriptor");
//        auto defaultSourceDescriptor = sourceDescriptor->as<DefaultSourceDescriptor>();
//        auto defaultSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableDefaultSourceDescriptor();
//        defaultSerializedSourceDescriptor.set_frequency(defaultSourceDescriptor->getFrequency());
//        defaultSerializedSourceDescriptor.set_numbufferstoprocess(defaultSourceDescriptor->getNumbersOfBufferToProduce());
//        // serialize source schema
//        SchemaSerializationUtil::serializeSchema(defaultSourceDescriptor->getSchema(), defaultSerializedSourceDescriptor.mutable_sourceschema());
//        sourceDetails->mutable_sourcedescriptor()->PackFrom(defaultSerializedSourceDescriptor);
//    } else if (sourceDescriptor->instanceOf<BinarySourceDescriptor>()) {
//        // serialize binary source descriptor
//        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as SerializableOperator_SourceDetails_SerializableBinarySourceDescriptor");
//        auto binarySourceDescriptor = sourceDescriptor->as<BinarySourceDescriptor>();
//        auto binarySerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableBinarySourceDescriptor();
//        binarySerializedSourceDescriptor.set_filepath(binarySourceDescriptor->getFilePath());
//        // serialize source schema
//        SchemaSerializationUtil::serializeSchema(binarySourceDescriptor->getSchema(), binarySerializedSourceDescriptor.mutable_sourceschema());
//        sourceDetails->mutable_sourcedescriptor()->PackFrom(binarySerializedSourceDescriptor);
//    } else if (sourceDescriptor->instanceOf<CsvSourceDescriptor>()) {
//        // serialize csv source descriptor
//        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as SerializableOperator_SourceDetails_SerializableCsvSourceDescriptor");
//        auto csvSourceDescriptor = sourceDescriptor->as<CsvSourceDescriptor>();
//        auto csvSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableCsvSourceDescriptor();
//        csvSerializedSourceDescriptor.set_filepath(csvSourceDescriptor->getFilePath());
//        csvSerializedSourceDescriptor.set_frequency(csvSourceDescriptor->getFrequency());
//        csvSerializedSourceDescriptor.set_delimiter(csvSourceDescriptor->getDelimiter());
//        csvSerializedSourceDescriptor.set_numberoftuplestoproduceperbuffer(csvSourceDescriptor->getNumberOfTuplesToProducePerBuffer());
//        csvSerializedSourceDescriptor.set_numbufferstoprocess(csvSourceDescriptor->getNumBuffersToProcess());
//        // serialize source schema
//        SchemaSerializationUtil::serializeSchema(csvSourceDescriptor->getSchema(), csvSerializedSourceDescriptor.mutable_sourceschema());
//        sourceDetails->mutable_sourcedescriptor()->PackFrom(csvSerializedSourceDescriptor);
//    } else if (sourceDescriptor->instanceOf<SenseSourceDescriptor>()) {
//        // serialize sense source descriptor
//        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as SerializableOperator_SourceDetails_SerializableSenseSourceDescriptor");
//        auto senseSourceDescriptor = sourceDescriptor->as<SenseSourceDescriptor>();
//        auto senseSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableSenseSourceDescriptor();
//        senseSerializedSourceDescriptor.set_udfs(senseSourceDescriptor->getUdfs());
//        // serialize source schema
//        SchemaSerializationUtil::serializeSchema(senseSourceDescriptor->getSchema(), senseSerializedSourceDescriptor.mutable_sourceschema());
//        sourceDetails->mutable_sourcedescriptor()->PackFrom(senseSerializedSourceDescriptor);
//    } else if (sourceDescriptor->instanceOf<LogicalStreamSourceDescriptor>()) {
//        // serialize logical stream source descriptor
//        NES_TRACE("OperatorSerializationUtil:: serialized SourceDescriptor as SerializableOperator_SourceDetails_SerializableLogicalStreamSourceDescriptor");
//        auto logicalStreamSourceDescriptor = sourceDescriptor->as<LogicalStreamSourceDescriptor>();
//        auto logicalStreamSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableLogicalStreamSourceDescriptor();
//        logicalStreamSerializedSourceDescriptor.set_streamname(logicalStreamSourceDescriptor->getStreamName());
//        // serialize source schema
//        SchemaSerializationUtil::serializeSchema(logicalStreamSourceDescriptor->getSchema(), logicalStreamSerializedSourceDescriptor.mutable_sourceschema());
//        sourceDetails->mutable_sourcedescriptor()->PackFrom(logicalStreamSerializedSourceDescriptor);
//    } else {
//        NES_ERROR("OperatorSerializationUtil: Unknown Source Descriptor Type " << sourceDescriptor->toString());
//        throw std::invalid_argument("Unknown Source Descriptor Type");
//    }
//    return sourceDetails;
//}
//
//z3::expr OperatorToFOLUtil::serializeSinkDescriptor(SinkDescriptorPtr sinkDescriptor, z3::context& context) {
//    // serialize a sink descriptor and all its properties depending of its type
//    NES_DEBUG("OperatorSerializationUtil:: serialized SinkDescriptor ");
//    if (sinkDescriptor->instanceOf<PrintSinkDescriptor>()) {
//        // serialize print sink descriptor
//        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as SerializableOperator_SinkDetails_SerializablePrintSinkDescriptor");
//        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializablePrintSinkDescriptor();
//        sinkDetails->mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
//    } else if (sinkDescriptor->instanceOf<ZmqSinkDescriptor>()) {
//        // serialize zmq sink descriptor
//        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as SerializableOperator_SinkDetails_SerializableZMQSinkDescriptor");
//        auto zmqSinkDescriptor = sinkDescriptor->as<ZmqSinkDescriptor>();
//        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableZMQSinkDescriptor();
//        serializedSinkDescriptor.set_port(zmqSinkDescriptor->getPort());
//        serializedSinkDescriptor.set_isinternal(zmqSinkDescriptor->isInternal());
//        serializedSinkDescriptor.set_host(zmqSinkDescriptor->getHost());
//        sinkDetails->mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
//    }
//#ifdef ENABLE_OPC_BUILD
//    else if (sinkDescriptor->instanceOf<OPCSinkDescriptor>()) {
//        // serialize opc sink descriptor
//        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as SerializableOperator_SinkDetails_SerializableOPCSinkDescriptor");
//        auto opcSinkDescriptor = sinkDescriptor->as<OPCSinkDescriptor>();
//        auto opcSerializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableOPCSinkDescriptor();
//        char* ident = (char*) UA_malloc(sizeof(char) * opcSinkDescriptor->getNodeId().identifier.string.length + 1);
//        memcpy(ident, opcSinkDescriptor->getNodeId().identifier.string.data, opcSinkDescriptor->getNodeId().identifier.string.length);
//        ident[opcSinkDescriptor->getNodeId().identifier.string.length] = '\0';
//        opcSerializedSinkDescriptor.set_identifier(ident);
//        free(ident);
//        opcSerializedSinkDescriptor.set_url(opcSinkDescriptor->getUrl());
//        opcSerializedSinkDescriptor.set_namespaceindex(opcSinkDescriptor->getNodeId().namespaceIndex);
//        opcSerializedSinkDescriptor.set_identifiertype(opcSinkDescriptor->getNodeId().identifierType);
//        opcSerializedSinkDescriptor.set_user(opcSinkDescriptor->getUser());
//        opcSerializedSinkDescriptor.set_password(opcSinkDescriptor->getPassword());
//        sinkDetails->mutable_sinkdescriptor()->PackFrom(opcSerializedSinkDescriptor);
//    }
//#endif
//    else if (sinkDescriptor->instanceOf<Network::NetworkSinkDescriptor>()) {
//        // serialize zmq sink descriptor
//        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as SerializableOperator_SinkDetails_SerializableNetworkSinkDescriptor");
//        auto networkSinkDescriptor = sinkDescriptor->as<Network::NetworkSinkDescriptor>();
//        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableNetworkSinkDescriptor();
//        //set details of NesPartition
//        auto serializedNesPartition = serializedSinkDescriptor.mutable_nespartition();
//        auto nesPartition = networkSinkDescriptor->getNesPartition();
//        serializedNesPartition->set_queryid(nesPartition.getQueryId());
//        serializedNesPartition->set_operatorid(nesPartition.getOperatorId());
//        serializedNesPartition->set_partitionid(nesPartition.getPartitionId());
//        serializedNesPartition->set_subpartitionid(nesPartition.getSubpartitionId());
//        //set details of NodeLocation
//        auto serializedNodeLocation = serializedSinkDescriptor.mutable_nodelocation();
//        auto nodeLocation = networkSinkDescriptor->getNodeLocation();
//        serializedNodeLocation->set_nodeid(nodeLocation.getNodeId());
//        serializedNodeLocation->set_hostname(nodeLocation.getHostname());
//        serializedNodeLocation->set_port(nodeLocation.getPort());
//        // set reconnection details
//        auto s = std::chrono::duration_cast<std::chrono::seconds>(networkSinkDescriptor->getWaitTime());
//        serializedSinkDescriptor.set_waittime(s.count());
//        serializedSinkDescriptor.set_retrytimes(networkSinkDescriptor->getRetryTimes());
//        //pack to output
//        sinkDetails->mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
//    } else if (sinkDescriptor->instanceOf<FileSinkDescriptor>()) {
//        // serialize file sink descriptor. The file sink has different types which have to be set correctly
//        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as SerializableOperator_SinkDetails_SerializableFileSinkDescriptor");
//        auto fileSinkDescriptor = sinkDescriptor->as<FileSinkDescriptor>();
//        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableFileSinkDescriptor();
//
//        serializedSinkDescriptor.set_filepath(fileSinkDescriptor->getFileName());
//        serializedSinkDescriptor.set_append(fileSinkDescriptor->getAppend());
//
//        auto format = fileSinkDescriptor->getSinkFormatAsString();
//        if (format == "JSON_FORMAT") {
//            serializedSinkDescriptor.set_sinkformat("JSON_FORMAT");
//        } else if (format == "CSV_FORMAT") {
//            serializedSinkDescriptor.set_sinkformat("CSV_FORMAT");
//        } else if (format == "NES_FORMAT") {
//            serializedSinkDescriptor.set_sinkformat("NES_FORMAT");
//        } else if (format == "TEXT_FORMAT") {
//            serializedSinkDescriptor.set_sinkformat("TEXT_FORMAT");
//        } else {
//            NES_ERROR("serializeSinkDescriptor: format not supported");
//        }
//        sinkDetails->mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
//    } else {
//        NES_ERROR("OperatorSerializationUtil: Unknown Sink Descriptor Type - " << sinkDescriptor->toString());
//        throw std::invalid_argument("Unknown Sink Descriptor Type");
//    }
//    return sinkDetails;
//}

}// namespace NES