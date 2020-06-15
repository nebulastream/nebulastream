
#include <API/Schema.hpp>
#include <GRPC/Serialization/DataTypeSerializationUtil.hpp>
#include <GRPC/Serialization/ExpressionSerializationUtil.hpp>
#include <GRPC/Serialization/OperatorSerializationUtil.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/CsvSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <SerializableOperator.pb.h>

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
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        // serialize map operator
        NES_TRACE("OperatorSerializationUtil:: serialize to MapLogicalOperatorNode");
        auto mapDetails = SerializableOperator_MapDetails();
        auto mapOperator = operatorNode->as<MapLogicalOperatorNode>();
        // serialize map expression
        ExpressionSerializationUtil::serializeExpression(mapOperator->getMapExpression(), mapDetails.mutable_expression());
        serializedOperator->mutable_details()->PackFrom(mapDetails);
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
        operatorNode = createSourceLogicalOperatorNode(sourceDescriptor);
    } else if (details.Is<SerializableOperator_SinkDetails>()) {
        // de-serialize sink operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to SinkLogicalOperator");
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails();
        details.UnpackTo(&serializedSinkDescriptor);
        // de-serialize sink descriptor
        auto sinkDescriptor = deserializeSinkDescriptor(&serializedSinkDescriptor);
        operatorNode = createSinkLogicalOperatorNode(sinkDescriptor);
    } else if (details.Is<SerializableOperator_FilterDetails>()) {
        // de-serialize filter operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to FilterLogicalOperator");
        auto serializedFilterOperator = SerializableOperator_FilterDetails();
        details.UnpackTo(&serializedFilterOperator);
        // de-serialize filter expression
        auto filterExpression = ExpressionSerializationUtil::deserializeExpression(serializedFilterOperator.mutable_predicate());
        operatorNode = createFilterLogicalOperatorNode(filterExpression);
    } else if (details.Is<SerializableOperator_MapDetails>()) {
        // de-serialize map operator
        NES_TRACE("OperatorSerializationUtil:: de-serialize to MapLogicalOperator");
        auto serializedMapOperator = SerializableOperator_MapDetails();
        details.UnpackTo(&serializedMapOperator);
        // de-serialize map expression
        auto fieldAssignmentExpression = ExpressionSerializationUtil::deserializeExpression(serializedMapOperator.mutable_expression());
        operatorNode = createMapLogicalOperatorNode(fieldAssignmentExpression->as<FieldAssignmentExpressionNode>());
    } else {
        NES_FATAL_ERROR("OperatorSerializationUtil: could not de-serialize this serialized operator: " << details.type_url());
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
    NES_DEBUG("OperatorSerializationUtil:: de-serialize " << serializedOperator->DebugString() << " to " << operatorNode->toString());
    return operatorNode;
}

SerializableOperator_SourceDetails OperatorSerializationUtil::serializeSourceOperator(SourceLogicalOperatorNodePtr sourceOperator) {
    auto sourceDetails = SerializableOperator_SourceDetails();
    auto sourceDescriptor = sourceOperator->getSourceDescriptor();
    serializeSourceSourceDescriptor(sourceDescriptor, &sourceDetails);
    return sourceDetails;
}

OperatorNodePtr OperatorSerializationUtil::deserializeSourceOperator(SerializableOperator_SourceDetails* serializedSourceDetails) {
    auto sourceDescriptor = deserializeSourceDescriptor(serializedSourceDetails);
    return createSourceLogicalOperatorNode(sourceDescriptor);
}

SerializableOperator_SinkDetails OperatorSerializationUtil::serializeSinkOperator(SinkLogicalOperatorNodePtr sinkOperator) {
    auto sinkDetails = SerializableOperator_SinkDetails();
    auto sinkDescriptor = sinkOperator->getSinkDescriptor();
    serializeSinkDescriptor(sinkDescriptor, &sinkDetails);
    return sinkDetails;
}

OperatorNodePtr OperatorSerializationUtil::deserializeSinkOperator(SerializableOperator_SinkDetails* sinkDetails) {
    auto sinkDescriptor = deserializeSinkDescriptor(sinkDetails);
    return createSinkLogicalOperatorNode(sinkDescriptor);
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
    } else if (sourceDescriptor->instanceOf<Network::NetworkSourceDescriptor>()) {
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
        sourceDetails->mutable_sourcedescriptor()->PackFrom(logicalStreamSerializedSourceDescriptor);
    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown Source Descriptor Type " << sourceDescriptor->toString());
        throw std::invalid_argument("Unknown Source Descriptor Type");
    }
    return sourceDetails;
}

SourceDescriptorPtr OperatorSerializationUtil::deserializeSourceDescriptor(SerializableOperator_SourceDetails* serializedSourceDetails) {
    // de-serialize source details and all its properties to a SourceDescriptor
    NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor " << serializedSourceDetails->DebugString());
    const auto& serializedSourceDescriptor = serializedSourceDetails->sourcedescriptor();
    if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableZMQSourceDescriptor>()) {
        // de-serialize zmq source descriptor
        NES_DEBUG("OperatorSerializationUtil:: de-serialized SourceDescriptor as ZmqSourceDescriptor");
        auto zmqSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableZMQSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&zmqSerializedSourceDescriptor);
        // de-serialize source schema
        auto schema = SchemaSerializationUtil::deserializeSchema(zmqSerializedSourceDescriptor.release_sourceschema());
        return ZmqSourceDescriptor::create(schema, zmqSerializedSourceDescriptor.host(), zmqSerializedSourceDescriptor.port());
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableNetworkSourceDescriptor>()) {
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
        return CsvSourceDescriptor::create(schema, csvSerializedSourceDescriptor.filepath(), csvSerializedSourceDescriptor.delimiter(), csvSerializedSourceDescriptor.numbufferstoprocess(), csvSerializedSourceDescriptor.frequency());
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
        return LogicalStreamSourceDescriptor::create(logicalStreamSerializedSourceDescriptor.streamname());
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
        serializedSinkDescriptor.set_host(zmqSinkDescriptor->getHost());
        sinkDetails->mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
    } else if (sinkDescriptor->instanceOf<Network::NetworkSinkDescriptor>()) {
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
        auto s = std::chrono::duration_cast<std::chrono::seconds> (networkSinkDescriptor->getWaitTime());
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
        sinkDetails->mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
    } else if (sinkDescriptor->instanceOf<CsvSinkDescriptor>()) {
        // serialize file sink descriptor. The file sink has different types which have to be set correctly
        NES_TRACE("OperatorSerializationUtil:: serialized SinkDescriptor as SerializableOperator_SinkDetails_SerializableCsvSinkDescriptor");
        auto fileSinkDescriptor = sinkDescriptor->as<CsvSinkDescriptor>();
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableCsvSinkDescriptor();
        auto outputMode = fileSinkDescriptor->getFileOutputMode() == CsvSinkDescriptor::APPEND
            ? SerializableOperator_SinkDetails_SerializableCsvSinkDescriptor_FileOutputMode_FILE_APPEND
            : SerializableOperator_SinkDetails_SerializableCsvSinkDescriptor_FileOutputMode_FILE_OVERWRITE;
        serializedSinkDescriptor.set_outputmode(outputMode);
        serializedSinkDescriptor.set_filepath(fileSinkDescriptor->getFileName());
        sinkDetails->mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown Sink Descriptor Type - " << sinkDescriptor->toString());
        throw std::invalid_argument("Unknown Sink Descriptor Type");
    }
    return sinkDetails;
}
SinkDescriptorPtr OperatorSerializationUtil::deserializeSinkDescriptor(SerializableOperator_SinkDetails* sinkDetails) {
    // de-serialize a sink descriptor and all its properties to a SinkDescriptor.
    NES_DEBUG("OperatorSerializationUtil:: de-serialized SinkDescriptor " << sinkDetails->DebugString());
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
        return ZmqSinkDescriptor::create(serializedSinkDescriptor.host(), serializedSinkDescriptor.port());
    } else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableNetworkSinkDescriptor>()) {
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
        return FileSinkDescriptor::create(serializedSinkDescriptor.filepath());
    } else if (deserializedSinkDescriptor.Is<SerializableOperator_SinkDetails_SerializableCsvSinkDescriptor>()) {
        // de-serialize csv sink descriptor
        auto serializedCsvDescriptor = SerializableOperator_SinkDetails_SerializableCsvSinkDescriptor();
        deserializedSinkDescriptor.UnpackTo(&serializedCsvDescriptor);
        NES_TRACE("OperatorSerializationUtil:: de-serialized SinkDescriptor as CSVSinkDescriptor");
        auto outputmode = serializedCsvDescriptor.outputmode() == SerializableOperator_SinkDetails_SerializableCsvSinkDescriptor_FileOutputMode_FILE_APPEND
            ? CsvSinkDescriptor::APPEND
            : CsvSinkDescriptor::OVERWRITE;
        return CsvSinkDescriptor::create(serializedCsvDescriptor.filepath(), outputmode);
    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown sink Descriptor Type " << sinkDetails->DebugString());
        throw std::invalid_argument("Unknown Sink Descriptor Type");
    }
}
}// namespace NES