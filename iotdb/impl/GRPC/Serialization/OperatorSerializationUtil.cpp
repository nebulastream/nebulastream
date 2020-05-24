
#include <API/Schema.hpp>
#include <GRPC/Serialization/DataTypeSerializationUtil.hpp>
#include <GRPC/Serialization/ExpressionSerializationUtil.hpp>
#include <GRPC/Serialization/OperatorSerializationUtil.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/KafkaSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <SerializableOperator.pb.h>

namespace NES {

SerializableOperatorPtr OperatorSerializationUtil::serialize(QueryPlanPtr plan) {
    auto ope = std::shared_ptr<SerializableOperator>();
    auto rootOperator = plan->getRootOperator();
    serializeOperator(rootOperator, ope.get());
    return ope;
}

SerializableOperator* OperatorSerializationUtil::serializeOperator(NodePtr node, SerializableOperator* serializedOperator) {
    auto operatorNode = node->as<OperatorNode>();
    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        auto sourceDetails = serializeSourceOperator(operatorNode->as<SourceLogicalOperatorNode>());
        serializedOperator->mutable_details()->PackFrom(sourceDetails);
    } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        auto sinkDetails = serializeSinkOperator(operatorNode->as<SinkLogicalOperatorNode>());
        serializedOperator->mutable_details()->PackFrom(sinkDetails);
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        auto filterDetails = SerializableOperator_FilterDetails();
        auto filterOperator = operatorNode->as<FilterLogicalOperatorNode>();
        ExpressionSerializationUtil::serializeExpression(filterOperator->getPredicate(), filterDetails.mutable_predicate());
        serializedOperator->mutable_details()->PackFrom(filterDetails);
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        auto mapDetails = SerializableOperator_MapDetails();
        auto mapOperator = operatorNode->as<MapLogicalOperatorNode>();
        ExpressionSerializationUtil::serializeExpression(mapOperator->getMapExpression(), mapDetails.mutable_expression());
        serializedOperator->mutable_details()->PackFrom(mapDetails);
    } else {
        NES_FATAL_ERROR("OperatorSerializationUtil: we could not serialize this operator: " << node->toString());
    }
    // serialize input schema
    SchemaSerializationUtil::serializeSchema(operatorNode->getInputSchema(), serializedOperator->mutable_inputschema());
    // serialize output schema
    SchemaSerializationUtil::serializeSchema(operatorNode->getOutputSchema(), serializedOperator->mutable_outputschema());

    // serialize operator id
    serializedOperator->set_operatorid(operatorNode->getId());

    // append children if the node has any
    for (const auto& child : node->getChildren()) {
        auto serializedChild = serializedOperator->add_children();
        serializeOperator(child, serializedChild);
    }
    return serializedOperator;
}

LogicalOperatorNodePtr OperatorSerializationUtil::deserializeOperator(SerializableOperator* serializableOperator) {
    auto details = serializableOperator->details();
    LogicalOperatorNodePtr operatorNode;
    if (details.Is<SerializableOperator_SourceDetails>()) {
        // source operator
        auto serializedSourceDescriptor = SerializableOperator_SourceDetails();
        details.UnpackTo(&serializedSourceDescriptor);
        auto sourceDescriptor = deserializeSourceDescriptor(&serializedSourceDescriptor);
        operatorNode = createSourceLogicalOperatorNode(sourceDescriptor);
    } else if (details.Is<SerializableOperator_SinkDetails>()) {
        // sink operator
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails();
        details.UnpackTo(&serializedSinkDescriptor);
        auto sinkDescriptor = deserializeSinkDescriptor(&serializedSinkDescriptor);
        operatorNode = createSinkLogicalOperatorNode(sinkDescriptor);
    } else if (details.Is<SerializableOperator_FilterDetails>()) {
        auto serializedFilterOperator = SerializableOperator_FilterDetails();
        details.UnpackTo(&serializedFilterOperator);
        auto filterExpression = ExpressionSerializationUtil::deserializeExpression(serializedFilterOperator.mutable_predicate());
        operatorNode = createFilterLogicalOperatorNode(filterExpression);
    } else if (details.Is<SerializableOperator_MapDetails>()) {
        auto serializedMapOperator = SerializableOperator_MapDetails();
        details.UnpackTo(&serializedMapOperator);
        auto mapExpression = ExpressionSerializationUtil::deserializeExpression(serializedMapOperator.mutable_expression());
        auto type = DataTypeSerializationUtil::deserializeDataType(serializedMapOperator.mutable_field()->mutable_type());
        auto fileAccessNode = FieldAccessExpressionNode::create(type, serializedMapOperator.mutable_field()->name());
        auto fieldAssignmentNode = FieldAssignmentExpressionNode::create(fileAccessNode->as<FieldAccessExpressionNode>(), mapExpression);
        operatorNode = createMapLogicalOperatorNode(fieldAssignmentNode);
    }

    operatorNode->setOutputSchema(SchemaSerializationUtil::deserializeSchema(serializableOperator->mutable_outputschema()));
    operatorNode->setInputSchema(SchemaSerializationUtil::deserializeSchema(serializableOperator->mutable_inputschema()));
    operatorNode->setId(serializableOperator->operatorid());
    for (auto child : serializableOperator->children()) {
        operatorNode->addChild(deserializeOperator(&child));
    }
    return operatorNode;
}

SerializableOperator_SourceDetails OperatorSerializationUtil::serializeSourceOperator(SourceLogicalOperatorNodePtr sourceOperator) {
    auto sourceDetails = SerializableOperator_SourceDetails();
    auto sourceDescriptor = sourceOperator->getSourceDescriptor();
    serializeSourceSourceDescriptor(sourceDescriptor, &sourceDetails);
    return sourceDetails;
}

LogicalOperatorNodePtr OperatorSerializationUtil::deserializeSourceOperator(SerializableOperator_SourceDetails* serializedSourceDetails) {
    auto sourceDescriptor = deserializeSourceDescriptor(serializedSourceDetails);
    return createSourceLogicalOperatorNode(sourceDescriptor);
}

SerializableOperator_SinkDetails OperatorSerializationUtil::serializeSinkOperator(SinkLogicalOperatorNodePtr sinkOperator) {
    auto sinkDetails = SerializableOperator_SinkDetails();
    auto sinkDescriptor = sinkOperator->getSinkDescriptor();
    serializeSinkDescriptor(sinkDescriptor, &sinkDetails);
    return sinkDetails;
}

LogicalOperatorNodePtr OperatorSerializationUtil::deserializeSinkOperator(SerializableOperator_SinkDetails* sinkDetails) {
    auto sinkDescriptor = deserializeSinkDescriptor(sinkDetails);
    return createSinkLogicalOperatorNode(sinkDescriptor);
}

SerializableOperator_SourceDetails* OperatorSerializationUtil::serializeSourceSourceDescriptor(SourceDescriptorPtr sourceDescriptor, SerializableOperator_SourceDetails* sourceDetails) {

    if (sourceDescriptor->instanceOf<ZmqSourceDescriptor>()) {
        auto zmqSourceDescriptor = sourceDescriptor->as<ZmqSourceDescriptor>();
        auto zmqSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableZMQSourceDescriptor();
        zmqSerializedSourceDescriptor.set_host(zmqSourceDescriptor->getHost());
        zmqSerializedSourceDescriptor.set_port(zmqSourceDescriptor->getPort());
        SchemaSerializationUtil::serializeSchema(zmqSourceDescriptor->getSchema(), zmqSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(zmqSerializedSourceDescriptor);
    } else if (sourceDescriptor->instanceOf<DefaultSourceDescriptor>()) {
        auto defaultSourceDescriptor = sourceDescriptor->as<DefaultSourceDescriptor>();
        auto defaultSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableDefaultSourceDescriptor();
        defaultSerializedSourceDescriptor.set_frequency(defaultSourceDescriptor->getFrequency());
        defaultSerializedSourceDescriptor.set_numbufferstoprocess(defaultSourceDescriptor->getNumbersOfBufferToProduce());
        SchemaSerializationUtil::serializeSchema(defaultSourceDescriptor->getSchema(), defaultSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(defaultSerializedSourceDescriptor);
    } else if (sourceDescriptor->instanceOf<BinarySourceDescriptor>()) {
        auto binarySourceDescriptor = sourceDescriptor->as<BinarySourceDescriptor>();
        auto binarySerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableBinarySourceDescriptor();
        binarySerializedSourceDescriptor.set_filepath(binarySourceDescriptor->getFilePath());
        SchemaSerializationUtil::serializeSchema(binarySourceDescriptor->getSchema(), binarySerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(binarySerializedSourceDescriptor);
    } else if (sourceDescriptor->instanceOf<CsvSourceDescriptor>()) {
        auto csvSourceDescriptor = sourceDescriptor->as<CsvSourceDescriptor>();
        auto csvSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableCsvSourceDescriptor();
        csvSerializedSourceDescriptor.set_filepath(csvSourceDescriptor->getFilePath());
        csvSerializedSourceDescriptor.set_frequency(csvSourceDescriptor->getFrequency());
        csvSerializedSourceDescriptor.set_delimiter(csvSourceDescriptor->getDelimiter());
        csvSerializedSourceDescriptor.set_numbufferstoprocess(csvSourceDescriptor->getNumBuffersToProcess());
        SchemaSerializationUtil::serializeSchema(csvSourceDescriptor->getSchema(), csvSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(csvSerializedSourceDescriptor);
    } else if (sourceDescriptor->instanceOf<SenseSourceDescriptor>()) {
        auto senseSourceDescriptor = sourceDescriptor->as<SenseSourceDescriptor>();
        auto senseSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableSenseSourceDescriptor();
        senseSerializedSourceDescriptor.set_udfs(senseSourceDescriptor->getUdfs());
        SchemaSerializationUtil::serializeSchema(senseSourceDescriptor->getSchema(), senseSerializedSourceDescriptor.mutable_sourceschema());
        sourceDetails->mutable_sourcedescriptor()->PackFrom(senseSerializedSourceDescriptor);
    } else if (sourceDescriptor->instanceOf<LogicalStreamSourceDescriptor>()) {
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
    const auto& serializedSourceDescriptor = serializedSourceDetails->sourcedescriptor();
    if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableZMQSourceDescriptor>()) {
        auto zmqSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableZMQSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&zmqSerializedSourceDescriptor);
        auto schema = SchemaSerializationUtil::deserializeSchema(zmqSerializedSourceDescriptor.release_sourceschema());
        return ZmqSourceDescriptor::create(schema, zmqSerializedSourceDescriptor.host(), zmqSerializedSourceDescriptor.port());
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableDefaultSourceDescriptor>()) {
        auto defaultSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableDefaultSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&defaultSerializedSourceDescriptor);
        auto schema = SchemaSerializationUtil::deserializeSchema(defaultSerializedSourceDescriptor.release_sourceschema());
        return DefaultSourceDescriptor::create(schema, defaultSerializedSourceDescriptor.numbufferstoprocess(), defaultSerializedSourceDescriptor.frequency());
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableBinarySourceDescriptor>()) {
        auto binarySerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableBinarySourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&binarySerializedSourceDescriptor);
        auto schema = SchemaSerializationUtil::deserializeSchema(binarySerializedSourceDescriptor.release_sourceschema());
        return BinarySourceDescriptor::create(schema, binarySerializedSourceDescriptor.filepath());
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableCsvSourceDescriptor>()) {
        auto csvSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableCsvSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&csvSerializedSourceDescriptor);
        auto schema = SchemaSerializationUtil::deserializeSchema(csvSerializedSourceDescriptor.release_sourceschema());
        return CsvSourceDescriptor::create(schema, csvSerializedSourceDescriptor.filepath(), csvSerializedSourceDescriptor.delimiter(), csvSerializedSourceDescriptor.numbufferstoprocess(), csvSerializedSourceDescriptor.frequency());
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableSenseSourceDescriptor>()) {
        auto senseSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableSenseSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&senseSerializedSourceDescriptor);
        auto schema = SchemaSerializationUtil::deserializeSchema(senseSerializedSourceDescriptor.release_sourceschema());
        return SenseSourceDescriptor::create(schema, senseSerializedSourceDescriptor.udfs());
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SourceDetails_SerializableLogicalStreamSourceDescriptor>()) {
        auto logicalStreamSerializedSourceDescriptor = SerializableOperator_SourceDetails_SerializableLogicalStreamSourceDescriptor();
        serializedSourceDescriptor.UnpackTo(&logicalStreamSerializedSourceDescriptor);
        return LogicalStreamSourceDescriptor::create(logicalStreamSerializedSourceDescriptor.streamname());
    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown Source Descriptor Type " << serializedSourceDescriptor.type_url());
        throw std::invalid_argument("Unknown Source Descriptor Type");
    }
}
SerializableOperator_SinkDetails* OperatorSerializationUtil::serializeSinkDescriptor(SinkDescriptorPtr sinkDescriptor, SerializableOperator_SinkDetails* sinkDetails) {
    if (sinkDescriptor->instanceOf<PrintSinkDescriptor>()) {
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializablePrintSinkDescriptor();
        sinkDetails->mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
    } else if (sinkDescriptor->instanceOf<ZmqSinkDescriptor>()) {
        auto zmqSinkDescriptor = sinkDescriptor->as<ZmqSinkDescriptor>();
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableZMQSinkDescriptor();
        serializedSinkDescriptor.set_port(zmqSinkDescriptor->getPort());
        serializedSinkDescriptor.set_host(zmqSinkDescriptor->getHost());
        sinkDetails->mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
    } else if (sinkDescriptor->instanceOf<FileSinkDescriptor>()) {
        auto fileSinkDescriptor = sinkDescriptor->as<FileSinkDescriptor>();
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableFileSinkDescriptor();

        serializedSinkDescriptor.set_filepath(fileSinkDescriptor->getFileName());

        auto fileOutputType = fileSinkDescriptor->getFileOutputType() == BINARY_TYPE
            ? SerializableOperator_SinkDetails_SerializableFileSinkDescriptor_FileOutputType_BINARY_TYPE
            : SerializableOperator_SinkDetails_SerializableFileSinkDescriptor_FileOutputType_CSV_TYPE;
        serializedSinkDescriptor.set_fileoutputtype(fileOutputType);

        auto fileOutputMode = fileSinkDescriptor->getFileOutputMode() == FILE_OVERWRITE
            ? SerializableOperator_SinkDetails_SerializableFileSinkDescriptor_FileOutputMode_FILE_OVERWRITE
            : SerializableOperator_SinkDetails_SerializableFileSinkDescriptor_FileOutputMode_FILE_APPEND;
        serializedSinkDescriptor.set_fileoutputmode(fileOutputMode);

        sinkDetails->mutable_sinkdescriptor()->PackFrom(serializedSinkDescriptor);
    } else {
        NES_ERROR("OperatorSerializationUtil: Unknown Sink Descriptor Type - " << sinkDescriptor->toString());
        throw std::invalid_argument("Unknown Sink Descriptor Type");
    }
    return sinkDetails;
}
SinkDescriptorPtr OperatorSerializationUtil::deserializeSinkDescriptor(SerializableOperator_SinkDetails* sinkDetails) {
    const auto& serializedSourceDescriptor = sinkDetails->sinkdescriptor();
    if (serializedSourceDescriptor.Is<SerializableOperator_SinkDetails_SerializablePrintSinkDescriptor>()) {
        return PrintSinkDescriptor::create();
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SinkDetails_SerializableZMQSinkDescriptor>()) {
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableZMQSinkDescriptor();
        serializedSourceDescriptor.UnpackTo(&serializedSinkDescriptor);
        return ZmqSinkDescriptor::create(serializedSinkDescriptor.host(), serializedSinkDescriptor.port());
    } else if (serializedSourceDescriptor.Is<SerializableOperator_SinkDetails_SerializableFileSinkDescriptor>()) {
        auto serializedSinkDescriptor = SerializableOperator_SinkDetails_SerializableFileSinkDescriptor();
        serializedSourceDescriptor.UnpackTo(&serializedSinkDescriptor);
        auto fileOutputType = serializedSinkDescriptor.fileoutputtype() == SerializableOperator_SinkDetails_SerializableFileSinkDescriptor_FileOutputType_BINARY_TYPE
            ? BINARY_TYPE
            : CSV_TYPE;

        auto fileOutputMode = serializedSinkDescriptor.fileoutputmode() == SerializableOperator_SinkDetails_SerializableFileSinkDescriptor_FileOutputMode_FILE_APPEND
            ? FILE_APPEND
            : FILE_OVERWRITE;

        return FileSinkDescriptor::create(serializedSinkDescriptor.filepath(), fileOutputMode, fileOutputType);
    }
    return nullptr;
}
}// namespace NES