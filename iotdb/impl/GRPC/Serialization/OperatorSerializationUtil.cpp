
#include <API/Schema.hpp>
#include <GRPC/Serialization/OperatorSerializationUtil.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
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
        auto sinkDetails = SerializableOperator_SinkDetails();
        serializedOperator->mutable_details()->PackFrom(sinkDetails);
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        auto filterDetails = SerializableOperator_FilterDetails();
        serializedOperator->mutable_details()->PackFrom(filterDetails);
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        auto mapDetails = SerializableOperator_MapDetails();
        serializedOperator->mutable_details()->PackFrom(mapDetails);
    } else {
        NES_FATAL_ERROR("OperatorSerializationUtil: we could not serialize this operator: " << node->toString());
    }
    // serialize input schema
    SchemaSerializationUtil::serializeSchema(operatorNode->getInputSchema(), serializedOperator->mutable_inputschema());
    // serialize output schema
    SchemaSerializationUtil::serializeSchema(operatorNode->getOutputSchema(), serializedOperator->mutable_outputschema());

    // append children if the node has any
    for (const auto& child : node->getChildren()) {
        auto serializedChild = serializedOperator->add_children();
        serializeOperator(child, serializedChild);
    }
    return serializedOperator;
}

SerializableOperator_SourceDetails OperatorSerializationUtil::serializeSourceOperator(SourceLogicalOperatorNodePtr sourceOperator) {
    auto sourceDetails = SerializableOperator_SourceDetails();
    auto sourceDescriptor = sourceOperator->getSourceDescriptor();
    serializeSourceSourceDescriptor(sourceDescriptor, &sourceDetails);
    return sourceDetails;
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
}// namespace NES