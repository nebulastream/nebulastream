#include <boost/algorithm/string.hpp>

#include <cstddef>
#include <iostream>
#include <API/Query.hpp>
#include <SourceSink/DataSink.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Util/Logger.hpp>
#include <Nodes/Operators/LogicalOperators/SourceLogicalOperatorNode.hpp>
#include <API/UserAPIExpression.hpp>

namespace NES {

Query::Query(StreamPtr source_stream) : sourceStream(source_stream) {}

/* TODO: perform deep copy of operator graph */
Query::Query(const Query& query) : sourceStream(query.sourceStream) {}

Query& Query::operator=(const Query& query) {
    if (&query != this) {
        this->sourceStream = query.sourceStream;
    }
    return *this;
}

Query Query::from(Stream& stream) {
    Query q(std::make_shared<Stream>(stream));

    //TODO:here we assume that all sources are of the same type
    std::vector<StreamCatalogEntryPtr> catalogEntry = StreamCatalog::instance()
        .getPhysicalStreams(stream.getName());

    OperatorNodePtr op;

    if (catalogEntry.size() == 0) {
        NES_WARNING(
            "InputQuery::from stream does not exists this should only be used by tests " << stream.getName())

        op = createSourceLogicalOperatorNode(createDefaultDataSourceWithSchemaForOneBuffer(stream.getSchema()));

    } else {

        std::string name = catalogEntry[0]->getPhysicalName();
        std::string type = catalogEntry[0]->getSourceType();
        std::string conf = catalogEntry[0]->getSourceConfig();
        size_t frequency = catalogEntry[0]->getSourceFrequency();
        size_t numBuffers = catalogEntry[0]->getNumberOfBuffersToProduce();

        NES_DEBUG(
            "InputQuery::from logical stream name=" << stream.getName() << " pyhName=" << name << " srcType=" << type
                                                    << " srcConf=" << conf << " frequency=" << frequency
                                                    << " numBuffers=" << numBuffers)

        if (type == "DefaultSource") {
            if (numBuffers == 1) {
                NES_DEBUG("InputQuery::from create default source for one buffer")
                op = createSourceLogicalOperatorNode(createDefaultDataSourceWithSchemaForOneBuffer(stream.getSchema()));
            } else {
                NES_DEBUG(
                    "InputQuery::from create default source for " << numBuffers << " buffers")
                op = createSourceLogicalOperatorNode(createDefaultDataSourceWithSchemaForVarBuffers(stream.getSchema(),
                                                                                                    numBuffers,
                                                                                                    frequency));
            }
        } else if (type == "CSVSource") {
            NES_DEBUG("InputQuery::from create CSV source for " << conf << " buffers")
            op = createSourceLogicalOperatorNode(createCSVFileSource(stream.getSchema(), /**fileName*/
                                                                     conf, /**delimiter**/
                                                                     ",",
                                                                     numBuffers,
                                                                     frequency));
        } else if (type == "SenseSource") {
            NES_DEBUG("InputQuery::from create Sense source for udfs " << conf)
            op = createSourceLogicalOperatorNode(createSenseSource(stream.getSchema(), /**udfs*/conf));
        } else {
            NES_DEBUG("InputQuery::from source type " << type << " not supported")
            NES_FATAL_ERROR("type not supported")
        }
    }
    int operatorId = q.getNextOperatorId();
    op->setId(operatorId);
    q.root = op;
    return q;
}

/*
 * Relational Operators
 */

Query& Query::select(const Field& field) {
    NES_NOT_IMPLEMENTED
}

Query& Query::select(const Field& field1, const Field& field2) {
    NES_NOT_IMPLEMENTED
}

Query& Query::filter(const ExpressionNodePtr expressionNodePtr) {

    OperatorNodePtr op = createFilterLogicalOperatorNode(expressionNodePtr);
    int operatorId = this->getNextOperatorId();
    op->setId(operatorId);
    root->addParent(op);
    root = op;
    return *this;
}

Query& Query::map(const AttributeField& field,
                  const Predicate predicate) {
    NES_NOT_IMPLEMENTED
}

Query& Query::combine(const Query& sub_query) {
    NES_NOT_IMPLEMENTED
}

Query& Query::join(const Query& sub_query,
                   const JoinPredicatePtr joinPred) {
    NES_NOT_IMPLEMENTED
}

Query& Query::windowByKey() {
    NES_NOT_IMPLEMENTED
}

Query& Query::window() {
    NES_NOT_IMPLEMENTED
}

// output operators
Query& Query::writeToFile(const std::string& file_name) {

    OperatorNodePtr op = createSinkLogicalOperatorNode(createBinaryFileSinkWithSchema(this->sourceStream->getSchema(),
                                                                                      file_name));
    int operatorId = this->getNextOperatorId();
    op->setId(operatorId);
    root->addParent(op);
    root = op;
    return *this;
}

// output operators
Query& Query::writeToCSVFile(const std::string& file_name) {

    OperatorNodePtr op = createSinkLogicalOperatorNode(createCSVFileSinkWithSchema(this->sourceStream->getSchema(),
                                                                                   file_name));
    int operatorId = this->getNextOperatorId();
    op->setId(operatorId);
    root->addParent(op);
    root = op;
    return *this;
}

Query& Query::writeToZmq(const std::string& logicalStreamName,
                         const std::string& host,
                         const uint16_t& port) {

    SchemaPtr schemaPtr = StreamCatalog::instance().getSchemaForLogicalStream(logicalStreamName);
    OperatorNodePtr op = createSinkLogicalOperatorNode(createZmqSink(schemaPtr, host, port));
    int operatorId = this->getNextOperatorId();
    op->setId(operatorId);
    root->addParent(op);
    root = op;
    return *this;
}

Query& Query::print(std::ostream& out) {

    OperatorNodePtr op = createSinkLogicalOperatorNode(createPrintSinkWithSchema(this->sourceStream->getSchema(), out));
    int operatorId = this->getNextOperatorId();
    op->setId(operatorId);
    root->addParent(op);
    root = op;
    return *this;
}

Query& Query::writeToKafka(const std::string& topic, const cppkafka::Configuration& config) {

    OperatorNodePtr op = createSinkLogicalOperatorNode(createKafkaSinkWithSchema(this->sourceStream->getSchema(), topic,
                                                                                 config));
    int operatorId = this->getNextOperatorId();
    op->setId(operatorId);
    root->addParent(op);
    root = op;
    return *this;
}

Query& Query::writeToKafka(const std::string& brokers, const std::string& topic, const size_t kafkaProducerTimeout) {

    OperatorNodePtr op = createSinkLogicalOperatorNode(createKafkaSinkWithSchema(this->sourceStream->getSchema(),
                                                                                 brokers, topic, kafkaProducerTimeout));
    int operatorId = this->getNextOperatorId();
    op->setId(operatorId);
    root->addParent(op);
    root = op;
    return *this;
}

const StreamPtr Query::getSourceStream() const {
    return sourceStream;
}

vector<SourceLogicalOperatorNodePtr> Query::getSourceOperators() {
    return root->getNodesByType<SourceLogicalOperatorNode>();
}

std::vector<SinkLogicalOperatorNodePtr> Query::getSinkOperators() {
    return root->getNodesByType<SinkLogicalOperatorNode>();
}

}  // namespace NES
