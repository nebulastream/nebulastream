#include <boost/algorithm/string.hpp>

#include <API/Query.hpp>
#include <API/UserAPIExpression.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Nodes/Operators/LogicalOperators/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/SourceLogicalOperatorNode.hpp>
#include <SourceSink/DataSink.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <cstddef>
#include <iostream>
#include <Nodes/Util/DumpContext.hpp>

namespace NES {

Query::Query(StreamPtr sourceStreamPtr) : sourceStream(sourceStreamPtr), operatorIdCounter(0) {}

Query::Query(const Query& query)
    : sourceStream(query.sourceStream), operatorIdCounter(query.operatorIdCounter), rootNode(query.rootNode) {}

Query& Query::operator=(const Query& query) {
    this->sourceStream = query.sourceStream;
    this->operatorIdCounter = query.operatorIdCounter;
    this->rootNode = query.rootNode;
    return *this;
}

Query Query::from(Stream& stream) {
    Query query(std::make_shared<Stream>(stream));

    // TODO:here we assume that all sources are of the same type
    std::vector<StreamCatalogEntryPtr> catalogEntry = StreamCatalog::instance().getPhysicalStreams(stream.getName());

    OperatorNodePtr op;

    if (catalogEntry.size() == 0) {
        NES_WARNING("InputQuery::from stream does not exists this should only be used by tests " << stream.getName())
        op = createSourceLogicalOperatorNode(createDefaultDataSourceWithSchemaForOneBuffer(stream.getSchema()));
    } else {
        std::string name = catalogEntry[0]->getPhysicalName();
        std::string type = catalogEntry[0]->getSourceType();
        std::string conf = catalogEntry[0]->getSourceConfig();
        size_t frequency = catalogEntry[0]->getSourceFrequency();
        size_t numBuffers = catalogEntry[0]->getNumberOfBuffersToProduce();

        NES_DEBUG("InputQuery::from logical stream name=" << stream.getName() << " pyhName=" << name
                                                          << " srcType=" << type << " srcConf=" << conf
                                                          << " frequency=" << frequency << " numBuffers=" << numBuffers)

        if (type == "DefaultSource") {
            if (numBuffers == 1) {
                NES_DEBUG("InputQuery::from create default source for one buffer")
                op = createSourceLogicalOperatorNode(createDefaultDataSourceWithSchemaForOneBuffer(stream.getSchema()));
            } else {
                NES_DEBUG("InputQuery::from create default source for " << numBuffers << " buffers")
                op = createSourceLogicalOperatorNode(
                    createDefaultDataSourceWithSchemaForVarBuffers(stream.getSchema(), numBuffers, frequency));
            }
        } else if (type == "CSVSource") {
            NES_DEBUG("InputQuery::from create CSV source for " << conf << " buffers")
            op = createSourceLogicalOperatorNode(createCSVFileSource(stream.getSchema(), /**fileName*/
                                                                     conf,               /**delimiter**/
                                                                     ",", numBuffers, frequency));
        } else if (type == "SenseSource") {
            NES_DEBUG("InputQuery::from create Sense source for udfs " << conf)
            op = createSourceLogicalOperatorNode(createSenseSource(stream.getSchema(), /**udfs*/ conf));
        } else {
            NES_ERROR("InputQuery::from source type " << type << " not supported")
            NES_FATAL_ERROR("type not supported")
        }
    }
    int operatorId = query.getNextOperatorId();
    op->setId(operatorId);
    query.rootNode = op;
    return query;
}

/*
 * Relational Operators
 */

Query& Query::select(const Field& field) {NES_NOT_IMPLEMENTED }

Query& Query::select(const Field& field1, const Field& field2) {NES_NOT_IMPLEMENTED }

Query& Query::filter(const ExpressionNodePtr expressionNodePtr) {
    OperatorNodePtr op = createFilterLogicalOperatorNode(expressionNodePtr);
    assignOperatorIdAndSwitchTheRoot(op);
    return *this;
}

Query& Query::map(const AttributeField& field, const Predicate predicate) {NES_NOT_IMPLEMENTED }

Query& Query::combine(const Query& subQuery) {NES_NOT_IMPLEMENTED }

Query& Query::join(const Query& subQuery, const JoinPredicatePtr joinPred) {NES_NOT_IMPLEMENTED }

Query& Query::windowByKey() {NES_NOT_IMPLEMENTED }

Query& Query::window() {NES_NOT_IMPLEMENTED }

Query& Query::to(const std::string& name) {NES_NOT_IMPLEMENTED }

// output operators
Query& Query::writeToFile(const std::string& fileName) {
    OperatorNodePtr op =
        createSinkLogicalOperatorNode(createBinaryFileSinkWithSchema(this->sourceStream->getSchema(), fileName));
    assignOperatorIdAndSwitchTheRoot(op);
    return *this;
}

// output operators
Query& Query::writeToCSVFile(const std::string& fileName) {

    OperatorNodePtr op =
        createSinkLogicalOperatorNode(createCSVFileSinkWithSchema(sourceStream->getSchema(), fileName));
    assignOperatorIdAndSwitchTheRoot(op);
    return *this;
}

Query& Query::writeToZmq(const std::string& logicalStreamName, const std::string& host, const uint16_t& port) {

    SchemaPtr schemaPtr = StreamCatalog::instance().getSchemaForLogicalStream(logicalStreamName);
    OperatorNodePtr op = createSinkLogicalOperatorNode(createZmqSink(schemaPtr, host, port));
    assignOperatorIdAndSwitchTheRoot(op);
    return *this;
}

Query& Query::print(std::ostream& out) {

    OperatorNodePtr op = createSinkLogicalOperatorNode(createPrintSinkWithSchema(sourceStream->getSchema(), out));
    assignOperatorIdAndSwitchTheRoot(op);
    return *this;
}

Query& Query::writeToKafka(const std::string& topic, const cppkafka::Configuration& config) {

    OperatorNodePtr op =
        createSinkLogicalOperatorNode(createKafkaSinkWithSchema(sourceStream->getSchema(), topic, config));
    assignOperatorIdAndSwitchTheRoot(op);
    return *this;
}

Query& Query::writeToKafka(const std::string& brokers, const std::string& topic, const size_t kafkaProducerTimeout) {

    OperatorNodePtr op = createSinkLogicalOperatorNode(
        createKafkaSinkWithSchema(sourceStream->getSchema(), brokers, topic, kafkaProducerTimeout));
    assignOperatorIdAndSwitchTheRoot(op);
    return *this;
}

const StreamPtr Query::getSourceStream() const { return sourceStream; }

vector<SourceLogicalOperatorNodePtr> Query::getSourceOperators() {
    return rootNode->getNodesByType<SourceLogicalOperatorNode>();
}

std::vector<SinkLogicalOperatorNodePtr> Query::getSinkOperators() {
    return rootNode->getNodesByType<SinkLogicalOperatorNode>();
}

std::string Query::toString() {
    stringstream ss;
    DumpContextPtr dumpContext;
    dumpContext->dump(rootNode, ss);
    return ss.str();
}

size_t Query::getNextOperatorId() {
    operatorIdCounter++;
    return operatorIdCounter;
}

void Query::assignOperatorIdAndSwitchTheRoot(OperatorNodePtr op) {
    int operatorId = getNextOperatorId();
    op->setId(operatorId);
    rootNode->addParent(op);
    rootNode = op;
}

} // namespace NES
