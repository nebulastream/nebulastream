#include <boost/algorithm/string.hpp>

#include <cstddef>
#include <iostream>
#include <API/Query.hpp>
#include <Operators/Operator.hpp>
#include <QueryCompiler/Compiler/Compiler.hpp>
#include <API/UserAPIExpression.hpp>
#include <SourceSink/DataSink.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <API/Window/WindowDefinition.hpp>
#include <Util/Logger.hpp>
#include <Nodes/Operators/LogicalOperators/SourceLogicalOperatorNode.hpp>

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
            op = createSourceLogicalOperatorNode(createCSVFileSource(stream.getSchema(), /**fileName*/ conf, /**delimiter**/ ",",
                numBuffers, frequency));
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

Query& Query::filter(const UserAPIExpression& predicate) {
    PredicatePtr pred = createPredicate(predicate);
    OperatorPtr op = createFilterOperator(pred);
    int operatorId = this->getNextOperatorId();
    op->setOperatorId(operatorId);
    return *this;
}

Query& Query::map(const AttributeField& field,
                  const Predicate predicate) {
    PredicatePtr pred = createPredicate(predicate);
    AttributeFieldPtr attr = field.copy();
    OperatorPtr op = createMapOperator(attr, pred);
    int operatorId = this->getNextOperatorId();
    op->setOperatorId(operatorId);
    return *this;
}

Query& Query::combine(const Query& sub_query) {
    NES_NOT_IMPLEMENTED
}

Query& Query::join(const Query& sub_query,
                   const JoinPredicatePtr joinPred) {
    OperatorPtr op = createJoinOperator(joinPred);
    int operatorId = this->getNextOperatorId();
    op->setOperatorId(operatorId);
    addChild(op, root);
    // addChild(op, copyRecursive(sub_query,sub_query.root));
    addChild(op, sub_query.root);
    root = op;
    return *this;
}

Query& Query::windowByKey(const AttributeFieldPtr onKey,
                          const WindowTypePtr windowType,
                          const WindowAggregationPtr aggregation) {
    auto window_def_ptr = std::make_shared<WindowDefinition>(onKey, aggregation,
                                                             windowType);
    OperatorPtr op = createWindowOperator(window_def_ptr);
    op->setOperatorId(this->getNextOperatorId());
    addChild(op, root);
    root = op;
    // add a window scan operator with the window result schema.
    SchemaPtr schemaPtr = Schema::create()->addField(aggregation->asField());
    OperatorPtr windowScan = createWindowScanOperator(schemaPtr);
    windowScan->setOperatorId(this->getNextOperatorId());
    addChild(windowScan, root);
    root = windowScan;
    return *this;
}

Query& Query::window(const NES::WindowTypePtr windowType,
                     const WindowAggregationPtr aggregation) {
    auto window_def_ptr = std::make_shared<WindowDefinition>(aggregation,
                                                             windowType);
    OperatorPtr op = createWindowOperator(window_def_ptr);
    op->setOperatorId(this->getNextOperatorId());
    addChild(op, root);
    root = op;
    // add a window scan operator with the window result schema.
    SchemaPtr schemaPtr = Schema::create()->addField(aggregation->asField());
    OperatorPtr windowScan = createWindowScanOperator(schemaPtr);
    windowScan->setOperatorId(this->getNextOperatorId());
    addChild(windowScan, root);
    root = windowScan;

    return *this;
}

// output operators
Query& Query::writeToFile(const std::string& file_name) {
    OperatorPtr op = createSinkOperator(
        createBinaryFileSinkWithSchema(this->sourceStream->getSchema(),
                                       file_name));
    int operatorId = this->getNextOperatorId();
    op->setOperatorId(operatorId);
    return *this;
}

// output operators
Query& Query::writeToCSVFile(const std::string& file_name) {
    OperatorPtr op = createSinkOperator(
        createCSVFileSinkWithSchema(this->sourceStream->getSchema(), file_name));
    int operatorId = this->getNextOperatorId();
    op->setOperatorId(operatorId);
    return *this;
}

Query& Query::writeToZmq(const std::string& logicalStreamName,
                         const std::string& host,
                         const uint16_t& port) {
    SchemaPtr ptr = StreamCatalog::instance().getSchemaForLogicalStream(
        logicalStreamName);
    OperatorPtr op = createSinkOperator(createZmqSink(ptr, host, port));
    int operatorId = this->getNextOperatorId();
    op->setOperatorId(operatorId);
    return *this;
}

Query& Query::print(std::ostream& out) {
    OperatorPtr op = createSinkOperator(
        createPrintSinkWithSchema(this->sourceStream->getSchema(), out));
    int operatorId = this->getNextOperatorId();
    op->setOperatorId(operatorId);
    return *this;
}

Query& Query::writeToKafka(const std::string& topic,
                           const cppkafka::Configuration& config) {
    OperatorPtr op = createSinkOperator(
        createKafkaSinkWithSchema(this->sourceStream->getSchema(), topic,
                                  config));
    int operatorId = this->getNextOperatorId();
    op->setOperatorId(operatorId);
    return *this;
}

Query& Query::writeToKafka(const std::string& brokers,
                           const std::string& topic,
                           const size_t kafkaProducerTimeout) {
    OperatorPtr op = createSinkOperator(
        createKafkaSinkWithSchema(this->sourceStream->getSchema(), brokers, topic,
                                  kafkaProducerTimeout));
    int operatorId = this->getNextOperatorId();
    op->setOperatorId(operatorId);
    return *this;
}
const StreamPtr Query::getSourceStream() const {
    return sourceStream;
}
void Query::setSourceStream(const StreamPtr sourceStream) {
    Query::sourceStream = sourceStream;
}

}  // namespace NES
