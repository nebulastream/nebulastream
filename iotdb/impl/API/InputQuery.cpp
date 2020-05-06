#include <boost/algorithm/string.hpp>

#include <API/InputQuery.hpp>
#include <API/UserAPIExpression.hpp>
#include <API/Window/WindowDefinition.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Operators/Operator.hpp>
#include <QueryCompiler/Compiler/Compiler.hpp>
#include <SourceSink/DataSink.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <cstddef>
#include <iostream>

namespace NES {

const OperatorPtr recursiveCopy(OperatorPtr ptr) {
    OperatorPtr operatorPtr = ptr->copy();
    operatorPtr->setParent(ptr->getParent());
    operatorPtr->setOperatorId(ptr->getOperatorId());
    vector<OperatorPtr> destinationChildren = operatorPtr->getChildren();
    std::vector<OperatorPtr> children = ptr->getChildren();

    for (uint32_t i = 0; i < children.size(); i++) {
        const OperatorPtr copiedChild = recursiveCopy(children[i]);
        if (!copiedChild) {
            return nullptr;
        }
        destinationChildren.push_back(copiedChild);
    }

    operatorPtr->setChildren(destinationChildren);
    return operatorPtr;
}

/* some utility functions to encapsulate node handling to be
 * independent of implementation of query graph */
const std::vector<OperatorPtr> getChildNodes(const OperatorPtr op);

void addChild(const OperatorPtr opParent, const OperatorPtr opChild);

static inline void ltrim(std::string& s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](int ch) {
                return !std::isspace(ch);
            }));
}

std::string InputQuery::getUdsf() {
    return udfs;
}

void InputQuery::setUdsf(std::string udsf) {
    this->udfs = udfs;
}

InputQuery::InputQuery(StreamPtr source_stream)
    : sourceStream(source_stream),
      udfs(""),
      root() {
}

/* TODO: perform deep copy of operator graph */
InputQuery::InputQuery(const InputQuery& query)
    : sourceStream(query.sourceStream),
      root(recursiveCopy(query.root)) {
}

InputQuery& InputQuery::operator=(const InputQuery& query) {
    if (&query != this) {
        this->sourceStream = query.sourceStream;
        this->root = recursiveCopy(query.root);
    }
    return *this;
}

InputQuery::~InputQuery() {
}

InputQuery InputQuery::from(Stream& stream) {
    InputQuery q(std::make_shared<Stream>(stream));

    //TODO:here we assume that all sources are of the same type
    std::vector<StreamCatalogEntryPtr> catalogEntry = StreamCatalog::instance()
                                                          .getPhysicalStreams(stream.getName());

    OperatorPtr op;

    BufferManagerPtr bPtr;
    QueryManagerPtr dPtr;
    if (catalogEntry.size() == 0) {
        NES_WARNING(
            "InputQuery::from stream does not exists this should only be used by tests " << stream.getName());
        op = createSourceOperator(
            createDefaultDataSourceWithSchemaForOneBuffer(stream.getSchema(), bPtr, dPtr));
    } else {

        std::string name = catalogEntry[0]->getPhysicalName();
        std::string type = catalogEntry[0]->getSourceType();
        std::string conf = catalogEntry[0]->getSourceConfig();
        size_t frequency = catalogEntry[0]->getSourceFrequency();
        size_t numBuffers = catalogEntry[0]->getNumberOfBuffersToProduce();

        NES_DEBUG(
            "InputQuery::from logical stream name=" << stream.getName() << " pyhName=" << name << " srcType="
                                                    << type << " srcConf=" << conf << " frequency=" << frequency
                                                    << " numBuffers=" << numBuffers);

        if (type == "DefaultSource") {
            if (numBuffers == 1) {
                NES_DEBUG("InputQuery::from create default source for one buffer");
                op = createSourceOperator(
                    createDefaultDataSourceWithSchemaForOneBuffer(stream.getSchema(), bPtr, dPtr));
            } else {
                NES_DEBUG(
                    "InputQuery::from create default source for " << numBuffers << " buffers");
                op = createSourceOperator(
                    createDefaultDataSourceWithSchemaForVarBuffers(stream.getSchema(),
                                                                   bPtr, dPtr,
                                                                   numBuffers,
                                                                   frequency));
            }
        } else if (type == "CSVSource") {
            NES_DEBUG("InputQuery::from create CSV source for " << conf << " buffers");
            op = createSourceOperator(
                createCSVFileSource(stream.getSchema(), bPtr, dPtr, /**fileName*/ conf, ",",
                                    /**numberOfBufferToProduce*/ numBuffers,
                                    frequency));
        } else if (type == "SenseSource") {
            NES_DEBUG("InputQuery::from create Sense source for udfs " << conf);
            op = createSourceOperator(
                createSenseSource(stream.getSchema(), bPtr, dPtr, /**udfs*/ conf));
        } else {
            NES_DEBUG("InputQuery::from source type " << type << " not supported");
            NES_FATAL_ERROR("type not supported");
        }
    }
    int operatorId = q.getNextOperatorId();
    op->setOperatorId(operatorId);
    q.root = op;
    return q;
}

/*
 * Relational Operators
 */

InputQuery& InputQuery::select(const Field& field) {
    NES_NOT_IMPLEMENTED();
}

InputQuery& InputQuery::select(const Field& field1, const Field& field2) {
    NES_NOT_IMPLEMENTED();
}

InputQuery& InputQuery::sample(const std::string& udfs) {
    OperatorPtr op = createSampleOperator(udfs);
    int operatorId = this->getNextOperatorId();
    op->setOperatorId(operatorId);
    addChild(op, root);
    root = op;
    //TODO: this currently set it globally, in the future it should be connected to a source
    this->udfs = udfs;
    return *this;
}

InputQuery& InputQuery::filter(const UserAPIExpression& predicate) {
    PredicatePtr pred = createPredicate(predicate);
    OperatorPtr op = createFilterOperator(pred);
    int operatorId = this->getNextOperatorId();
    op->setOperatorId(operatorId);
    addChild(op, root);
    root = op;
    return *this;
}

InputQuery& InputQuery::map(const AttributeField& field,
                            const Predicate predicate) {
    PredicatePtr pred = createPredicate(predicate);
    AttributeFieldPtr attr = field.copy();
    OperatorPtr op = createMapOperator(attr, pred);
    int operatorId = this->getNextOperatorId();
    op->setOperatorId(operatorId);
    addChild(op, root);
    root = op;
    return *this;
}

InputQuery& InputQuery::combine(const NES::InputQuery& sub_query) {
    NES_NOT_IMPLEMENTED();
}

InputQuery& InputQuery::join(const InputQuery& sub_query,
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

InputQuery& InputQuery::windowByKey(const AttributeFieldPtr onKey,
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

InputQuery& InputQuery::window(const NES::WindowTypePtr windowType,
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
InputQuery& InputQuery::writeToFile(const std::string& file_name) {
    OperatorPtr op = createSinkOperator(
        createBinaryFileSinkWithSchema(this->sourceStream->getSchema(),
                                       file_name));
    int operatorId = this->getNextOperatorId();
    op->setOperatorId(operatorId);
    addChild(op, root);
    root = op;
    return *this;
}

// output operators
InputQuery& InputQuery::writeToCSVFile(const std::string& file_name, const std::string& outputMode) {
    OperatorPtr op;
    if (outputMode == "append") {
        NES_DEBUG("InputQuery::writeToCSVFile: with modus append");
        op = createSinkOperator(
            createCSVFileSinkWithSchemaAppend(this->sourceStream->getSchema(), file_name));
    } else if (outputMode == "truncate") {
        NES_DEBUG("InputQuery::writeToCSVFile: with modus truncate");
        op = createSinkOperator(
            createCSVFileSinkWithSchemaOverwrite(this->sourceStream->getSchema(), file_name));
    } else {
        NES_ERROR("writeToCSVFile mode not supported " << outputMode);
    }

    int operatorId = this->getNextOperatorId();
    op->setOperatorId(operatorId);
    addChild(op, root);
    root = op;
    return *this;
}

InputQuery& InputQuery::writeToZmq(const std::string& logicalStreamName,
                                   const std::string& host,
                                   const uint16_t& port) {
    SchemaPtr ptr = StreamCatalog::instance().getSchemaForLogicalStream(
        logicalStreamName);
    OperatorPtr op = createSinkOperator(createZmqSink(ptr, host, port));
    int operatorId = this->getNextOperatorId();
    op->setOperatorId(operatorId);
    addChild(op, root);
    root = op;
    return *this;
}

InputQuery& InputQuery::print(std::ostream& out) {
    OperatorPtr op = createSinkOperator(
        createPrintSinkWithSchema(this->sourceStream->getSchema(), out));
    int operatorId = this->getNextOperatorId();
    op->setOperatorId(operatorId);
    addChild(op, root);
    root = op;
    return *this;
}

InputQuery& InputQuery::writeToKafka(const std::string& brokers,
                                     const std::string& topic,
                                     const size_t kafkaProducerTimeout) {
    OperatorPtr op = createSinkOperator(
        createKafkaSinkWithSchema(this->sourceStream->getSchema(), brokers, topic,
                                  kafkaProducerTimeout));
    int operatorId = this->getNextOperatorId();
    op->setOperatorId(operatorId);
    addChild(op, root);
    root = op;
    return *this;
}
const StreamPtr InputQuery::getSourceStream() const {
    return sourceStream;
}
void InputQuery::setSourceStream(const StreamPtr sourceStream) {
    InputQuery::sourceStream = sourceStream;
}

void addChild(const OperatorPtr opParent, const OperatorPtr opChild) {
    if (opParent && opChild) {
        opChild->setParent(opParent);
        vector<OperatorPtr> children = opParent->getChildren();
        children.push_back(opChild);
        opParent->setChildren(children);
    }
}

}// namespace NES
