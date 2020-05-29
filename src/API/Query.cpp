#include <API/Query.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <iostream>
#include <Catalogs/StreamCatalogEntry.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>

namespace NES {

Query::Query(std::string sourceStreamName, QueryPlanPtr queryPlan) : sourceStreamName(sourceStreamName), queryPlan(queryPlan){}

Query::Query(const Query& query)
    : sourceStreamName(query.sourceStreamName),
      queryPlan(query.queryPlan) {
}

StreamCatalogPtr Query::streamCatalog = nullptr;

Query Query::from(const std::string sourceStreamName) {
    NES_DEBUG("Query: create query for input stream " << sourceStreamName);

    std::vector<StreamCatalogEntryPtr> catalogEntry;
    if (streamCatalog) {
        catalogEntry = streamCatalog->getPhysicalStreams(sourceStreamName);
    } else {
        NES_ERROR("Query::from: stream catalog not set");
    }

    SchemaPtr schemaPtr = streamCatalog->getSchemaForLogicalStream(sourceStreamName);

    OperatorNodePtr sourceOperator;
    if (catalogEntry.size() == 0) {
        NES_ERROR(
            "Query::from physical stream does not exists: " << sourceStreamName);
        throw Exception("Query: physical stream does not exists:" + sourceStreamName);
    } else {

        std::string name = catalogEntry[0]->getPhysicalName();
        std::string type = catalogEntry[0]->getSourceType();
        std::string conf = catalogEntry[0]->getSourceConfig();
        size_t frequency = catalogEntry[0]->getSourceFrequency();
        size_t numBuffers = catalogEntry[0]->getNumberOfBuffersToProduce();

        NES_DEBUG(
            "Query::from logical stream name=" << sourceStreamName << " pyhName=" << name << " srcType="
                                               << type << " srcConf=" << conf << " frequency=" << frequency
                                               << " numBuffers=" << numBuffers);
        if (type == "DefaultSource") {
            NES_DEBUG("Query::from create default source for  " << numBuffers <<" buffer(s)");
            sourceOperator = createSourceLogicalOperatorNode(DefaultSourceDescriptor::create(schemaPtr, numBuffers,
                                                                                             frequency));
        } else if (type == "CSVSource") {
            NES_DEBUG("Query::from create CSV source for " << conf << " buffers");
            sourceOperator = createSourceLogicalOperatorNode(CsvSourceDescriptor::create(schemaPtr,
                /**fileName*/ conf, /**delimiter*/ ",", numBuffers, frequency));
        } else if (type == "SenseSource") {
            NES_DEBUG("Query::from create Sense source for udfs " << conf);
            sourceOperator = createSourceLogicalOperatorNode(SenseSourceDescriptor::create(schemaPtr, /**udfs*/ conf));
        } else {
            NES_DEBUG("Query::from source type " << type << " not supported");
            NES_FATAL_ERROR("type not supported");
        }
    }

    auto queryPlan = QueryPlan::create(sourceOperator);
    return Query(sourceStreamName, queryPlan);
}

//FIXME: Temp method for porting code from operators to nes node implementation. We will remove the call once we have fixed
// issue #512 and #511
Query Query::createFromQueryPlan(std::string sourceStreamName, QueryPlanPtr queryPlan) {
    return Query(sourceStreamName, queryPlan);
}

Query& Query::filter(const ExpressionNodePtr filterExpression) {
    NES_DEBUG("Query: add filter operator to query");
    OperatorNodePtr op = createFilterLogicalOperatorNode(filterExpression);
    queryPlan->appendOperator(op);
    return *this;
}

Query& Query::map(const FieldAssignmentExpressionNodePtr mapExpression) {
    NES_DEBUG("Query: add map operator to query");
    OperatorNodePtr op = createMapLogicalOperatorNode(mapExpression);
    queryPlan->appendOperator(op);
    return *this;
}

Query& Query::sink(const SinkDescriptorPtr sinkDescriptor) {
    NES_DEBUG("Query: add sink operator to query");
    OperatorNodePtr op = createSinkLogicalOperatorNode(sinkDescriptor);
    queryPlan->appendOperator(op);
    return *this;
}

QueryPlanPtr Query::getQueryPlan() {
    return queryPlan;
}

const std::string Query::getSourceStreamName() const {
    return sourceStreamName;
}

}// namespace NES
