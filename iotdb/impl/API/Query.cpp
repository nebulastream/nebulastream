#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Util/Logger.hpp>
#include <cstddef>
#include <iostream>

namespace NES {

Query::Query(QueryPlanPtr queryPlan) : queryPlan(queryPlan) {}

Query::Query(const Query& query)
    : queryPlan(query.queryPlan) {}

Query Query::from(Stream& stream) {

    // TODO:here we assume that all sources are of the same type
    std::vector<StreamCatalogEntryPtr> catalogEntry = StreamCatalog::instance().getPhysicalStreams(stream.getName());

    if (catalogEntry.empty()) {
        NES_ERROR("InputQuery::from stream does not exists this should only be used by tests " << stream.getName())
        throw std::invalid_argument("Non existing logical stream : " + stream.getName() + " provided");
    }

    OperatorNodePtr rootNode;
    SchemaPtr schema = stream.getSchema();

    if (catalogEntry.size() == 0) {
        // If no catalog entry found with the stream name then create a default source

        DefaultSourceDescriptorPtr defaultSourceDescriptor =
            std::make_shared<DefaultSourceDescriptor>(schema, /*bufferCnt*/ 1, /*frequency*/ 1);
        rootNode = createSourceLogicalOperatorNode(defaultSourceDescriptor);
    } else {
        // Pick the first element from the catalog entry and identify the type to create appropriate source type

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
                DefaultSourceDescriptorPtr
                    defaultSourceDescriptor =
                    std::make_shared<DefaultSourceDescriptor>(schema, /*bufferCnt*/ 1, /*frequency*/ 1);

                rootNode = createSourceLogicalOperatorNode(defaultSourceDescriptor);
            } else {
                NES_DEBUG("InputQuery::from create default source for " << numBuffers << " buffers")
                DefaultSourceDescriptorPtr
                    defaultSourceDescriptor = std::make_shared<DefaultSourceDescriptor>(schema, numBuffers, frequency);

                rootNode = createSourceLogicalOperatorNode(defaultSourceDescriptor);
            }
        } else if (type == "CSVSource") {
            NES_DEBUG("InputQuery::from create CSV source for " << conf << " buffers")
            CsvSourceDescriptorPtr csvSourceDescriptor =
                std::make_shared<CsvSourceDescriptor>(schema, /**filePath*/
                                                      conf, /**delimiter*/
                                                      ",",
                                                      numBuffers,
                                                      frequency);
            rootNode = createSourceLogicalOperatorNode(csvSourceDescriptor);
        } else if (type == "SenseSource") {
            NES_DEBUG("InputQuery::from create Sense source for udfs " << conf)
            SenseSourceDescriptorPtr
                csvSourceDescriptor = std::make_shared<SenseSourceDescriptor>(schema, /**udfs*/ conf);
            rootNode = createSourceLogicalOperatorNode(csvSourceDescriptor);
        } else {
            NES_ERROR("InputQuery::from source type " << type << " not supported")
            NES_FATAL_ERROR("type not supported")
        }
    }
    auto streamPtr = std::make_shared<Stream>(stream);
    QueryPlanPtr queryPlan = QueryPlan::create(rootNode, streamPtr);
    Query query(queryPlan);
    return query;
}

/*
 * Relational Operators
 */

Query& Query::filter(const ExpressionNodePtr filterExpression) {
    OperatorNodePtr op = createFilterLogicalOperatorNode(filterExpression);
    queryPlan->appendOperator(op);
    return *this;
}

Query& Query::map(const FieldAssignmentExpressionNodePtr mapExpression) {
    OperatorNodePtr op = createMapLogicalOperatorNode(mapExpression);
    queryPlan->appendOperator(op);
    return *this;
}

Query& Query::combine(const Query& subQuery) {NES_NOT_IMPLEMENTED }

Query& Query::join(const Query& subQuery, const JoinPredicatePtr joinPred) {NES_NOT_IMPLEMENTED }

Query& Query::windowByKey() {NES_NOT_IMPLEMENTED }

Query& Query::window() {NES_NOT_IMPLEMENTED }

Query& Query::to(const std::string& name) {NES_NOT_IMPLEMENTED }

Query& Query::sink(const SinkDescriptorPtr sinkDescriptor) {
    OperatorNodePtr op = createSinkLogicalOperatorNode(sinkDescriptor);
    queryPlan->appendOperator(op);
    return *this;
}

QueryPlanPtr Query::getQueryPlan() {
    return queryPlan;
}

std::string Query::toString() {
    stringstream ss;
    ss << queryPlan->toString();
    return ss.str();
}

} // namespace NES
