#ifndef API_PATTERN_H
#define API_PATTERN_H

#include <API/Config.hpp>
#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/Query.hpp>
#include <API/Schema.hpp>
#include <API/Stream.hpp>

#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <SourceSink/DataSource.hpp>
#include <cppkafka/configuration.h>
#include <string>

namespace NES {

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class ExpressionNode;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;

class SourceLogicalOperatorNode;
typedef std::shared_ptr<SourceLogicalOperatorNode> SourceLogicalOperatorNodePtr;

class SinkLogicalOperatorNode;
typedef std::shared_ptr<SinkLogicalOperatorNode> SinkLogicalOperatorNodePtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

/**
 * User interface to specify a pattern query.
 */
class Pattern : public Query {

  public:
    Pattern(const Pattern&);
    ~Pattern() = default;

    /**
     * @brief: Monitors a condition from a particular source stream. The source stream is identified by its name.
     * During processing the underlying source descriptor is retrieved from the stream catalog.
     * @param sourceStreamName name of the stream to monitor. This name has to be registered in the query catalog.
     * @return the pattern
     */
    static Pattern from(const std::string sourceStreamName);

    /**
     * @brief Add sink operator for the query.
     * The Sink operator is defined by the sink descriptor, which represents the semantic of this sink.
     * @param sinkDescriptor
     */
    //Pattern& sink(const SinkDescriptorPtr sinkDescriptor);

    const std::string& getPatternName() const;
    void setPatternName(const std::string& patternName);

  private:
    // creates a new pattern object
    Pattern(QueryPlanPtr queryPlan);

    std::string patternName;
};

typedef std::shared_ptr<Pattern> PatternPtr;

}// namespace NES

#endif
