/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef API_PATTERN_H
#define API_PATTERN_H

#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/Query.hpp>
#include <API/Schema.hpp>

#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Sources/DataSource.hpp>
#ifdef ENABLE_KAFKA_BUILD
#include <cppkafka/configuration.h>
#endif
#include <string>

namespace NES {

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class ExpressionNode;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;

class SourceLogicalOperatorNode;
using SourceLogicalOperatorNodePtr = std::shared_ptr<SourceLogicalOperatorNode>;

class SinkLogicalOperatorNode;
using SinkLogicalOperatorNodePtr = std::shared_ptr<SinkLogicalOperatorNode>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

/**
 * User interface to specify a pattern query.
 */
class Pattern : public Query {

  public:
    Pattern(const Pattern&);
    ~Pattern() override = default;

    /**
     * @brief: Monitors a condition from a particular source stream. The source stream is identified by its name.
     * During processing the underlying source descriptor is retrieved from the stream catalog.
     * @param sourceStreamName name of the stream to monitor. This name has to be registered in the query catalog.
     * @return the pattern
     */
    static Pattern from(std::string const& sourceStreamName);

    /**
    * @brief: Filter records according to the predicate.
    * @example filter(Attribute("f1" < 10))
    * @param predicate as expression node
    * @return the query
    */
    Pattern& filter(ExpressionNodePtr const& filterExpression);

    /**
     * @brief: Monitors a condition from a particular source stream. The source stream is identified by its name.
     * During processing the underlying source descriptor is retrieved from the stream catalog.
     * @param sourceStreamName name of the stream to monitor. This name has to be registered in the query catalog.
     * @return the pattern
     */
    Pattern iter(uint64_t minIterations, uint64_t maxIteration);

    /**
     * @brief Add sink operator for the query.
     * The Sink operator is defined by the sink descriptor, which represents the semantic of this sink.
     * @param sinkDescriptor
     */
    Pattern& sink(SinkDescriptorPtr sinkDescriptor) override;

    [[nodiscard]] const std::string& getPatternName() const;
    void setPatternName(const std::string& patternName);

  private:
    // creates a new pattern object
    Pattern(QueryPlanPtr queryPlan);

    std::string patternName;
};

using PatternPtr = std::shared_ptr<Pattern>;

}// namespace NES

#endif
