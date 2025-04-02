/*
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

#ifndef NES_CLIENT_INCLUDE_API_QUERY_HPP_
#define NES_CLIENT_INCLUDE_API_QUERY_HPP_

#include <API/Expressions/Expressions.hpp>
#include <API/TimeUnit.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinDescriptor.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Metrics/StatisticMetric.hpp>
#include <Operators/LogicalOperators/StatisticCollection/WindowStatisticDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <memory>
#include <string>
#include <vector>

namespace NES {

class Query;
class Operator;
using OperatorPtr = std::shared_ptr<Operator>;

class ExpressionNode;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;

class FieldAssignmentExpressionNode;
using FieldAssignmentExpressionNodePtr = std::shared_ptr<FieldAssignmentExpressionNode>;

class SourceLogicalOperator;
using SourceLogicalOperatorPtr = std::shared_ptr<SourceLogicalOperator>;

class SinkLogicalOperator;
using SinkLogicalOperatorPtr = std::shared_ptr<SinkLogicalOperator>;

class SinkDescriptor;
using SinkDescriptorPtr = std::shared_ptr<SinkDescriptor>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

namespace API {
class WindowAggregation;
using WindowAggregationPtr = std::shared_ptr<WindowAggregation>;
}// namespace API

namespace Catalogs::UDF {
class UDFDescriptor;
using UDFDescriptorPtr = std::shared_ptr<UDFDescriptor>;
}// namespace Catalogs::UDF

namespace WindowOperatorBuilder {

class WindowedQuery;
class KeyedWindowedQuery;

}// namespace WindowOperatorBuilder
namespace Windowing {
class WindowType;
using WindowTypePtr = std::shared_ptr<WindowType>;

class WindowAggregationDescriptor;
using WindowAggregationDescriptorPtr = std::shared_ptr<WindowAggregationDescriptor>;

class WatermarkStrategyDescriptor;
using WatermarkStrategyDescriptorPtr = std::shared_ptr<WatermarkStrategyDescriptor>;
}// namespace Windowing

static constexpr uint64_t defaultTriggerTimeInMs = 1000;

namespace JoinOperatorBuilder {

class JoinWhere;

class Join {
  public:
    /**
     * @brief Constructor. Initialises always subQueryRhs and original Query
     * @param subQueryRhs
     * @param originalQuery
     */
    Join(const Query& subQueryRhs, Query& originalQuery);

    /**
     * @brief is called to append all joinExpressions (key predicates) to the previous defined join, i.e.,
     * it sets all condition for the join matches
     * @param joinExpression : a set of binary expressions to compare left and right tuples
     * @return object of type JoinWhere on which window function is defined and can be called.
     */
    [[nodiscard]] JoinWhere where(ExpressionNodePtr joinExpression) const;

  private:
    const Query& subQueryRhs;
    Query& originalQuery;
};

class JoinWhere {
  public:
    /**
     * @brief Constructor. Initialises always subQueryRhs, original Query and the joinExpression
     * @param subQueryRhs
     * @param originalQuery
     * @param joinExpression : a set of binary expressions to compare left and right tuples
     */
    JoinWhere(const Query& subQueryRhs, Query& originalQuery, ExpressionNodePtr joinExpressions);

    /**
     * @brief: calls internal the original joinWith function with all the gathered parameters.
     * @param windowType
     * @return the query with the result of the original joinWith function is returned.
     */
    [[nodiscard]] Query& window(Windowing::WindowTypePtr const& windowType) const;

  private:
    const Query& subQueryRhs;
    Query& originalQuery;
    ExpressionNodePtr joinExpressions;
};

}//namespace JoinOperatorBuilder

/**
* @brief BatchJoinOperatorBuilder.
* @note Initialize as Join between originalQuery and subQueryRhs.
* @note In contrast to the JoinOperatorBuilder only .where() and .key() need to be applied to join the query.
* @note No windowing is required.
*/
namespace Experimental::BatchJoinOperatorBuilder {

class JoinWhere;

class Join {
  public:
    /**
     * @brief Constructor. Initialises always subQueryRhs and original Query
     * @param subQueryRhs
     * @param originalQuery
     */
    Join(const Query& subQueryRhs, Query& originalQuery);

    /** @brief is called to append all joinExpressions (key predicates) to the previous defined join, i.e.,
     * it sets all condition for the join matches
     * @param joinExpression : a set of binary expressions to compare left and right tuples
     * @return object of type JoinWhere on which equalsTo function is defined and can be called.
     */
    [[nodiscard]] Query& where(const ExpressionNodePtr joinExpression) const;

  private:
    const Query& subQueryRhs;
    Query& originalQuery;
};

}//namespace Experimental::BatchJoinOperatorBuilder

/**
* @brief CrossJoinOperatorBuilder.
* @note Initialize as Cartesian Product between originalQuery and subQueryRhs.
* @note In contrast to the JoinOperatorBuilder no .where() needs to be applied to the query.
* @note windowing is required.
*/
namespace CrossJoinOperatorBuilder {

class CrossJoin {
  public:
    CrossJoin(const Query& subQueryRhs, Query& subQueryLhs);

    /**
     * @brief: calls internal the original joinWith function with all the gathered parameters.
     * @param windowType
     * @return the query with the result of the original joinWith function is returned.
     */
    [[nodiscard]] Query& window(Windowing::WindowTypePtr const& windowType) const;

  private:
    Query& subQueryRhs;
    Query& subQueryLhs;
    ExpressionNodePtr joinExpressions;
};

}//namespace CrossJoinOperatorBuilder

/**
 * @brief IntervalJoinOperator
 * @note Initialize as Join between originalQuery and subQueryRhs.
 * @note In contrast to the JoinOperatorBuilder the windowing is replaced by an interval
 * @note There is no windowing possible
 * @note An interval (given by an upper und lower bound) is required
 */
namespace IntervalJoinOperatorBuilder {

class JoinWhere;
class JoinLowerBound;

class IntervalJoin {
  public:
    /**
     * @brief Constructor. Initialises always subQueryRhs and original Query
     * @param subQueryRhs
     * @param originalQuery
     */
    IntervalJoin(const Query& subQueryRhs, Query& originalQuery);

    /**
     * @brief is called to append all joinExpressions (key predicates) to the previous defined join, i.e.,
     * it sets all condition for the join matches
     * @param joinExpression : a set of binary expressions to compare left and right tuples
     * @return object of type JoinWhere on which the lowerBound function is defined and can be called.
     */
    [[nodiscard]] JoinWhere where(ExpressionNodePtr joinExpression) const;

  private:
    const Query& subQueryRhs;
    Query& originalQuery;
};

class JoinWhere {
  public:
    /**
     * @brief Constructor. Initialises always subQueryRhs, original Query and onLeftKey
     * @param subQueryRhs
     * @param originalQuery
     * @param joinExpression : a set of binary expressions to compare left and right tuples
     */
    JoinWhere(const Query& subQueryRhs, Query& originalQuery, ExpressionNodePtr joinExpression);

    /**
     * @brief sets the parameters of the lower interval bound.
     * @param timeCharacteristic : to identify what should be used for the tuple timestamp
     * @param lowerBound : the value of the lower bound
     * @param lowerTimeUnit : the time unit (seconds, minutes, etc) of the lower bound
     * @param lowerBoundInclusive : true if the lower bound is inclusive; false if it is exclusive. default: true
     * @return object of type JoinLowerBound on which the upperBound function can be called
     */
    [[nodiscard]] JoinLowerBound lowerBound(Windowing::TimeCharacteristicPtr timeCharacteristic,
                                            int64_t lowerBound,
                                            Windowing::TimeUnit lowerTimeUnit,
                                            bool lowerBoundInclusive = true) const;

  private:
    const Query& subQueryRhs;
    Query& originalQuery;
    ExpressionNodePtr joinExpression;
};

class JoinLowerBound {
  public:
    /**
     * Constructor which always initializes the following parameters
     * @param subQueryRhs
     * @param originalQuery
     * @param joinExpression
     * @param timeCharacteristic
     * @param lowerBound
     * @param lowerTimeUnit
     * @param lowerBoundInclusive
     */
    JoinLowerBound(const Query& subQueryRhs,
                   Query& originalQuery,
                   ExpressionNodePtr joinExpression,
                   Windowing::TimeCharacteristicPtr timeCharacteristic,
                   int64_t lowerBound,
                   Windowing::TimeUnit lowerTimeUnit,
                   bool lowerBoundInclusive);

    /**
     * @brief: calls internal the original joinWith function with all the gathered parameters.
     * @param upperBound : the value of the upper bound
     * @param upperTimeUnit : the time unit (seconds, minutes, etc) of the upper bound
     * @param upperBoundInclusive : true if the upper bound is inclusive; false if it is exclusive, default = true
     * @return the query with the result of the original joinWith function is returned.
     */
    [[nodiscard]] Query& upperBound(int64_t upperBound, Windowing::TimeUnit upperTimeUnit, bool upperBoundInclusive = true) const;

  private:
    const Query& subQueryRhs;
    Query& originalQuery;
    ExpressionNodePtr joinExpression;
    Windowing::TimeCharacteristicPtr timeCharacteristic;
    int64_t lowerBound;
    Windowing::TimeUnit lowerTimeUnit;
    bool lowerBoundInclusive;
};

}//namespace IntervalJoinOperatorBuilder

namespace CEPOperatorBuilder {

class And {
  public:
    /**
     * @brief Constructor. Initialises always subQueryRhs and original Query
     * @param subQueryRhs
     * @param originalQuery
     */
    And(const Query& subQueryRhs, Query& originalQuery);

    /**
     * @brief: calls internal the original andWith function with all the gathered parameters.
     * @param windowType
     * @return the query with the result of the original andWith function is returned.
     */
    [[nodiscard]] Query& window(Windowing::WindowTypePtr const& windowType) const;

  private:
    Query& subQueryRhs;
    Query& originalQuery;
    ExpressionNodePtr joinExpression;
};

class Seq {
  public:
    /**
     * @brief Constructor. Initialises always subQueryRhs and original Query
     * @param subQueryRhs
     * @param originalQuery
     */
    Seq(const Query& subQueryRhs, Query& originalQuery);

    /**
     * @brief: calls internal the original seqWith function with all the gathered parameters.
     * @param windowType
     * @return the query with the result of the original seqWith function is returned.
     */
    [[nodiscard]] Query& window(Windowing::WindowTypePtr const& windowType) const;

  private:
    Query& subQueryRhs;
    Query& originalQuery;
    ExpressionNodePtr joinExpression;
};

/**
     * @brief: This operator is a CEP operator, in CEP engines also called iteration operator. It
     * allows for multiple occurrences of a specified event, i.e., tuples.
     * Thus, 'times' enables patterns of arbitrary length (when only minOccurrences are defined) or
     * requires a specified number of tuples (minOccurrence, maxOccurrence) to occur
     * The Times operator requires the call of the window operator afterwards
     * @return cepBuilder
     */

class Times {
  public:
    /**
     * @brief Constructor (bounded variant to a number of minOccurrences to maxOccurrences of event occurrence)
     * @param minOccurrences: minimal number of occurrences of a specified event, i.e., tuples
     * @param maxOccurrences: maximal number of occurrences of a specified event, i.e., tuples
     * @param originalQuery
     * @return cepBuilder
     */
    Times(const uint64_t minOccurrences, const uint64_t maxOccurrences, Query& originalQuery);

    /**
     * @brief Constructor (bounded variant to exact amount of occurrence)
     * @param occurrence the exact amount of occurrences expected
     * @param originalQuery
     * @return cepBuilder
     */
    Times(const uint64_t occurrences, Query& originalQuery);

    /**
     * @brief Constructor (unbounded variant)
     * @param originalQuery
     * @return cepBuilder
     */
    Times(Query& originalQuery);

    /**
     * @brief: calls internal the original seqWith function with all the gathered parameters.
     * @param windowType
     * @return the query with the result of the original seqWith function is returned.
     */
    [[nodiscard]] Query& window(Windowing::WindowTypePtr const& windowType) const;

  private:
    Query& originalQuery;
    uint64_t minOccurrences;
    uint64_t maxOccurrences;
    bool bounded;
};
//TODO this method is a quick fix to generate unique keys for andWith chains and should be removed after implementation of Cartesian Product (#2296)
/**
     * @brief: this function creates a virtual key for the left side of the binary operator
     * @param keyName the attribute name
     * @return the unique name of the key
     */
std::string keyAssignment(std::string keyName);

}//namespace CEPOperatorBuilder

/**
 * User interface to create stream processing queryIdAndCatalogEntryMapping.
 * The current api exposes method to create queryIdAndCatalogEntryMapping using all currently supported operators.
 */
class Query {
  public:
    Query(const Query&);

    virtual ~Query() = default;

    // Join, IntervalJoin and CEPOperatorBuilder friend classes, are required as they use the private joinWith method.
    friend class JoinOperatorBuilder::JoinWhere;
    friend class NES::Experimental::BatchJoinOperatorBuilder::Join;
    friend class IntervalJoinOperatorBuilder::JoinWhere;
    friend class CrossJoinOperatorBuilder::CrossJoin;
    friend class IntervalJoinOperatorBuilder::JoinLowerBound;
    friend class CEPOperatorBuilder::And;
    friend class CEPOperatorBuilder::Seq;
    friend class WindowOperatorBuilder::WindowedQuery;
    friend class WindowOperatorBuilder::KeyedWindowedQuery;

    WindowOperatorBuilder::WindowedQuery window(Windowing::WindowTypePtr const& windowType);

    /**
     * @brief can be called on the original query with the query to be joined with and sets this query in the class Join.
     * @param subQueryRhs
     * @return object where where() function is defined and can be called by user
     */
    JoinOperatorBuilder::Join joinWith(const Query& subQueryRhs);

    /**
     * @brief can be called on the original query with the query to be joined with and sets this query in the class BatchJoinOperatorBuilder::Join.
     * @warning The batch join is an experimental feature.
     * @param subQueryRhs
     * @return object where where() function is defined and can be called by user
     */
    NES::Experimental::BatchJoinOperatorBuilder::Join batchJoinWith(const Query& subQueryRhs);

    /**
     * @brief can be called on the original query with the query to be cross joined with and sets this query in the class CrossJoinOperatorBuilder::Join.
     * @param subQueryRhs
     * @return object of type CrossJoin on which the window can be can be called.
     */
    CrossJoinOperatorBuilder::CrossJoin crossJoinWith(const Query& subQueryRhs);

    /**
     * @brief can be called on the original query with the query to be joined with and sets this query in the class IntervalJoinOperatorBuilder::Join.
     * @param subQueryRhs
     * @return object where where() function is defined and can be called by user
     */
    IntervalJoinOperatorBuilder::IntervalJoin intervalJoinWith(const Query& subQueryRhs);

    /**
     * @brief can be called on the original query with the query to be composed with and sets this query in the class And.
     * @param subQueryRhs
     * @return CEPOperatorBuilder object where the window() function is defined and can be called by user
     */
    CEPOperatorBuilder::And andWith(const Query& subQueryRhs);

    /**
     * @brief can be called on the original query with the query to be composed with and sets this query in the class Join.
     * @param subQueryRhs
     * @return CEPOperatorBuilder object where the window() function is defined and can be called by user
     */
    CEPOperatorBuilder::Seq seqWith(const Query& subQueryRhs);

    /**
     * @brief can be called on the original query to detect an number event occurrences between minOccurrence and maxOccurrence in a stream
     * @param minOccurrences
     * @param maxOccurrences
     * @return CEPOperatorBuilder object where the window() function is defined and can be called by user
     */
    CEPOperatorBuilder::Times times(const uint64_t minOccurrences, const uint64_t maxOccurrences);

    /**
     * @brief can be called on the original query to detect an exact number event occurrences in a stream
     * @param occurrences
     * @return CEPOperatorBuilder object where the window() function is defined and can be called by user
     */
    CEPOperatorBuilder::Times times(const uint64_t occurrences);

    /**
     * @brief can be called on the original query to detect multiple occurrences of specified events in a stream
     * @return CEPOperatorBuilder object where the window() function is defined and can be called by user
     */
    CEPOperatorBuilder::Times times();

    /**
     * @brief can be called on the original query with the query to be composed with and sets this query in the class Or.
     * @param subQueryRhs
     * @return the query (pushed to union with)
     */
    Query& orWith(const Query& subQuery);

    /**
     * @brief: Creates a query from a particular source. The source is identified by its name.
     * During query processing the underlying source descriptor is retrieved from the source catalog.
     * @param sourceName name of the source to query. This name has to be registered in the query catalog.
     * @return the query
     */
    static Query from(std::string const& sourceName);

    /**
     * @brief Add a synopsis build operator to the query
     * @param window
     * @param statisticDescriptor
     * @param metricHash: The hash of the metric, this operator is collecting, e.g., `cardinality` over field `f1`
     * @param sendingPolicy: Policy so when and how to send the data
     * @param triggerCondition: Policy when and how to call the callback method
     * @return The query
     */
    Query& buildStatistic(Windowing::WindowTypePtr window,
                          Statistic::WindowStatisticDescriptorPtr statisticDescriptor,
                          Statistic::StatisticMetricHash metricHash,
                          Statistic::SendingPolicyPtr sendingPolicy,
                          Statistic::TriggerConditionPtr triggerCondition);

    /**
    * This looks ugly, but we can't reference to QueryPtr at this line.
    * @param subQuery is the query to be unioned
    * @return the query
    */
    Query& unionWith(const Query& subQuery);

    /**
     * @brief this call projects out the attributes in the parameter list
     * @param attribute list
     * @return the query
     */
    template<typename... Args>
    auto project(Args&&... args) -> std::enable_if_t<std::conjunction_v<std::is_constructible<ExpressionItem, Args>...>, Query&> {
        return project({std::forward<Args>(args).getExpressionNode()...});
    }

    /**
      * @brief this call projects out the attributes in the parameter list
      * @param attribute list
      * @return the query
      */
    Query& project(std::vector<ExpressionNodePtr> expressions);

    /**
     * This looks ugly, but we can't reference to QueryPtr at this line.
     * @param new source name
     * @return the query
     */
    Query& as(std::string const& newSourceName);

    /**
     * @brief: Filter records according to the predicate. An
     * examplary usage would be: filter(Attribute("f1" < 10))
     * @param predicate as expression node
     * @return the query
     */
    Query& filter(ExpressionNodePtr const& filterExpression);

    /**
     * @brief: Limit the number of records according to the limit count.
     * @param limitCount
     * @return the query
     */
    Query& limit(const uint64_t limit);

    /**
     * @brief: Create watermark assigner operator.
     * @param watermarkStrategyDescriptor
     * @return query.
     */
    Query& assignWatermark(Windowing::WatermarkStrategyDescriptorPtr const& watermarkStrategyDescriptor);

    /**
     * @brief: Create map java udf operator.
     * @param descriptor java udf descriptor
     * @return query
     */
    Query& mapUDF(Catalogs::UDF::UDFDescriptorPtr const& descriptor);

    /**
     * @brief: Create flat map java udf operator.
     * @param descriptor java udf descriptor
     * @return query
     */
    Query& flatMapUDF(Catalogs::UDF::UDFDescriptorPtr const& descriptor);

    /**
     * @brief: Map records according to a map expression. An
     * examplary usage would be: map(Attribute("f2") = Attribute("f1") * 42 )
     * @param map expression
     * @return query
     */
    Query& map(FieldAssignmentExpressionNodePtr const& mapExpression);

    /**
     * @brief: inferModel
     * @example example
     * @param param
     * @return query
     */
    Query& inferModel(std::string model,
                      std::initializer_list<ExpressionItem> inputFields,
                      std::initializer_list<ExpressionItem> outputFields);

    
    /**
     * @brief Add sink operator for the query.
     * The Sink operator is defined by the sink descriptor, which represents the semantic of this sink.
     * @param sinkDescriptor
     * @param workerId: location where sink is to be placed
     */
    virtual Query& sink(SinkDescriptorPtr sinkDescriptor, WorkerId workerId = INVALID_WORKER_NODE_ID);

    /**
     * @brief Gets the query plan from the current query.
     * @return QueryPlan
     */
    QueryPlanPtr getQueryPlan() const;

    // creates a new query object
    Query(QueryPlanPtr queryPlan);

  protected:
    // query plan containing the operators.
    QueryPlanPtr queryPlan;

  private:
    /**
     * @new change: Now it's private, because we don't want the user to have access to it.
     * We call it only internal as a last step during the Join operation
     * @brief This methods adds the joinType to the join operator and calls the join function to add the operator to a query
     * @param subQueryRhs subQuery to be joined
     * @param onLeftKey key attribute of the left source
     * @param onLeftKey key attribute of the right source
     * @param windowType Window definition.
     * @return the query
     */
    Query& joinWith(const Query& subQueryRhs, ExpressionNodePtr joinExpression, Windowing::WindowTypePtr const& windowType);

    /**
     * @new change: Now it's private, because we don't want the user to have access to it.
     * We call it only internal as a last step during the Join operation
     * @note In contrast to joinWith(), batchJoinWith() does not require a window to be specified.
     * @param subQueryRhs subQuery to be joined
     * @param onLeftKey key attribute of the left stream
     * @param onLeftKey key attribute of the right stream
     * @return the query
     */
    Query& batchJoinWith(const Query& subQueryRhs, ExpressionNodePtr joinExpression);

    /**
      * @new change: Now it's private, because we don't want the user to have access to it.
      * We call it only internal as a last step during the Join operation
      * @note In contrast to joinWith() and batchJoinWith(), intervalJoinWith() requires interval bounds instead of a window to be specified .
      * @param subQueryRhs : subQuery to be joined
      * @param joinExpression : a set of binary expressions to compare left and right tuples
      * @param timeCharacteristic : to identify what should be used for the tuple timestamp
      * @param lowerBound : the value of the lower bound
      * @param lowerTimeUnit : TimeUnit describing the unit of the lower bound
      * @param lowerBoundInclusive : true if the lower bound is inclusive; false if it is exclusive
      * @param upperBound : the value of the upper bound
      * @param upperTimeUnit : TimeUnit describing the unit of the upper bound
      * @param upperBoundInclusive : true if the upper bound is inclusive; false if it is exclusive
      * @return the query
      */
    Query& intervalJoinWith(const Query& subQueryRhs,
                            ExpressionNodePtr joinExpression,
                            Windowing::TimeCharacteristicPtr TimeCharacteristic,
                            int64_t lowerBound,
                            Windowing::TimeUnit lowerTimeUnit,
                            bool lowerBoundInclusive,
                            int64_t upperBound,
                            Windowing::TimeUnit upperTimeUnit,
                            bool upperBoundInclusive);

    /**
     * @new change: Now it's private, because we don't want the user to have access to it.
     * We call it only internal as a last step during the AND operation
     * @brief This methods adds the joinType to the join operator and calls join function to add the operator to a query
     * @param subQueryRhs subQuery to be composed
     * @param onLeftKey key attribute of the left source
     * @param onLeftKey key attribute of the right source
     * @param windowType Window definition.
     * @return the query
     */
    Query& andWith(const Query& subQueryRhs, ExpressionNodePtr joinExpressions, Windowing::WindowTypePtr const& windowType);

    /**
     * @new change: Now it's private, because we don't want the user to have access to it.
     * We call it only internal as a last step during the SEQ operation
     * @brief This methods adds the joinType to the join operator and calls join function to add the operator to a query
     * @param subQueryRhs subQuery to be composed
     * @param onLeftKey key attribute of the left source
     * @param onLeftKey key attribute of the right source
     * @param windowType Window definition.
     * @return the query
     */
    Query& seqWith(const Query& subQueryRhs, ExpressionNodePtr joinExpressions, Windowing::WindowTypePtr const& windowType);

    /**
     * @new change: similar to join, the original window and windowByKey become private --> only internal use
     * @brief: Creates a window aggregation.
     * @param windowType Window definition.
     * @param aggregations Window aggregation function.
     * @return query.
     */
    Query& window(Windowing::WindowTypePtr const& windowType, std::vector<API::WindowAggregationPtr> aggregations);

    /**
      * @brief: Creates a keyed window aggregation.
      * @param joinExpressions keys.
      * @param windowType Window definition.
      * @param aggregations Window aggregation functions.
      * @return query.
      */
    Query& windowByKey(std::vector<ExpressionNodePtr> joinExpressions,
                       Windowing::WindowTypePtr const& windowType,
                       std::vector<API::WindowAggregationPtr> aggregations);

    /**
      * @brief: Given a Expression is identifies which JoinType has to be used for processing, i.e., Equi-Join enables
      * different Join algorithms, while all other join types right lead to the NestedLoopJoin
      * @param joinExpressions key expressions
      * @return joinType
      */
    Join::LogicalJoinDescriptor::JoinType identifyJoinType(ExpressionNodePtr joinExpressions);
};

using QueryPtr = std::shared_ptr<Query>;

}// namespace NES

#endif// NES_CLIENT_INCLUDE_API_QUERY_HPP_
