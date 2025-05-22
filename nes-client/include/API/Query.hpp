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

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <API/Functions/Functions.hpp>
#include <API/Windowing.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Types/WindowType.hpp>

namespace NES
{
class Query;

namespace JoinOperatorBuilder
{
class JoinWhere;
}

namespace WindowOperatorBuilder
{
class WindowedQuery;
class KeyedWindowedQuery;
}

static constexpr uint64_t defaultTriggerTimeInMs = 1000;

namespace JoinOperatorBuilder
{
class Join
{
public:
    /**
     * @brief Constructor. Initialises always subQueryRhs and original Query
     * @param subQueryRhs
     * @param originalQuery
     */
    Join(const Query& subQueryRhs, Query& originalQuery);

    /**
     * @brief is called to append all joinFunctions (key predicates) to the previous defined join, i.e.,
     * it sets all condition for the join matches
     * @param joinFunction : a set of binary functions to compare left and right tuples
     * @return object of type JoinWhere on which window function is defined and can be called.
     */
    [[nodiscard]] JoinWhere where(std::shared_ptr<NodeFunction> joinFunction) const;

private:
    const Query& subQueryRhs;
    Query& originalQuery;
};

class JoinWhere
{
public:
    /**
     * @brief Constructor. Initialises always subQueryRhs, original Query and the joinFunction
     * @param subQueryRhs
     * @param originalQuery
     * @param joinFunction : a set of binary functions to compare left and right tuples
     */
    JoinWhere(const Query& subQueryRhs, Query& originalQuery, std::shared_ptr<NodeFunction> joinFunctions);

    /**
     * @brief: calls internal the original joinWith function with all the gathered parameters.
     * @param windowType
     * @return the query with the result of the original joinWith function is returned.
     */
    [[nodiscard]] Query& window(const std::shared_ptr<Windowing::WindowType>& windowType) const;

private:
    const Query& subQueryRhs;
    Query& originalQuery;
    std::shared_ptr<NodeFunction> joinFunctions;
};

}

namespace CEPOperatorBuilder
{

class And
{
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
    [[nodiscard]] Query& window(const std::shared_ptr<Windowing::WindowType>& windowType) const;

private:
    Query& subQueryRhs;
    Query& originalQuery;
    std::shared_ptr<NodeFunction> joinFunction;
};

class Seq
{
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
    [[nodiscard]] Query& window(const std::shared_ptr<Windowing::WindowType>& windowType) const;

private:
    Query& subQueryRhs;
    Query& originalQuery;
    std::shared_ptr<NodeFunction> joinFunction;
};

/**
     * @brief: This operator is a CEP operator, in CEP engines also called iteration operator. It
     * allows for multiple occurrences of a specified event, i.e., tuples.
     * Thus, 'times' enables patterns of arbitrary length (when only minOccurrences are defined) or
     * requires a specified number of tuples (minOccurrence, maxOccurrence) to occur
     * The Times operator requires the call of the window operator afterwards
     * @return cepBuilder
     */

class Times
{
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
    [[nodiscard]] Query& window(const std::shared_ptr<Windowing::WindowType>& windowType) const;

private:
    Query& originalQuery;
    uint64_t minOccurrences;
    uint64_t maxOccurrences;
    bool bounded;
};

///TODO this method is a quick fix to generate unique keys for andWith chains and should be removed after implementation of Cartesian Product (#2296)
/**
     * @brief: this function creates a virtual key for the left side of the binary operator
     * @param keyName the attribute name
     * @return the unique name of the key
     */
std::string keyAssignment(std::string keyName);

}

/**
 * User interface to create stream processing queryIdAndCatalogEntryMapping.
 * The current api exposes method to create queryIdAndCatalogEntryMapping using all currently supported operators.
 */
class Query
{
public:
    Query(const Query&);

    virtual ~Query() = default;

    ///both, Join and CEPOperatorBuilder friend classes, are required as they use the private joinWith method.
    friend class JoinOperatorBuilder::JoinWhere;
    friend class CEPOperatorBuilder::And;
    friend class CEPOperatorBuilder::Seq;
    friend class WindowOperatorBuilder::WindowedQuery;
    friend class WindowOperatorBuilder::KeyedWindowedQuery;

    WindowOperatorBuilder::WindowedQuery window(const std::shared_ptr<Windowing::WindowType>& windowType);

    /**
     * @brief can be called on the original query with the query to be joined with and sets this query in the class Join.
     * @param subQueryRhs
     * @return object where where() function is defined and can be called by user
     */
    JoinOperatorBuilder::Join joinWith(const Query& subQueryRhs);

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

    /// Creates a query from a particular source. The source is identified by its name.
    /// During query processing the underlying source descriptor is retrieved from the source catalog.
    static Query from(const std::string& logicalSourceName);

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
    template <typename... Args>
    auto project(Args&&... args) -> std::enable_if_t<std::conjunction_v<std::is_constructible<FunctionItem, Args>...>, Query&>
    {
        return project({std::forward<Args>(args).getNodeFunction()...});
    }

    /**
      * @brief this call projects out the attributes in the parameter list
      * @param attribute list
      * @return the query
      */
    Query& project(const std::vector<std::shared_ptr<NodeFunction>>& functions);

    /**
     * @brief: Filter records according to the predicate. An
     * examplary usage would be: filter(Attribute("f1" < 10))
     * @param predicate as function node
     * @return the query
     */
    Query& selection(const std::shared_ptr<NodeFunction>& filterFunction);

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
    Query& assignWatermark(const std::shared_ptr<Windowing::WatermarkStrategyDescriptor>& watermarkStrategyDescriptor);

    /**
     * @brief: Map records according to a map function. An
     * examplary usage would be: map(Attribute("f2") = Attribute("f1") * 42 )
     * @param map function
     * @return query
     */
    Query& map(const std::shared_ptr<NodeFunctionFieldAssignment>& mapFunction);

    /// Add a sink operator to the query plan that contains a SinkName. In a later step, we look up all sinks that registered using that SinkName
    /// and replace the operator containing only the sink name with operators containing the concrete descriptor of the sink.
    virtual Query& sink(std::string sinkName, WorkerId workerId = INVALID_WORKER_NODE_ID);

    [[nodiscard]] std::shared_ptr<QueryPlan> getQueryPlan() const;

    /// creates a new query object
    explicit Query(std::shared_ptr<QueryPlan> queryPlan);

protected:
    /// query plan containing the operators.
    std::shared_ptr<QueryPlan> queryPlan;

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
    Query& joinWith(
        const Query& subQueryRhs,
        const std::shared_ptr<NodeFunction>& joinFunction,
        const std::shared_ptr<Windowing::WindowType>& windowType);

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
    Query& andWith(
        const Query& subQueryRhs,
        const std::shared_ptr<NodeFunction>& joinFunctions,
        const std::shared_ptr<Windowing::WindowType>& windowType);

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
    Query& seqWith(
        const Query& subQueryRhs,
        const std::shared_ptr<NodeFunction>& joinFunctions,
        const std::shared_ptr<Windowing::WindowType>& windowType);

    /**
     * @new change: similar to join, the original window and windowByKey become private --> only internal use
     * @brief: Creates a window aggregation.
     * @param windowType Window definition.
     * @param aggregations Window aggregation function.
     * @return query.
     */
    Query&
    window(const std::shared_ptr<Windowing::WindowType>& windowType, std::vector<std::shared_ptr<API::WindowAggregation>> aggregations);


    Query& windowByKey(
        std::vector<std::shared_ptr<NodeFunction>> keys,
        const std::shared_ptr<Windowing::WindowType>& windowType,
        std::vector<std::shared_ptr<API::WindowAggregation>> aggregations);

    /**
      * @brief: Given a Function is identifies which JoinType has to be used for processing, i.e., Equi-Join enables
      * different Join algorithms, while all other join types right lead to the NestedLoopJoin
      * @param joinFunctions key functions
      * @return joinType
      */
    static Join::LogicalJoinDescriptor::JoinType identifyJoinType(const std::shared_ptr<NodeFunction>& joinFunctions);
};


}
