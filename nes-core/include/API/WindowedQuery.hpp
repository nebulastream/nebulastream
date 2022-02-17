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

#ifndef NES_INCLUDE_API_WINDOWEDQUERY_HPP_
#define NES_INCLUDE_API_WINDOWEDQUERY_HPP_

#ifdef ENABLE_KAFKA_BUILD
#include <cppkafka/configuration.h>
#endif// KAFKASINK_HPP
#include <string>
namespace NES {

class Query;
class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class ExpressionItem;

class ExpressionNode;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;

class FieldAssignmentExpressionNode;
using FieldAssignmentExpressionNodePtr = std::shared_ptr<FieldAssignmentExpressionNode>;

namespace WindowOperatorBuilder {

class WindowedQuery;
class KeyedWindowedQuery;

/**
 * @brief A fragment of the query, which is windowed according to a window type and specific keys.
 */
class KeyedWindowedQuery {
  public:
    /**
    * @brief: Constructor. Initialises always originalQuery, windowType, onKey
    * @param originalQuery
    * @param windowType
    */
    KeyedWindowedQuery(Query& originalQuery, Windowing::WindowTypePtr windowType, std::vector<ExpressionNodePtr> keys);

    /**
    * @brief: Applies a set of aggregation functions to the window and returns a query object.
    * @param aggregations list of aggregation functions.
    * @return Query
    */
    template<class... WindowAggregations>
    [[nodiscard]] Query& apply(WindowAggregations... aggregations) {
        std::vector<Windowing::WindowAggregationPtr> windowAggregations;
        (windowAggregations.push_back(std::forward<Windowing::WindowAggregationPtr>(aggregations)), ...);
        return originalQuery.windowByKey(keys, windowType, windowAggregations);
    }

  private:
    Query& originalQuery;
    Windowing::WindowTypePtr windowType;
    std::vector<ExpressionNodePtr> keys;
};

/**
 * @brief A fragment of the query, which is windowed according to a window type.
 */
class WindowedQuery {
  public:
    /**
    * @brief: Constructor. Initialises always originalQuery, windowType
    * @param originalQuery
    * @param windowType
    */
    WindowedQuery(Query& originalQuery, Windowing::WindowTypePtr windowType);

    /**
    * @brief: Sets attributes for the keyBy Operation. For example `byKey(Attribute("x"), Attribute("y")))`
    * Creates a KeyedWindowedQuery object.
    * @param onKeys list of keys
    * @return KeyedWindowedQuery
    */
    template<class... ExpressionItems>
    [[nodiscard]] KeyedWindowedQuery byKey(ExpressionItems... onKeys) {
        std::vector<ExpressionNodePtr> keyExpressions;
        (keyExpressions.push_back(std::forward<ExpressionItems>(onKeys).getExpressionNode()), ...);
        return KeyedWindowedQuery(originalQuery, windowType, keyExpressions);
    };

    /**
    * @brief: Applies a set of aggregation functions to the window and returns a query object.
    * @param aggregations list of aggregation functions.
    * @return Query
    */
    template<class... WindowAggregations>
    [[nodiscard]] Query& apply(WindowAggregations... aggregations) {
        std::vector<Windowing::WindowAggregationPtr> windowAggregations;
        (windowAggregations.push_back(std::forward<Windowing::WindowAggregationPtr>(aggregations)), ...);
        return originalQuery.window(windowType, windowAggregations);
    }

  private:
    Query& originalQuery;
    Windowing::WindowTypePtr windowType;
};

}// namespace WindowOperatorBuilder
}// namespace NES

#endif// NES_INCLUDE_API_WINDOWEDQUERY_HPP_
