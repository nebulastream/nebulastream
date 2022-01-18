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

class WindowedQuery {
  public:
    /**
    * @brief: Constructor. Initialises always originalQuery, windowType
    * @param originalQuery
    * @param windowType
    */
    WindowedQuery(Query& originalQuery, Windowing::WindowTypePtr windowType);

    /**
    * @brief: sets the Attribute for the keyBy Operation. Creates a KeyedWindowedQuery object.
    * @param onKey
    */
    [[nodiscard]] KeyedWindowedQuery byKey(ExpressionItem onKey) const;
    [[nodiscard]] KeyedWindowedQuery byKey(std::initializer_list<ExpressionItem> onKey) const;

    /**
   * @brief: Calls internally the original window() function and returns the Query&
   * @param aggregation
   */
    Query& apply(Windowing::WindowAggregationPtr const& aggregation);

  private:
    Query& originalQuery;
    Windowing::WindowTypePtr windowType;
};

class KeyedWindowedQuery {
  public:
    /**
    * @brief: Constructor. Initialises always originalQuery, windowType, onKey
    * @param originalQuery
    * @param windowType
    */
    KeyedWindowedQuery(Query& originalQuery, Windowing::WindowTypePtr windowType, std::vector<ExpressionNodePtr> onKey);

    /**
    * @brief: Calls internally the original windowByKey() function and returns the Query&
    * @param aggregation
    */
    Query& apply(Windowing::WindowAggregationPtr aggregation);

  private:
    Query& originalQuery;
    Windowing::WindowTypePtr windowType;
    std::vector<ExpressionNodePtr> onKey;
};

}// namespace WindowOperatorBuilder
}// namespace NES

#endif  // NES_INCLUDE_API_WINDOWEDQUERY_HPP_
