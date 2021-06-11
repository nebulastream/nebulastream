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

#ifndef NES_SRC_API_WINDOWEDQUERY_H_
#define NES_SRC_API_WINDOWEDQUERY_H_

#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/Schema.hpp>
#include <API/Windowing.hpp>

#include <API/Expressions/Expressions.hpp>
#include <API/WindowedQuery.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Sources/DataSource.hpp>

#ifdef ENABLE_KAFKA_BUILD
#include <cppkafka/configuration.h>
#endif// KAFKASINK_HPP
#include <string>

namespace NES {
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
    [[nodiscard]] KeyedWindowedQuery byKey(const ExpressionItem& onKey) const;

    /**
   * @brief: Calls internally the original window() function and returns the Query&
   * @param aggregation
   */
    Query& apply(Windowing::WindowAggregationPtr& aggregation);

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
    KeyedWindowedQuery(Query& originalQuery, Windowing::WindowTypePtr windowType, const ExpressionItem& onKey);

    /**
    * @brief: Calls internally the original windowByKey() function and returns the Query&
    * @param aggregation
    */
    Query& apply(Windowing::WindowAggregationPtr aggregation);

  private:
    Query& originalQuery;
    Windowing::WindowTypePtr windowType;
    ExpressionItem onKey;
};

}// namespace WindowOperatorBuilder
}// namespace NES

#endif//NES_SRC_API_WINDOWEDQUERY_H_
