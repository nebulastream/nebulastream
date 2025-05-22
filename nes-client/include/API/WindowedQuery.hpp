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
#include <API/Query.hpp>
#include <API/Windowing.hpp>
#include <Functions/NodeFunction.hpp>
#include <Types/WindowType.hpp>

namespace NES::WindowOperatorBuilder
{
/**
 * @brief A fragment of the query, which is windowed according to a window type and specific keys.
 */
class KeyedWindowedQuery
{
public:
    /**
    * @brief: Constructor. Initialises always originalQuery, windowType, onKey
    * @param originalQuery
    * @param windowType
    */
    KeyedWindowedQuery(
        Query& originalQuery, std::shared_ptr<Windowing::WindowType> windowType, std::vector<std::shared_ptr<NodeFunction>> keys);

    /**
    * @brief: Applies a set of aggregation functions to the window and returns a query object.
    * @param aggregations list of aggregation functions.
    * @return Query
    */
    template <class... WindowAggregations>
    [[nodiscard]] Query& apply(WindowAggregations... aggregations)
    {
        std::vector<std::shared_ptr<API::WindowAggregation>> windowAggregations;
        (windowAggregations.emplace_back(std::forward<std::shared_ptr<API::WindowAggregation>>(aggregations)), ...);
        return originalQuery.windowByKey(keys, windowType, windowAggregations);
    }

private:
    Query& originalQuery;
    std::shared_ptr<Windowing::WindowType> windowType;
    std::vector<std::shared_ptr<NodeFunction>> keys;
};

/**
 * @brief A fragment of the query, which is windowed according to a window type.
 */
class WindowedQuery
{
public:
    /**
    * @brief: Constructor. Initialises always originalQuery, windowType
    * @param originalQuery
    * @param windowType
    */
    WindowedQuery(Query& originalQuery, std::shared_ptr<Windowing::WindowType> windowType);

    /**
    * @brief: Sets attributes for the keyBy Operation. For example `byKey(Attribute("x"), Attribute("y")))`
    * Creates a KeyedWindowedQuery object.
    * @param onKeys list of keys
    * @return KeyedWindowedQuery
    */
    template <class... FunctionItems>
    [[nodiscard]] KeyedWindowedQuery byKey(FunctionItems... onKeys)
    {
        std::vector<std::shared_ptr<NodeFunction>> keyFunctions;
        (keyFunctions.emplace_back(std::forward<FunctionItems>(onKeys).getNodeFunction()), ...);
        return KeyedWindowedQuery(originalQuery, windowType, keyFunctions);
    };

    /**
    * @brief: Applies a set of aggregation functions to the window and returns a query object.
    * @param aggregations list of aggregation functions.
    * @return Query
    */
    template <class... WindowAggregations>
    [[nodiscard]] Query& apply(WindowAggregations... aggregations)
    {
        std::vector<std::shared_ptr<API::WindowAggregation>> windowAggregations;
        (windowAggregations.emplace_back(std::forward<std::shared_ptr<API::WindowAggregation>>(aggregations)), ...);
        return originalQuery.window(windowType, windowAggregations);
    }

private:
    Query& originalQuery;
    std::shared_ptr<Windowing::WindowType> windowType;
};

}
