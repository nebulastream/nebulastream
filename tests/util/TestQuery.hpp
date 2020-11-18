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

#ifndef NES_TESTS_UTIL_TESTQUERY_HPP_
#define NES_TESTS_UTIL_TESTQUERY_HPP_

#include "SchemaSourceDescriptor.hpp"
#include <API/Query.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
namespace NES {

/**
 * @brief TestQuery api for testing.
 */
class TestQuery : public Query {
  public:
    static Query from(SchemaPtr inputSchme) {
        auto sourceOperator = LogicalOperatorFactory::createSourceOperator(SchemaSourceDescriptor::create(inputSchme));
        auto queryPlan = QueryPlan::create(sourceOperator);
        return Query(queryPlan);
    }
};

}// namespace NES

#endif//NES_TESTS_UTIL_TESTQUERY_HPP_
