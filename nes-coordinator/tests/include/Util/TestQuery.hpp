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

#ifndef NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTQUERY_HPP_
#define NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTQUERY_HPP_

#include <API/Query.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/SchemaSourceDescriptor.hpp>
#include <utility>
namespace NES {

/**
 * @brief TestQuery api for testing.
 */
class TestQuery : public Query {
  public:
    /**
     * @brief Creates a query from a SourceDescriptor
     * @param descriptor
     * @return Query
     */
    static Query from(const SourceDescriptorPtr& descriptor) {
        auto sourceOperator = LogicalOperatorFactory::createSourceOperator(descriptor);
        auto queryPlan = QueryPlan::create(sourceOperator);
        return {queryPlan};
    }

    /**
     * @brief Creates a query from an input schema
     * @param inputSchema
     * @return Query
     */
    static Query from(const SchemaPtr& inputSchema) {
        auto sourceOperator =
            LogicalOperatorFactory::createSourceOperator(SchemaSourceDescriptor::create(std::move(inputSchema)));
        auto queryPlan = QueryPlan::create(sourceOperator);
        return {queryPlan};
    }
};

}// namespace NES

#endif// NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTQUERY_HPP_
