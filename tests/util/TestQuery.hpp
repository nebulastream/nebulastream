#ifndef NES_TESTS_UTIL_TESTQUERY_HPP_
#define NES_TESTS_UTIL_TESTQUERY_HPP_

#include <API/Query.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include "SchemaSourceDescriptor.hpp"
namespace NES{

/**
 * @brief TestQuery api for testing.
 */
class TestQuery : public Query{
  public:
    static Query from(SchemaPtr inputSchme){
        auto sourceOperator = LogicalOperatorFactory::createSourceOperator(SchemaSourceDescriptor::create(inputSchme));
        auto queryPlan = QueryPlan::create(sourceOperator);
        return Query(queryPlan);
    }
};


}

#endif//NES_TESTS_UTIL_TESTQUERY_HPP_
