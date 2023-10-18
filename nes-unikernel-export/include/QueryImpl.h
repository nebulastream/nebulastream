//
// Created by ls on 29.09.23.
//

#ifndef NES_QUERYIMPL_H
#define NES_QUERYIMPL_H
#include "Operators/LogicalOperators/Sources/NoOpSourceDescriptor.h"
#include <API/Query.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES {
/**
 * @brief TestQuery api for testing.
 */
class QueryImpl : public Query {

  public:
    QueryPlanPtr qp;

    explicit QueryImpl(QueryPlanPtr queryPlan) : Query(queryPlan), qp(std::move(queryPlan)) {}

    /**
     * @brief Creates a query from a SourceDescriptor
     * @param descriptor
     * @return Query
     */
    static QueryImpl from() {
        auto sourceOperator = LogicalOperatorFactory::createSourceOperator(std::make_shared<NoOpSourceDescriptor>( Schema::create(), ""));
        auto queryPlan = QueryPlan::create();
        return QueryImpl(queryPlan);
    }
};
}// namespace NES
#endif//NES_QUERYIMPL_H
