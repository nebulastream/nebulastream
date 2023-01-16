//
// Created by kasper on 1/16/23.
//

#ifndef NES_DECORATEDLORAWANNODEENGINE_HPP
#define NES_DECORATEDLORAWANNODEENGINE_HPP

#include <Runtime/NodeEngine.hpp>

namespace NES::Runtime {

class DecoratedLoRaWANNodeEngine: public NodeEngine
{
    /**
     * @brief registers a query
     * TODO: since this doesn't override it only hides the function from the parent class.
     * This means any internal call from the parent class will refer to the parent method. not the child.
     * Possibly not an issue
     * @param queryId: id of the query sub plan to be registered
     * @param queryExecutionId: query execution plan id
     * @param operatorTree: query sub plan to register
     * @return true if succeeded, else false
     */
    [[nodiscard]] bool registerQueryInNodeEngine(const QueryPlanPtr& queryPlan);
};

}

#endif//NES_DECORATEDLORAWANNODEENGINE_HPP
