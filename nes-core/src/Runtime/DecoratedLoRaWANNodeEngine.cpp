//
// Created by kasper on 1/16/23.
//

#include <Runtime/DecoratedLoRaWANNodeEngine.hpp>
namespace NES::Runtime {
bool DecoratedLoRaWANNodeEngine::registerQueryInNodeEngine(const NES::QueryPlanPtr& queryPlan) {
    return NodeEngine::registerQueryInNodeEngine(queryPlan);
}
}

