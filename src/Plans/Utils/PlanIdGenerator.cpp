#include <Plans/Utils/PlanIdGenerator.hpp>
#include <atomic>

namespace NES{

QueryId PlanIdGenerator::getNextQueryId() {
    static std::atomic_uint64_t id = 0;
    return ++id;
}

GlobalQueryId PlanIdGenerator::getNextGlobalQueryId() {
    static std::atomic_uint64_t id = 0;
    return ++id;
}

uint64_t PlanIdGenerator::getNextQuerySubPlanId() {
    static std::atomic_uint64_t id = 0;
    return ++id;
}

}

