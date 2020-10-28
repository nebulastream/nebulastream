#include <Plans/utils/IdGenerator.hpp>
#include <atomic>

namespace NES{

QueryId IdGenerator::getNextQueryId() {
    static std::atomic_uint64_t id = 0;
    return ++id;
}

GlobalQueryId IdGenerator::getNextGlobalQueryId() {
    static std::atomic_uint64_t id = 0;
    return ++id;
}

uint64_t IdGenerator::getNextQuerySubPlanId() {
    static std::atomic_uint64_t id = 0;
    return ++id;
}

}

