#include <Monitoring/Metrics/IntCounter.hpp>
#include <Util/Logger.hpp>

namespace NES {

IntCounter::IntCounter(uint64_t initCount) : count(initCount) {
    NES_DEBUG("Counter: Initializing with count " << initCount);
}

void IntCounter::inc() {
    count++;
}

uint64_t IntCounter::inc(uint64_t val) {
    count += val;
    return count;
}

void IntCounter::dec() {
    if (count > 0) {
        count--;
    }
}

uint64_t IntCounter::dec(uint64_t val) {
    if (count > val) {
        count -= val;
    }
    else {
        count = 0;
    }
    return count;
}

uint64_t IntCounter::getCount() const {
    return count;
}
}