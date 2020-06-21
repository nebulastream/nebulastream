#include <Monitoring/Metrics/Counter.hpp>
#include <Util/Logger.hpp>

namespace NES {

Counter::Counter(int64_t initCount) : Metric(MetricType::CounterType), count(initCount) {
    NES_DEBUG("Counter: Initializing with count " << initCount);
}

void Counter::inc() {
    count++;
}

void Counter::inc(int64_t n) {
    count += n;
}

void Counter::dec() {
    count--;
}

void Counter::dec(int64_t n) {
    count -= n;
}

int64_t Counter::getCount() const {
    return count;
}

}