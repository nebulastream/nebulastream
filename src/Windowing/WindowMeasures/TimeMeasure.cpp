#include <Windowing/WindowMeasures/TimeMeasure.hpp>

namespace NES {
TimeMeasure::TimeMeasure(uint64_t ms) : ms(ms) {}

uint64_t TimeMeasure::getTime() const { return ms; }



}// namespace NES