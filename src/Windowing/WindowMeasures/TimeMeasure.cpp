#include <Windowing/WindowMeasures/TimeMeasure.hpp>

namespace NES {
TimeMeasure::TimeMeasure(uint64_t ms) : ms(ms) {}

uint64_t TimeMeasure::getTime() const { return ms; }

TimeMeasure Milliseconds(uint64_t milliseconds) { return TimeMeasure(milliseconds); }

TimeMeasure Seconds(uint64_t seconds) { return Milliseconds(seconds * 1000); }

TimeMeasure Minutes(uint64_t minutes) { return Seconds(minutes * 60); }

}// namespace NES