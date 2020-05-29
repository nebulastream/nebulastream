#include <API/Window/WindowMeasure.hpp>
#include <iostream>

namespace NES {

TimeMeasure::TimeMeasure(uint64_t ms) : _ms(ms) {}

u_int64_t TimeMeasure::getTime() const { return _ms; }

Milliseconds::Milliseconds(uint64_t milliseconds) : TimeMeasure(milliseconds) {}

Seconds::Seconds(uint64_t seconds) : Milliseconds(seconds * 1000) {}

Minutes::Minutes(uint64_t minutes) : Seconds(minutes * 60) {}

Hours::Hours(uint64_t hours) : Minutes(hours * 60) {}

}// namespace NES
