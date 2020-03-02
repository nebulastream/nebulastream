#include <memory>
#include <API/Window/WindowType.hpp>
#include <API/Window/WindowMeasure.hpp>
#include <Util/Logger.hpp>

namespace NES {

TimeType WindowType::getWindowTimeType() const {
    return this->timeType;
}

TumblingWindow::TumblingWindow(TimeType timeType, TimeMeasure size) : size(size) {}

WindowTypePtr TumblingWindow::of(TimeType timeType, TimeMeasure size) {
  return std::make_shared<TumblingWindow>(TumblingWindow(timeType, size));
}

void TumblingWindow::triggerWindows(WindowListPtr windows,
                                    uint64_t lastWatermark,
                                    uint64_t currentWatermark) const {
  long lastStart = lastWatermark - ((lastWatermark + size.getTime()) % size.getTime());
  for (long windowStart = lastStart; windowStart + size.getTime() <= currentWatermark; windowStart += size.getTime()) {
    windows->push_back(WindowState(windowStart, windowStart + size.getTime()));
  }
}


SlidingWindow::SlidingWindow(TimeType timeType,  TimeMeasure size, TimeMeasure slide) : size(size), slide(slide) {}

WindowTypePtr SlidingWindow::of(TimeType timeType, TimeMeasure size, TimeMeasure slide) {
  return std::make_shared<SlidingWindow>(SlidingWindow(timeType, size, slide));
}

void SlidingWindow::triggerWindows(WindowListPtr windows,
                                    uint64_t lastWatermark,
                                    uint64_t currentWatermark) const {
  NES_NOT_IMPLEMENTED
}


SessionWindow::SessionWindow(TimeType timeType, TimeMeasure gap) : gap(gap) {}

WindowTypePtr SessionWindow::withGap(TimeType timeType, TimeMeasure gap) {
  return std::make_shared<SessionWindow>(SessionWindow(timeType, gap));
}

void SessionWindow::triggerWindows(WindowListPtr windows,
                                   uint64_t lastWatermark,
                                   uint64_t currentWatermark) const {
  NES_NOT_IMPLEMENTED
}

WindowState::WindowState(uint64_t start, uint64_t an_end) : start(start), end(an_end) {}

uint64_t WindowState::getStartTs() const {
  return start;
}
uint64_t WindowState::getEndTs() const {
  return end;
}
}
