#include <memory>
#include <API/Window/WindowType.hpp>
#include <API/Window/WindowMeasure.hpp>
#include <vector>
#include <Util/Logger.hpp>

namespace iotdb {

TumblingWindow::TumblingWindow(iotdb::TimeMeasure size) : _size(size) {}

WindowTypePtr TumblingWindow::of(iotdb::TimeMeasure size) {
  return std::make_shared<TumblingWindow>(TumblingWindow(size));
}

void TumblingWindow::triggerWindows(iotdb::WindowListPtr windows,
                                    uint64_t lastWatermark,
                                    uint64_t currentWatermark) const {
  long lastStart = lastWatermark - ((lastWatermark + _size.getTime()) % _size.getTime());
  for (long windowStart = lastStart; windowStart + _size.getTime() <= currentWatermark; windowStart += _size.getTime()) {
    windows->push_back(WindowState(windowStart, windowStart + _size.getTime()));
  }
}


SlidingWindow::SlidingWindow(iotdb::TimeMeasure size, iotdb::TimeMeasure slide) : _size(size), _slide(slide) {}

WindowTypePtr SlidingWindow::of(iotdb::TimeMeasure size, TimeMeasure slide) {
  return std::make_shared<SlidingWindow>(SlidingWindow(size, slide));
}

void SlidingWindow::triggerWindows(iotdb::WindowListPtr windows,
                                    uint64_t lastWatermark,
                                    uint64_t currentWatermark) const {
  IOTDB_NOT_IMPLEMENTED
}


SessionWindow::SessionWindow(iotdb::TimeMeasure gap) : _gap(gap) {}

WindowTypePtr SessionWindow::withGap(iotdb::TimeMeasure gap) {
  return std::make_shared<SessionWindow>(SessionWindow(gap));
}

void SessionWindow::triggerWindows(iotdb::WindowListPtr windows,
                                   uint64_t lastWatermark,
                                   uint64_t currentWatermark) const {
  IOTDB_NOT_IMPLEMENTED
}

WindowState::WindowState(uint64_t start, uint64_t an_end) : start(start), end(an_end) {}

uint64_t WindowState::getStartTs() const {
  return start;
}
uint64_t WindowState::getEndTs() const {
  return end;
}
}
