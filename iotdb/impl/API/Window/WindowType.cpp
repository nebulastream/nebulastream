#include <memory>
#include <API/Window/WindowType.hpp>
#include <API/Window/WindowMeasure.hpp>

namespace iotdb {

TumblingWindow::TumblingWindow(iotdb::TimeMeasure size) : _size(size) {}

WindowTypePtr TumblingWindow::of(iotdb::TimeMeasure size) {
  return std::make_shared<TumblingWindow>(TumblingWindow(size));
}

SlidingWindow::SlidingWindow(iotdb::TimeMeasure size, iotdb::TimeMeasure slide) : _size(size), _slide(slide) {}

WindowTypePtr SlidingWindow::of(iotdb::TimeMeasure size, TimeMeasure slide) {
  return std::make_shared<SlidingWindow>(SlidingWindow(size, slide));
}

SessionWindow::SessionWindow(iotdb::TimeMeasure gap) : _gap(gap) {}

WindowTypePtr SessionWindow::withGap(iotdb::TimeMeasure gap) {
  return std::make_shared<SessionWindow>(SessionWindow(gap));
}
}