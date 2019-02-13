#ifndef API_WINDOW_H
#define API_WINDOW_H

#include <string>

#include "API/Assigner.hpp"
#include "API/Time.hpp"
#include "API/Trigger.hpp"

namespace iotdb{
class Counter {
public:
  Counter(size_t max) : max(max) {}

  size_t max;

private:
};

class Window {
public:
  Assigner *assigner;
  Trigger *trigger;

  Window withTrigger(Trigger &&trigger) {
    this->trigger = &trigger;
    return *this;
  }
};

class TumblingProcessingTimeWindow : public Window {
public:
  TumblingProcessingTimeWindow(Time size) {
    assigner = new TumblingProcessingTimeAssigner();
    trigger = new PurgingTrigger(new ProcessingTimeTrigger(size));
  }

  TumblingProcessingTimeWindow(Counter size) {
    assigner = new TumblingProcessingTimeAssigner();
    trigger = new PurgingTrigger(new CountTrigger(size.max));
  }
};

class SlidingProcessingTimeWindow : public Window {
public:
  SlidingProcessingTimeWindow(Time size, Time slide) {
    assigner = new SlidingProcessingTimeAssigner(size, slide);
    trigger = new PurgingTrigger(new ProcessingTimeTrigger(slide));
  }
};

class SessionProcessingTimeWindow : public Window {
public:
  SessionProcessingTimeWindow(Time timeout) {
    assigner = new SessionProcessingTimeAssigner(timeout);
    trigger = new PurgingTrigger(
        new ProcessingTimeTrigger(Time::seconds(365 * 24 * 60 * 60))); // maximum session timeout of one year
  }
};

class TumblingEventTimeWindow : public Window {
public:
  TumblingEventTimeWindow(Time size, std::string timestampFieldId) : size(size), timestampFieldId(timestampFieldId) {
    assigner = new TumblingEventTimeAssigner(size, timestampFieldId, Time::seconds(0));
    trigger = new PurgingTrigger(new EventTimeTrigger(size, timestampFieldId, Time::seconds(0)));
  }

  TumblingEventTimeWindow withAllowedLateness(Time lateness) {
    assigner = new TumblingEventTimeAssigner(size, timestampFieldId, lateness);
    trigger = new PurgingTrigger(new EventTimeTrigger(size, timestampFieldId, lateness));
    return *this;
  }

private:
  Time size;
  std::string timestampFieldId;
};

class SlidingEventTimeWindow : public Window {
public:
  SlidingEventTimeWindow(Time size, Time slide, std::string timestampFieldId)
      : size(size), slide(slide), timestampFieldId(timestampFieldId) {
    assigner = new SlidingEventTimeAssigner(size, slide, timestampFieldId, Time::seconds(0));
    trigger = new PurgingTrigger(new EventTimeTrigger(size, timestampFieldId, Time::seconds(0)));
  }

  SlidingEventTimeWindow withAllowedLateness(Time lateness) {
    assigner = new SlidingEventTimeAssigner(size, slide, timestampFieldId, lateness);
    trigger = new PurgingTrigger(new EventTimeTrigger(slide, timestampFieldId, lateness));
    return *this;
  }

private:
  Time size;
  Time slide;
  std::string timestampFieldId;
};

class SessionEventTimeWindow : public Window {
public:
  SessionEventTimeWindow(Time timeout, std::string timestampFieldId)
      : timeout(timeout), timestampFieldId(timestampFieldId) {
    assigner = new SessionEventTimeAssigner(timeout, timestampFieldId, Time::seconds(0));
    trigger = new PurgingTrigger(new EventTimeTrigger(timeout, timestampFieldId, Time::seconds(0), timeout));
  }

  SessionEventTimeWindow withAllowedLateness(Time lateness) {
    assigner = new SessionEventTimeAssigner(timeout, timestampFieldId, lateness);
    trigger = new PurgingTrigger(new EventTimeTrigger(timeout, timestampFieldId, lateness, timeout));
    return *this;
  }

private:
  Time timeout;
  std::string timestampFieldId;
};
}
#endif // API_WINDOW_H
