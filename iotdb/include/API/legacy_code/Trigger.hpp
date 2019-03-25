#ifndef API_TRIGGER_H
#define API_TRIGGER_H

#include "../CodeGen/CodeGen.hpp"
#include "../Operators/Operator.hpp"
#include "API/Time.hpp"

namespace iotdb {

class Trigger {

  public:
    bool purge = false;
    virtual std::string to_string() { return "trigger"; };
};

class CountTrigger : public Trigger {
  public:
    CountTrigger(size_t maxCount) : maxCount(maxCount){};
    std::string to_string() override { return "CountTrigger"; }

  private:
    size_t maxCount;
};

class ProcessingTimeTrigger : public Trigger {
  public:
    ProcessingTimeTrigger(Time every) : every(every) {}

    std::string to_string() override { return "ProcessingTimeTrigger"; }

  private:
    Time every;
};

class EventTimeTrigger : public Trigger {
  public:
    EventTimeTrigger(Time every, std::string tsFieldId, Time allowedLateness)
        : every(every), timestampFieldId(tsFieldId), allowedLateness(allowedLateness), merge(Time::seconds(0))
    {
    }
    EventTimeTrigger(Time every, std::string tsFieldId, Time allowedLateness, Time merge)
        : every(every), timestampFieldId(tsFieldId), allowedLateness(allowedLateness), merge(merge)
    {
    }

    std::string to_string() override { return "EventTimeTrigger"; }

  private:
    Time every;
    std::string timestampFieldId;
    Time allowedLateness;
    Time merge;
};

class PurgingTrigger : public Trigger {
  public:
    PurgingTrigger(Trigger* trigger) : trigger(trigger) {}
    PurgingTrigger(Trigger&& trigger) : trigger(&trigger) {}

    std::string to_string() override { return "PurgingTrigger"; }

  private:
    Trigger* trigger;
};
} // namespace iotdb
#endif // APPI_TRIGGER_H
