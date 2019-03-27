#ifndef API_ASSIGNER_H
#define API_ASSIGNER_H

#include <cmath>

#include "../CodeGen/CodeGen.hpp"
#include "../Operators/Operator.hpp"
#include "Time.hpp"

namespace iotdb {
class Assigner {

  public:
    virtual std::string to_string() { return "Assigner"; };

  protected:
    size_t pipeline;
};

class TumblingProcessingTimeAssigner : public Assigner {
  public:
    std::string to_string() override { return "TumblingProcessingTimeAssigner"; }
};

class SlidingProcessingTimeAssigner : public Assigner {
  public:
    SlidingProcessingTimeAssigner(Time size, Time slide) : size(size), slide(slide)
    {
        if (size.time % slide.time != 0)
            throw std::invalid_argument("Invalid window: size of window must be a multiple of slide");
        numWindows = (size.time / slide.time) * 2;
    }
    std::string to_string() override { return "SlidingProcessingTimeAssigner"; }

  private:
    Time size;
    Time slide;
    size_t numWindows;
};

class SessionProcessingTimeAssigner : public Assigner {
  public:
    SessionProcessingTimeAssigner(Time timeout) : timeout(timeout) {}
    std::string to_string() override { return "SessionProcessingTimeAssigner"; }

  private:
    Time timeout;
};

class TumblingEventTimeAssigner : public Assigner {
  public:
    TumblingEventTimeAssigner(Time size, std::string tsFieldId, Time al)
        : size(size), timestampFieldId(tsFieldId), allowedLateness(al)
    {
        if (allowedLateness.time % size.time != 0)
            throw std::invalid_argument("Invalid window: allowed lateness must be a multiple of size");
        numWindows = (allowedLateness.time / size.time) + 2;
    }
    std::string to_string() override { return "TumblingEventTimeAssigner"; }

  private:
    Time size;
    std::string timestampFieldId;
    Time allowedLateness;
    size_t numWindows;
};

class SlidingEventTimeAssigner : public Assigner {
  public:
    SlidingEventTimeAssigner(Time size, Time slide, std::string tsFieldId, Time al)
        : size(size), slide(slide), timestampFieldId(tsFieldId), allowedLateness(al)
    {
        if (size.time % slide.time != 0)
            throw std::invalid_argument("Invalid window: size of window must be a multiple of slide");
        numWindows = (allowedLateness.time / slide.time) + 2;
    }
    std::string to_string() override { return "SlidingEventTimeAssigner"; }

  private:
    Time size;
    Time slide;
    std::string timestampFieldId;
    Time allowedLateness;
    size_t numWindows;
};

class SessionEventTimeAssigner : public Assigner {
  public:
    SessionEventTimeAssigner(Time timeout, std::string tsFieldId, Time al)
        : timeout(timeout), timestampFieldId(tsFieldId), allowedLateness(al)
    {
        if (allowedLateness.time <= timeout.time) {
            numWindows = 2;
        }
        else {
            if (allowedLateness.time % timeout.time != 0)
                throw std::invalid_argument("Invalid window: allowed lateness must be a multiple of timeout");
            numWindows = (allowedLateness.time / timeout.time) + 2;
        }
    }
    std::string to_string() override { return "SessionEventTimeAssigner"; }

  private:
    Time timeout;
    std::string timestampFieldId;
    Time allowedLateness;
    size_t numWindows;
};

} // namespace iotdb
#endif // API_ASSIGNER_H
