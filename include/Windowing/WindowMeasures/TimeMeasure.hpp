#ifndef NES_INCLUDE_WINDOWING_WINDOWMEASURES_TIMEMEASURE_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWMEASURES_TIMEMEASURE_HPP_
#include <Windowing/WindowMeasures/WindowMeasure.hpp>
#include <cstdint>
namespace NES{

/**
 * A time based window measure.
 */
class TimeMeasure : public WindowMeasure {
  public:
    uint64_t getTime() const;

    TimeMeasure(const uint64_t ms);

  private:
    const uint64_t _ms;
};


TimeMeasure Milliseconds(uint64_t);
TimeMeasure Seconds(uint64_t);
TimeMeasure ndMinutes(uint64_t);

}

#endif//NES_INCLUDE_WINDOWING_WINDOWMEASURES_TIMEMEASURE_HPP_
