#ifndef NES_INCLUDE_WINDOWING_WINDOWMEASURES_TIMEMEASURE_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWMEASURES_TIMEMEASURE_HPP_
#include <Windowing/WindowMeasures/WindowMeasure.hpp>
#include <cstdint>
namespace NES {

/**
 * A time based window measure.
 */
class TimeMeasure : public WindowMeasure {
  public:
    [[nodiscard]] uint64_t getTime() const;

    explicit TimeMeasure(uint64_t ms);

  private:
    const uint64_t ms;
};



}// namespace NES

#endif//NES_INCLUDE_WINDOWING_WINDOWMEASURES_TIMEMEASURE_HPP_
