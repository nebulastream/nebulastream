#ifndef NES_INCLUDE_WINDOWING_WINDOWTYPES_SLIDINGWINDOW_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWTYPES_SLIDINGWINDOW_HPP_
#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
namespace NES{
/**
 * A SlidingWindow assigns records to multiple overlapping windows.
 */
class SlidingWindow : public WindowType {
  public:
    static WindowTypePtr of(TimeCharacteristicPtr timeType, TimeMeasure size, TimeMeasure slide);
    /**
  * Calculates the next window end based on a given timestamp.
  * @param currentTs
  * @return the next window end
  */
    uint64_t calculateNextWindowEnd(uint64_t currentTs) const override {
        return currentTs + slide.getTime() - (currentTs % slide.getTime());
    }
    /**
   * @brief Generates and adds all windows between which ended size the last watermark till the current watermark.
   * @param windows vector of windows
   * @param lastWatermark
   * @param currentWatermark
   */
    void triggerWindows(std::vector<WindowState> &windows, uint64_t lastWatermark, uint64_t currentWatermark) const override;

    bool isSlidingWindow() override;
    /**
    * @brief return size of the window
    * @return size of the window
    */
    TimeMeasure getSize();
    /**
    * @brief return size of the slide
    * @return size of the slide
    */
    TimeMeasure getSlide();

  private:
    SlidingWindow(TimeCharacteristicPtr timeType, TimeMeasure size, TimeMeasure slide);
    const TimeMeasure size;
    const TimeMeasure slide;
};




}

#endif//NES_INCLUDE_WINDOWING_WINDOWTYPES_SLIDINGWINDOW_HPP_
