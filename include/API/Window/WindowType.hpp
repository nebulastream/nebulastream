#ifndef INCLUDE_API_WINDOW_WINDOWTYPE_HPP_
#define INCLUDE_API_WINDOW_WINDOWTYPE_HPP_

#include <API/AbstractWindowDefinition.hpp>
#include <API/Window/WindowMeasure.hpp>
#include <vector>
namespace NES {

/**
 * A TumblingWindow assigns records to non-overlapping windows.
 */
class TumblingWindow : public WindowType {
  public:
    /**
   * Creates a new TumblingWindow that assigns
   * elements to time windows based on the element timestamp and offset.
   * For example, if you want window a stream by hour,but window begins at the 15th minutes
   * of each hour, you can use {@code of(Time.hours(1),Time.minutes(15))},then you will get
   * time windows start at 0:15:00,1:15:00,2:15:00,etc.
   * @param size
   * @return WindowTypePtr
   */
    static WindowTypePtr of(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size);

    /**
   * Calculates the next window end based on a given timestamp.
   * The next window end is equal to the current timestamp plus the window size minus the fraction of the current window
   * @param currentTs
   * @return the next window end
   */
    uint64_t calculateNextWindowEnd(uint64_t currentTs) const override {
        return currentTs + size.getTime() - (currentTs % size.getTime());
    }
    bool isTumblingWindow() override;

    /**
    * @brief Generates and adds all windows between which ended size the last watermark till the current watermark.
    * @param windows vector of windows
    * @param lastWatermark
    * @param currentWatermark
    */
    void triggerWindows(WindowListPtr windows, uint64_t lastWatermark, uint64_t currentWatermark) const override;

    /**
    * @brief return size of the window
    * @return
    */
    TimeMeasure getSize();

    /**
    * @brief return the time value
    * @return
    */
    uint64_t getTime() const override;

  private:
    TumblingWindow(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size);
    const TimeMeasure size;
};

/**
 * A SlidingWindow assigns records to multiple overlapping windows.
 * TODO This is currently not implemented!
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
        return currentTs + size.getTime() - (currentTs % size.getTime());
    }
     /**
     *  TODO not use, check if needed
    * @brief return the time value
    * @return
    */
    uint64_t getTime() const override;

    /**
   * @brief Generates and adds all windows between which ended size the last watermark till the current watermark.
   * @param windows vector of windows
   * @param lastWatermark
   * @param currentWatermark
   */
    void triggerWindows(WindowListPtr windows, uint64_t lastWatermark, uint64_t currentWatermark) const override;

    bool isSlidingWindow() override;
    /**
    * @brief return size of the window
    * @return
    */
    TimeMeasure getSize();
    /**
    * @brief return size of the slide
    * @return
    */
    TimeMeasure getSlide();
    /**
    * @brief return the time value of the window size
    * @return window size in time
    */
    uint64_t getSizeTime() const;
    /**
    * @brief return the time value of the window slide
    * @return slide size in time
    */
    uint64_t getSlideTime() const;

  private:
    SlidingWindow(TimeCharacteristicPtr timeType, TimeMeasure size, TimeMeasure slide);
    const TimeMeasure size;
    const TimeMeasure slide;
};

/**
 * A SessionWindow assigns records into sessions based on the timestamp of the
 * elements. Windows cannot overlap.
 * TODO This is currently not implemented!
 */
class SessionWindow : public WindowType {
  public:
    /**
  * Creates a new SessionWindow that assigns
  * elements to sessions based on the element timestamp.
  * @param size The session timeout, i.e. the time gap between sessions
  */
    static WindowTypePtr withGap(TimeCharacteristicPtr timeType, TimeMeasure gap);
    /**
  * Calculates the next window end based on a given timestamp.
  * @param currentTs
  * @return the next window end
  */
    uint64_t calculateNextWindowEnd(uint64_t) const override {
        return 0;
    }

    void triggerWindows(WindowListPtr windows, uint64_t lastWatermark, uint64_t currentWatermark) const override;

    uint64_t getTime() const override;

  private:
    SessionWindow(TimeCharacteristicPtr timeType, TimeMeasure gap);
    const TimeMeasure gap;
};
}// namespace NES

#endif//INCLUDE_API_WINDOW_WINDOWTYPE_HPP_
