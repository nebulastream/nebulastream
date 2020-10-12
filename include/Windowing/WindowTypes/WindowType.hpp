#ifndef NES_INCLUDE_WINDOWING_WINDOWTYPES_WINDOWTYPE_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWTYPES_WINDOWTYPE_HPP_
#include <Windowing/Runtime/WindowState.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <vector>
namespace NES {

class WindowType {
  public:
    explicit WindowType(TimeCharacteristicPtr timeCharacteristic);
    /**
      * Calculates the next window end based on a given timestamp
      * @param currentTs
      * @return the next window end
      */
    [[nodiscard]] virtual uint64_t calculateNextWindowEnd(uint64_t currentTs) const = 0;

    /**
     * @brief Generates and adds all windows between which ended size the last watermark till the current watermark.
     * @param windows vector of windows
     * @param lastWatermark
     * @param currentWatermark
     */
    virtual void triggerWindows(std::vector<WindowState>& windows, uint64_t lastWatermark, uint64_t currentWatermark) const = 0;

    /**
     * @brief Get the time characteristic of the window.
     * @return
     */
    [[nodiscard]] TimeCharacteristicPtr getTimeCharacteristic() const;

    /**
     * @return true if this is a tumbling window
     */
    virtual bool isTumblingWindow();

    /**
    * @return true if this is a sliding window
    */
    virtual bool isSlidingWindow();

  private:
    TimeCharacteristicPtr timeCharacteristic;
};

}// namespace NES

#endif//NES_INCLUDE_WINDOWING_WINDOWTYPES_WINDOWTYPE_HPP_
