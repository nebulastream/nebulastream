#ifndef NES_INCLUDE_WINDOWING_WINDOWTYPES_THRESHOLDWINDOW_HPP
#define NES_INCLUDE_WINDOWING_WINDOWTYPES_THRESHOLDWINDOW_HPP

#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>

namespace NES::Windowing {

class ThresholdWindow : public WindowType {
  public:

    static WindowTypePtr of(ExpressionNodePtr predicate);

    // TODO 2981: Some of the following method might not needed
    /**
    * Calculates the next window end based on a given timestamp.
    * The next window end is equal to the current timestamp plus the window size minus the fraction of the current window
    * @param currentTs
    * @return the next window end
    */
    [[nodiscard]] uint64_t calculateNextWindowEnd(uint64_t currentTs) const override;

    /**
    * @brief Returns true, because this a threshold window
    * @return true
    */
    bool isThresholdWindow() override;

    /**
    * @brief Generates and adds all windows between which ended size the last watermark till the current watermark.
    * @param windows vector of windows
    * @param lastWatermark
    * @param currentWatermark
    */
    void triggerWindows(std::vector<WindowState>& windows, uint64_t lastWatermark, uint64_t currentWatermark) const override;

    /**
    * @brief return size of the window
    * @return size of the window
    */
    TimeMeasure getSize() override;

    TimeMeasure getSlide() override;

    std::string toString() override;

    bool equal(WindowTypePtr otherWindowType) override;

    const ExpressionNodePtr& getPredicate() const;

  private:
    explicit ThresholdWindow(ExpressionNodePtr predicate);

    ExpressionNodePtr predicate;

};

} // namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWTYPES_THRESHOLDWINDOW_HPP
