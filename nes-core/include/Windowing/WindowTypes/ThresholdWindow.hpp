#ifndef NES_INCLUDE_WINDOWING_WINDOWTYPES_THRESHOLDWINDOW_HPP
#define NES_INCLUDE_WINDOWING_WINDOWTYPES_THRESHOLDWINDOW_HPP

#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>

namespace NES::Windowing {

class ThresholdWindow : public WindowType {
  public:

    static WindowTypePtr of(ExpressionNodePtr predicate);

    /**
    * @brief Returns true, because this a threshold window
    * @return true
    */
    bool isThresholdWindow() override;

    std::string toString() override;

    bool equal(WindowTypePtr otherWindowType) override;

    [[nodiscard]] const ExpressionNodePtr& getPredicate() const;

  private:
    explicit ThresholdWindow(ExpressionNodePtr predicate);

    ExpressionNodePtr predicate;

};

} // namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWTYPES_THRESHOLDWINDOW_HPP
