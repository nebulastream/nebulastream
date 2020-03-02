#include <memory>
#include <vector>

#ifndef INCLUDE_QUERYLIB_ABSTRACTWINDOWDEFINITION_HPP_
#define INCLUDE_QUERYLIB_ABSTRACTWINDOWDEFINITION_HPP_

namespace NES {

class WindowState {

  private:
    uint64_t start;
    uint64_t end;
  public:
    uint64_t getStartTs() const;
    uint64_t getEndTs() const;
    WindowState(uint64_t start, uint64_t an_end);
};

typedef std::shared_ptr<std::vector<WindowState>> WindowListPtr;

enum class TimeType {
    EventTime = 1,
    ProcessingTime = 2
};

class WindowType {
  public:
    WindowType(TimeType timeType);
    /**
      * Calculates the next window end based on a given timestamp
      * @param currentTs
      * @return the next window end
      */
    virtual uint64_t calculateNextWindowEnd(uint64_t currentTs) const = 0;

    /**
     * @brief Generates and adds all windows between which ended size the last watermark till the current watermark.
     * @param windows vector of windows
     * @param lastWatermark
     * @param currentWatermark
     */
    virtual void triggerWindows(WindowListPtr windows, uint64_t lastWatermark, uint64_t currentWatermark) const = 0;

    /**
     * @brief Get the time type of the window.
     * @return
     */
    TimeType getWindowTimeType() const;

  private:
    TimeType timeType;
};

typedef std::shared_ptr<WindowType> WindowTypePtr;

class WindowAggregation;

typedef std::shared_ptr<WindowAggregation> WindowAggregationPtr;

class AttributeField;
typedef std::shared_ptr<AttributeField> AttributeFieldPtr;

class WindowDefinition {
  public:
    WindowDefinition(const WindowAggregationPtr windowAggregation, const WindowTypePtr windowType);
    WindowDefinition(const AttributeFieldPtr onKey,
                     const WindowAggregationPtr windowAggregation,
                     const WindowTypePtr windowType);

    const WindowAggregationPtr windowAggregation;
    const WindowTypePtr windowType;
    const AttributeFieldPtr onKey;

};

typedef std::shared_ptr<WindowDefinition> WindowDefinitionPtr;

WindowDefinitionPtr createWindowDefinition(const WindowAggregationPtr windowAggregation,
                                           const WindowTypePtr windowType);
WindowDefinitionPtr createWindowDefinition(const AttributeFieldPtr onKey,
                                           const WindowAggregationPtr windowAggregation,
                                           const WindowTypePtr windowType);
}

#endif //INCLUDE_QUERYLIB_ABSTRACTWINDOWDEFINITION_HPP_
