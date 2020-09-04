#include <memory>
#include <vector>

#ifndef INCLUDE_QUERYLIB_ABSTRACTWINDOWDEFINITION_HPP_
#define INCLUDE_QUERYLIB_ABSTRACTWINDOWDEFINITION_HPP_
#include <API/Window/DistributionCharacteristic.hpp>

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

class AttributeField;
typedef std::shared_ptr<AttributeField> AttributeFieldPtr;

class TimeCharacteristic;
typedef std::shared_ptr<TimeCharacteristic> TimeCharacteristicPtr;

class DistributionCharacteristic;
typedef std::shared_ptr<DistributionCharacteristic> DistributionCharacteristicPtr;


class WindowType {
  public:
    WindowType(TimeCharacteristicPtr timeCharacteristic);
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
     * @brief Get the time characteristic of the window.
     * @return
     */
    TimeCharacteristicPtr getTimeCharacteristic() const;

    /**
     * @return true if this is a tumbling window
     */
    virtual bool isTumblingWindow();

    /**
    * @return true if this is a sliding window
    */
    virtual bool isSlidingWindow();
    /**
    * @return true if this is a sliding window
    */
    virtual bool isSessionWindow();

  private:
    TimeCharacteristicPtr timeCharacteristic;
};

typedef std::shared_ptr<WindowType> WindowTypePtr;

class WindowAggregation;

typedef std::shared_ptr<WindowAggregation> WindowAggregationPtr;

class AttributeField;
typedef std::shared_ptr<AttributeField> AttributeFieldPtr;

class WindowDefinition {
  public:
    WindowDefinition(const WindowAggregationPtr windowAggregation, const WindowTypePtr windowType, const DistributionCharacteristicPtr distrType);

    WindowDefinition(const AttributeFieldPtr onKey,
                     const WindowAggregationPtr windowAggregation,
                     const WindowTypePtr windowType,
                     const DistributionCharacteristicPtr distrType);

    const WindowAggregationPtr windowAggregation;
    const WindowTypePtr windowType;
    const AttributeFieldPtr onKey;
    const DistributionCharacteristicPtr distributionType;

    /**
     * @brief Returns true if this window is keyed.
     */
    bool isKeyed();

    DistributionCharacteristicPtr getDistributionType();

};

typedef std::shared_ptr<WindowDefinition> WindowDefinitionPtr;

/**
 * @brief Create a new window definition for a global window
 * @param windowAggregation
 * @param windowType
 * @return Window Definition
 */
WindowDefinitionPtr createWindowDefinition(const WindowAggregationPtr windowAggregation,
                                           const WindowTypePtr windowType,
                                           const DistributionCharacteristicPtr distrType);

/**
 * @brief Create a new window definition for a keyed window
 * @param windowAggregation
 * @param windowType
 * @return Window Definition
 */
WindowDefinitionPtr createWindowDefinition(const AttributeFieldPtr onKey,
                                           const WindowAggregationPtr windowAggregation,
                                           const WindowTypePtr windowType,
                                           const DistributionCharacteristicPtr distrType);
}// namespace NES

#endif//INCLUDE_QUERYLIB_ABSTRACTWINDOWDEFINITION_HPP_
