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

class WindowDefinition;
typedef std::shared_ptr<WindowDefinition> WindowDefinitionPtr;

class WindowDefinition {
  public:
    /**
     * @brief This constructor construts a key-less window
     * @param windowAggregation
     * @param windowType
     * @param distChar
     */
    WindowDefinition(const WindowAggregationPtr windowAggregation, const WindowTypePtr windowType, DistributionCharacteristicPtr distChar);

    /**
     * @brief This constructor constructs a key-by window
     * @param onKey key on which the window is constructed
     * @param windowAggregation
     * @param windowType
     * @param distChar
     * @param numberOfInputEdges
     */
    WindowDefinition(const AttributeFieldPtr onKey,
                     const WindowAggregationPtr windowAggregation,
                     const WindowTypePtr windowType,
                     DistributionCharacteristicPtr distChar,
                     uint64_t numberOfInputEdges);

    /**
 * @brief Create a new window definition for a global window
 * @param windowAggregation
 * @param windowType
 * @return Window Definition
 */
    static WindowDefinitionPtr create(const WindowAggregationPtr windowAggregation,
                                      const WindowTypePtr windowType, DistributionCharacteristicPtr distChar);

    /**
     * @brief Create a new window definition for a keyed window
     * @param windowAggregation
     * @param windowType
     * @return Window Definition
     */
    static WindowDefinitionPtr create(const AttributeFieldPtr onKey,
                                      const WindowAggregationPtr windowAggregation,
                                      const WindowTypePtr windowType, DistributionCharacteristicPtr distChar,
                                      uint64_t numberOfInputEdges);

    /**
     * @brief getter and setter for the distribution type (centralized or distributed)
     */
    void setDistributionCharacteristic(DistributionCharacteristicPtr characteristic);
    DistributionCharacteristicPtr getDistributionType();

    /**
     * @brief getter and setter for the number of input edges, which is used for the low watermarks
     */
    uint64_t getNumberOfInputEdges() const;
    void setNumberOfInputEdges(uint64_t numberOfInputEdges);

    /**
     * @brief Returns true if this window is keyed.
     * @return
    */
    bool isKeyed();

    /**
     * @brief getter/setter for window aggregation
     */
    WindowAggregationPtr& getWindowAggregation();
    void setWindowAggregation(WindowAggregationPtr& windowAggregation);

    /**
     * @brief getter/setter for window type
     */
    WindowTypePtr& getWindowType();
    void setWindowType(WindowTypePtr& windowType);

    /**
     * @brief getter/setter for on key
     */
    AttributeFieldPtr& getOnKey();
    void setOnKey(AttributeFieldPtr& onKey);

  private:
    WindowAggregationPtr windowAggregation;
    WindowTypePtr windowType;
    AttributeFieldPtr onKey;
    DistributionCharacteristicPtr distributionType;
    uint64_t numberOfInputEdges;
};

}// namespace NES

#endif//INCLUDE_QUERYLIB_ABSTRACTWINDOWDEFINITION_HPP_
