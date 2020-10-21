#ifndef NES_INCLUDE_WINDOWING_LOGICALWINDOWDEFINITION_HPP_
#define NES_INCLUDE_WINDOWING_LOGICALWINDOWDEFINITION_HPP_

#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Windowing {

class LogicalWindowDefinition {
  public:
    /**
     * @brief This constructor construts a key-less window
     * @param windowAggregation
     * @param windowType
     * @param distChar
     */
    LogicalWindowDefinition(WindowAggregationPtr windowAggregation, WindowTypePtr windowType, DistributionCharacteristicPtr distChar);

    /**
     * @brief This constructor constructs a key-by window
     * @param onKey key on which the window is constructed
     * @param windowAggregation
     * @param windowType
     * @param distChar
     * @param numberOfInputEdges
     */
    LogicalWindowDefinition(FieldAccessExpressionNodePtr onKey,
                     WindowAggregationPtr windowAggregation,
                     WindowTypePtr windowType,
                     DistributionCharacteristicPtr distChar,
                     uint64_t numberOfInputEdges);

    /**
     * @brief Create a new window definition for a global window
     * @param windowAggregation
     * @param windowType
     * @return Window Definition
     */
    static LogicalWindowDefinitionPtr create(WindowAggregationPtr windowAggregation,
                                      WindowTypePtr windowType,
                                      DistributionCharacteristicPtr distChar);

    /**
     * @brief Create a new window definition for a keyed window
     * @param windowAggregation
     * @param windowType
     * @return Window Definition
     */
    static LogicalWindowDefinitionPtr create(FieldAccessExpressionNodePtr onKey,
                                      WindowAggregationPtr windowAggregation,
                                      WindowTypePtr windowType,
                                      DistributionCharacteristicPtr distChar,
                                      uint64_t numberOfInputEdges);

    /**
    * @brief Create a new window definition for a keyed window
    * @param windowAggregation
    * @param windowType
    * @return Window Definition
    */
    static LogicalWindowDefinitionPtr create(ExpressionItem onKey,
                                             WindowAggregationPtr windowAggregation,
                                             WindowTypePtr windowType,
                                             DistributionCharacteristicPtr distChar,
                                             uint64_t numberOfInputEdges);


    /**
     * @brief getter and setter for the distribution type (centralized or distributed)
     */
    void setDistributionCharacteristic(DistributionCharacteristicPtr characteristic);
    DistributionCharacteristicPtr getDistributionType();

    /**
     * @brief getter and setter for the number of input edges, which is used for the low watermarks
     */
    [[nodiscard]] uint64_t getNumberOfInputEdges() const;
    void setNumberOfInputEdges(uint64_t numberOfInputEdges);

    /**
     * @brief Returns true if this window is keyed.
     * @return
    */
    bool isKeyed();

    /**
     * @brief getter/setter for window aggregation
     */
    WindowAggregationPtr getWindowAggregation();
    void setWindowAggregation(WindowAggregationPtr windowAggregation);

    /**
     * @brief getter/setter for window type
     */
    WindowTypePtr getWindowType();
    void setWindowType(WindowTypePtr windowType);

    /**
     * @brief getter/setter for on key
     */
    FieldAccessExpressionNodePtr getOnKey();
    void setOnKey(FieldAccessExpressionNodePtr onKey);

    LogicalWindowDefinitionPtr copy();

  private:
    WindowAggregationPtr windowAggregation;
    WindowTypePtr windowType;
    FieldAccessExpressionNodePtr onKey;
    DistributionCharacteristicPtr distributionType;
    uint64_t numberOfInputEdges;
};

}// namespace NES

#endif//NES_INCLUDE_WINDOWING_WINDOWTYPES_WINDOWDEFINITION_HPP_
