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
     * @param window trigger policy
     */
    LogicalWindowDefinition(WindowAggregationPtr windowAggregation, WindowTypePtr windowType, DistributionCharacteristicPtr distChar, WindowTriggerPolicyPtr triggerPolicy);

    /**
     * @brief This constructor constructs a key-by window
     * @param onKey key on which the window is constructed
     * @param windowAggregation
     * @param windowType
     * @param distChar
     * @param numberOfInputEdges
     * @param window trigger policy

     */
    LogicalWindowDefinition(FieldAccessExpressionNodePtr onKey,
                            WindowAggregationPtr windowAggregation,
                            WindowTypePtr windowType,
                            DistributionCharacteristicPtr distChar,
                            uint64_t numberOfInputEdges,
                            WindowTriggerPolicyPtr triggerPolicy);

    /**
     * @brief Create a new window definition for a global window
     * @param windowAggregation
     * @param windowType
     * @param window trigger policy

     * @return Window Definition
     */
    static LogicalWindowDefinitionPtr create(WindowAggregationPtr windowAggregation,
                                             WindowTypePtr windowType,
                                             DistributionCharacteristicPtr distChar,
                                             WindowTriggerPolicyPtr triggerPolicy);

    /**
     * @brief Create a new window definition for a keyed window
     * @param windowAggregation
     * @param windowType
     * @param window trigger policy
     * @return Window Definition
     */
    static LogicalWindowDefinitionPtr create(FieldAccessExpressionNodePtr onKey,
                                             WindowAggregationPtr windowAggregation,
                                             WindowTypePtr windowType,
                                             DistributionCharacteristicPtr distChar,
                                             uint64_t numberOfInputEdges,
                                             WindowTriggerPolicyPtr triggerPolicy);

    /**
    * @brief Create a new window definition for a keyed window
    * @param windowAggregation
    * @param windowType
    * @param window trigger policy
    * @return Window Definition
    */
    static LogicalWindowDefinitionPtr create(ExpressionItem onKey,
                                             WindowAggregationPtr windowAggregation,
                                             WindowTypePtr windowType,
                                             DistributionCharacteristicPtr distChar,
                                             uint64_t numberOfInputEdges,
                                             WindowTriggerPolicyPtr triggerPolicy);

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

    /**
     * @brief getter/setter for on trigger policy
     */
    WindowTriggerPolicyPtr getTriggerPolicy() const;
    void setTriggerPolicy(WindowTriggerPolicyPtr triggerPolicy);

  private:
    WindowAggregationPtr windowAggregation;
    WindowTriggerPolicyPtr triggerPolicy;
    WindowTypePtr windowType;
    FieldAccessExpressionNodePtr onKey;
    DistributionCharacteristicPtr distributionType;
    uint64_t numberOfInputEdges;
};

}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWTYPES_WINDOWDEFINITION_HPP_
