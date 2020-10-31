#ifndef NES_INCLUDE_JOIN_LOGICALJOINDEFINITION_HPP_
#define NES_INCLUDE_JOIN_LOGICALJOINDEFINITION_HPP_
#include <Join/JoinForwardRefs.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Join {

class LogicalJoinDefinition {
  public:
    static LogicalJoinDefinitionPtr create(FieldAccessExpressionNodePtr leftJoinKey, FieldAccessExpressionNodePtr rightJoinKey, Windowing::WindowTypePtr windowType);

    LogicalJoinDefinition(FieldAccessExpressionNodePtr leftJoinKey, FieldAccessExpressionNodePtr rightJoinKey, Windowing::WindowTypePtr windowType);

    /**
    * @brief getter/setter for on left join key
    */
    FieldAccessExpressionNodePtr getLeftJoinKey();
    void setLeftJoinKey(FieldAccessExpressionNodePtr key);

    /**
     * @brief getter/setter for on right join key
     */
    FieldAccessExpressionNodePtr getRightJoinKey();
    void setRightJoinKey(FieldAccessExpressionNodePtr key);

    /**
     * @brief getter/setter for window type
    */
    Windowing::WindowTypePtr getWindowType();
    void setWindowType(Windowing::WindowTypePtr windowType);

  private:
    FieldAccessExpressionNodePtr leftJoinKey;
    FieldAccessExpressionNodePtr rightJoinKey;
    Windowing::WindowTypePtr windowType;
};

typedef std::shared_ptr<LogicalJoinDefinition> LogicalJoinDefinitionPtr;
}// namespace NES::Join
#endif//NES_INCLUDE_JOIN_LOGICALJOINDEFINITION_HPP_
