#ifndef NES_INCLUDE_JOIN_LOGICALJOINDEFINITION_HPP_
#define NES_INCLUDE_JOIN_LOGICALJOINDEFINITION_HPP_
#include <Join/JoinForwardRefs.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Join {

class LogicalJoinDefinition {
  public:
    LogicalJoinDefinition();

    static LogicalJoinDefinitionPtr create();

    /**
    * @brief getter/setter for on key
    */
    FieldAccessExpressionNodePtr getOnKey();
    void setOnKey(FieldAccessExpressionNodePtr onKey);

    /**
     * @brief getter/setter for window type
    */
    Windowing::WindowTypePtr getWindowType();
    void setWindowType(Windowing::WindowTypePtr windowType);

  private:
    FieldAccessExpressionNodePtr onKey;
    Windowing::WindowTypePtr windowType;

};

typedef std::shared_ptr<LogicalJoinDefinition> LogicalJoinDefinitionPtr;
}
#endif//NES_INCLUDE_JOIN_LOGICALJOINDEFINITION_HPP_
