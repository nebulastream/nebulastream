#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_WINDOWING_WINDOWOPERATORNODE_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_WINDOWING_WINDOWOPERATORNODE_HPP_
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>

namespace NES::Windowing{

class LogicalWindowDefinition;
typedef std::shared_ptr<LogicalWindowDefinition> LogicalWindowDefinitionPtr;

}

namespace NES{


class WindowOperatorNode;
typedef std::shared_ptr<WindowOperatorNode> WindowOperatorNodePtr;


class WindowOperatorNode : public LogicalOperatorNode {
  public:
    WindowOperatorNode(const Windowing::LogicalWindowDefinitionPtr windowDefinition);

    /**
     * @brief Gets the window definition of the window operator.
     * @return LogicalWindowDefinitionPtr
     */
    Windowing::LogicalWindowDefinitionPtr getWindowDefinition() const;

  protected:
    Windowing::LogicalWindowDefinitionPtr windowDefinition;
};

}

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_WINDOWING_WINDOWOPERATORNODE_HPP_
