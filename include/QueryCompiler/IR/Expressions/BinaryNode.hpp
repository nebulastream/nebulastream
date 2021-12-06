#ifndef NES_INCLUDE_QUERYCOMPILER_IR_EXPRESSIONS_BINARYNODE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_IR_EXPRESSIONS_BINARYNODE_HPP_

#include <QueryCompiler/IR/Node.hpp>
namespace NES::QueryCompilation::IR {

template<class LeftType, class RightType>
class BinaryNode {
  public:
    BinaryNode(std::shared_ptr<LeftType> leftInput, std::shared_ptr<RightType> rightInput)
        : leftInput(leftInput), rightInput(rightInput) {}

    std::shared_ptr<LeftType> getLeft() { return leftInput; };
    std::shared_ptr<RightType> getRight() { return rightInput; };

  private:
    std::shared_ptr<LeftType> leftInput;
    std::shared_ptr<RightType> rightInput;
};

}// namespace NES::QueryCompilation::IR

#endif//NES_INCLUDE_QUERYCOMPILER_IR_EXPRESSIONS_BINARYNODE_HPP_
