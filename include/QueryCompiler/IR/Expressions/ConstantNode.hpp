#ifndef NES_INCLUDE_QUERYCOMPILER_IR_EXPRESSIONS_SUBNODE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_IR_EXPRESSIONS_SUBNODE_HPP_

#include <QueryCompiler/IR/Node.hpp>
#include <QueryCompiler/Interpreter/Values/NesValue.hpp>
namespace NES::QueryCompilation::IR {
class ConstantNode : public Node, public NodeInfo<ConstantNode> {
  public:
    explicit ConstantNode(std::shared_ptr<NesValue> value) : Node(NodeType()), value(std::move(value)){};
    static std::shared_ptr<ConstantNode> create(std::shared_ptr<NesValue> value){
        return std::make_shared<ConstantNode>(value);
    };
    std::shared_ptr<NesValue> getValue(){
        return value;
    };
  private:
    std::shared_ptr<NesValue> value;
};
}// namespace NES::QueryCompilation::IR
#endif//NES_INCLUDE_QUERYCOMPILER_IR_EXPRESSIONS_SUBNODE_HPP_
