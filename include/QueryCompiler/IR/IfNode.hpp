//
// Created by pgrulich on 06.12.21.
//

#ifndef NES_INCLUDE_QUERYCOMPILER_IR_IFNODE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_IR_IFNODE_HPP_
#include <QueryCompiler/IR/Block.hpp>
#include <QueryCompiler/IR/Node.hpp>
#include <utility>
namespace NES::QueryCompilation::IR {

class IFNode : public Node, public NodeInfo<IFNode> {
  public:
    explicit IFNode(std::shared_ptr<Node> expression) : Node(NodeType()), expression(std::move(expression)),
                                                        trueSuccessor(std::make_shared<Block>()),
                                                        falseSuccessor(std::make_shared<Block>()){};

    static std::shared_ptr<IFNode> create(const std::shared_ptr<Node>& expression){
        return std::make_shared<IFNode>(expression);
    }

    std::shared_ptr<Block> getTrueSuccessor() { return trueSuccessor; };
    std::shared_ptr<Block> getFalseSuccessor() { return falseSuccessor; };

  private:
    std::shared_ptr<Node> expression;
    std::shared_ptr<Block> trueSuccessor;
    std::shared_ptr<Block> falseSuccessor;
};
}// namespace NES::QueryCompilation::IR

#endif//NES_INCLUDE_QUERYCOMPILER_IR_IFNODE_HPP_
