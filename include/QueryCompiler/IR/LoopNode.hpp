//
// Created by pgrulich on 06.12.21.
//

#ifndef NES_INCLUDE_QUERYCOMPILER_IR_LOOPNODE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_IR_LOOPNODE_HPP_

#include <QueryCompiler/IR/Block.hpp>
#include <QueryCompiler/IR/Node.hpp>
namespace NES::QueryCompilation::IR {

class LoopNode : public Node, public NodeInfo<LoopNode> {
  public:
    LoopNode(std::shared_ptr<Node> lowerBound, std::shared_ptr<Node> upperBound, std::shared_ptr<Node> step)
        : Node(NodeType()), lowerBound(lowerBound), upperBound(upperBound), step(step), body(std::make_shared<Block>()) {}
    static std::shared_ptr<LoopNode>
    create(std::shared_ptr<Node> lowerBound, std::shared_ptr<Node> upperBound, std::shared_ptr<Node> step) {
        return std::make_shared<LoopNode>(lowerBound, upperBound, step);
    }

  private:
    std::shared_ptr<Node> lowerBound;
    std::shared_ptr<Node> upperBound;
    std::shared_ptr<Node> step;
    std::shared_ptr<Block> body;
};

}// namespace NES::QueryCompilation::IR

#endif//NES_INCLUDE_QUERYCOMPILER_IR_LOOPNODE_HPP_
