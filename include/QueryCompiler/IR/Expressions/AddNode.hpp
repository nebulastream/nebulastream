//
// Created by pgrulich on 06.12.21.
//

#ifndef NES_INCLUDE_QUERYCOMPILER_IR_EXPRESSIONS_ADDNODE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_IR_EXPRESSIONS_ADDNODE_HPP_

#include <QueryCompiler/IR/Expressions/BinaryNode.hpp>
#include <QueryCompiler/IR/Node.hpp>
#include <utility>
namespace NES::QueryCompilation::IR {

class AddNode : public Node, public NodeInfo<AddNode>, public BinaryNode<Node, Node> {
  public:
    AddNode(std::shared_ptr<Node> left, std::shared_ptr<Node> right)
        : Node(NodeType()), BinaryNode<Node, Node>(std::move(left), std::move(right)){};

    static const std::shared_ptr<AddNode> create(std::shared_ptr<Node> left, std::shared_ptr<Node> right) {
        return std::make_shared<AddNode>(left, right);
    }
};

}// namespace NES::QueryCompilation::IR

#endif//NES_INCLUDE_QUERYCOMPILER_IR_EXPRESSIONS_ADDNODE_HPP_
