
#ifndef NES_INCLUDE_QUERYCOMPILER_IR_INVOKENODE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_IR_INVOKENODE_HPP_

#include <QueryCompiler/IR/Block.hpp>
#include <QueryCompiler/IR/Node.hpp>
#include <utility>
namespace NES::QueryCompilation::IR {

class InvokeNode : public Node, public NodeInfo<InvokeNode> {
  public:
    explicit InvokeNode() : Node(NodeType()){}
  private:
};

class FunctionDescriptor{

};

}// namespace NES::QueryCompilation::IR

#endif//NES_INCLUDE_QUERYCOMPILER_IR_INVOKENODE_HPP_
