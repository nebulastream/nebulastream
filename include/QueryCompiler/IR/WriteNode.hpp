//
// Created by pgrulich on 06.12.21.
//

#ifndef NES_INCLUDE_QUERYCOMPILER_IR_WRITENODE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_IR_WRITENODE_HPP_

#include <QueryCompiler/IR/Node.hpp>
namespace NES::QueryCompilation::IR {

class WriteNode : public Node, public NodeInfo<WriteNode> {};

}// namespace NES::QueryCompilation::IR

#endif//NES_INCLUDE_QUERYCOMPILER_IR_WRITENODE_HPP_
