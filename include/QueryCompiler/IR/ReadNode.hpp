//
// Created by pgrulich on 06.12.21.
//

#ifndef NES_INCLUDE_QUERYCOMPILER_IR_READNODE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_IR_READNODE_HPP_

#include <QueryCompiler/IR/Node.hpp>
namespace NES::QueryCompilation::IR {

class ReadNode : public Node, public NodeInfo<ReadNode> {};

}// namespace NES::QueryCompilation::IR

#endif//NES_INCLUDE_QUERYCOMPILER_IR_READNODE_HPP_
