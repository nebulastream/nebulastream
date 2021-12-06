#ifndef NES_INCLUDE_QUERYCOMPILER_IR_BLOCK_HPP_
#define NES_INCLUDE_QUERYCOMPILER_IR_BLOCK_HPP_
#include <QueryCompiler/IR/Node.hpp>
#include <vector>
namespace NES::QueryCompilation::IR {
class Block {
  public:
    Block() = default;
    void addNode(std::shared_ptr<Node> node){
        nodes.emplace_back(node);
    }

  private:
    std::vector<std::shared_ptr<Node>> nodes;
};
}// namespace NES::QueryCompilation::IR

#endif//NES_INCLUDE_QUERYCOMPILER_IR_BLOCK_HPP_
