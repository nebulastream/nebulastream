#ifndef NES_NES_EXECUTION_ENGINE_SRC_EXPERIMENTAL_BABELFISH_IRSERIALIZATION_HPP_
#define NES_NES_EXECUTION_ENGINE_SRC_EXPERIMENTAL_BABELFISH_IRSERIALIZATION_HPP_
#include <cpprest/json.h>

#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>
#include <Experimental/NESIR/BasicBlocks/BasicBlockInvocation.hpp>
#include <Experimental/NESIR/NESIR.hpp>
#include <Experimental/NESIR/Operations/Operation.hpp>

namespace web::json {
class value;
}// namespace web::json

namespace NES::ExecutionEngine::Experimental {

class IRSerialization {
  public:
    std::string serialize(std::shared_ptr<IR::NESIR> ir);
    void serializeBlock(std::shared_ptr<IR::BasicBlock> block);
    void serializeOperation(std::shared_ptr<IR::Operations::Operation> block, std::vector<web::json::value>& currentBlock);
    web::json::value serializeOperation(IR::Operations::BasicBlockInvocation& blockInvocation);

  private:
    std::vector<web::json::value> blocks;
};

}// namespace NES::ExecutionEngine::Experimental

#endif//NES_NES_EXECUTION_ENGINE_SRC_EXPERIMENTAL_BABELFISH_IRSERIALIZATION_HPP_
