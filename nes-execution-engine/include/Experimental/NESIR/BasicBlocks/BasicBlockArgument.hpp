#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_BASICBLOCKS_BASICBLOCKARGUMENT_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_BASICBLOCKS_BASICBLOCKARGUMENT_HPP_

#include <Experimental/NESIR/Operations/Operation.hpp>
#include <ostream>
namespace NES::ExecutionEngine::Experimental::IR::Operations {

class BasicBlockArgument : public Operation {
  public:
    explicit BasicBlockArgument(const std::string identifier, PrimitiveStamp stamp);
    ~BasicBlockArgument() override = default;
    friend std::ostream& operator<<(std::ostream& os, const BasicBlockArgument& argument);
    std::string toString() override;
};

}// namespace NES::ExecutionEngine::Experimental::IR::Operations

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_BASICBLOCKS_BASICBLOCKARGUMENT_HPP_
