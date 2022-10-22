#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_COMPILATIONBACKEND_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_COMPILATIONBACKEND_HPP_
#include <Nautilus/IR/IRGraph.hpp>
namespace NES::Nautilus::Backends {
class Executable;
class CompilationBackend {
  public:
    virtual std::unique_ptr<Executable> compile(std::shared_ptr<IR::IRGraph>) = 0;
    virtual ~CompilationBackend() = default;
};

}// namespace NES::Nautilus::Backends

#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_COMPILATIONBACKEND_HPP_
