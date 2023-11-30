/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_MLIR_MLIRCOMPILATIONBACKEND_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_MLIR_MLIRCOMPILATIONBACKEND_HPP_
#include <Nautilus/Backends/CompilationBackend.hpp>
#include <Nautilus/Util/CompilationOptions.hpp>
namespace NES::Nautilus::Backends::MLIR {

/**
 * @brief Compilation backend that uses MLIR.
 */
class MLIRCompilationBackend : public CompilationBackend {
  public:
    std::unique_ptr<Executable>
    compile(std::shared_ptr<IR::IRGraph> ir, const CompilationOptions& options, const DumpHelper& dumpHelper) override;
};

}// namespace NES::Nautilus::Backends::MLIR
#endif// NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_MLIR_MLIRCOMPILATIONBACKEND_HPP_
