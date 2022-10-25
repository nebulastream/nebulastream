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

#ifndef NES_CUDAUTILITY_HPP
#define NES_CUDAUTILITY_HPP

#include <Nautilus/IR/IRGraph.hpp>
#include <mlir/ExecutionEngine/ExecutionEngine.h>
#include <mlir/IR/BuiltinOps.h>
#include <mlir/IR/MLIRContext.h>
#include <mlir/IR/OwningOpRef.h>
#include <string>

namespace NES::Nautilus::Backends::CUDA {


typedef int (*fptr)();

/**
 * @brief Provides utility functions for the MLIR backend.
 */
class CUDAUtility {

  public:
    /**
     * @brief Takes NESIR, lowers it to CUDA
     * @param ir: NESIR that is lowered to CUDA
     * @return // TODO define what should be returned
     */
    static fptr compileNESIRToMachineCode(std::shared_ptr<NES::Nautilus::IR::IRGraph> ir);

    static int exec();
  private:

};

}// namespace NES::Nautilus::Backends::CUDA

#endif//NES_CUDAUTILITY_HPP
