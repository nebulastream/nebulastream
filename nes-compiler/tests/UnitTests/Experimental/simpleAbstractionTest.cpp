/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)
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

#include <string>
#include <system_error>
#include "mlir/IR/AsmState.h"
#include "mlir/Pass/PassManager.h"
#include "mlir/Transforms/DialectConversion.h"

#include <MLIR/NESAbstractionToMLIR.hpp>
#include <MLIR/MLIRUtility.hpp>

#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
//#include <Exceptions/NesRuntimeException.hpp>
#include <iostream>

#include <utility/createNESAbstraction.hpp>


extern "C" int externalTest(const char *__restrict __format, ...) {
  std::vector<int> sampleInts{0,1,2,3};
  std::string returnString;
  for(int sampleInt : sampleInts) {
    returnString += std::to_string(sampleInt) + "-";
  }
  returnString += "\n";
  printf("%s", returnString.c_str());
  return 0;
}

int main(int argc, char **argv) {
  // Register any command line options.
  mlir::registerAsmPrinterCLOptions();
  mlir::registerMLIRContextCLOptions();
  mlir::registerPassManagerCLOptions();

  // Create sample NESAbstractionTree create an MLIR Module for it
  auto NESTree = createSimpleNESAbstractionTree();
  MLIRUtility *mlirUtility = new MLIRUtility();
  if (int error = mlirUtility->loadAndProcessMLIR(NESTree))
    return error;

  // JIT compile MLIR Module and call 'execute()'
  const std::vector<std::string> symbolNames{"externalTest"};
  const std::vector<llvm::JITTargetAddress> jitAddresses{
      llvm::pointerToJITTargetAddress(&externalTest)};
  return mlirUtility->runJit(symbolNames, jitAddresses);
}