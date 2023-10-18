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

#include <Nautilus/Backends/MLIR/MLIRExecutable.hpp>
#include <Nautilus/Backends/MLIR/MLIRLoweringProvider.hpp>
#include <Nautilus/Backends/MLIR/MLIRPassManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <mlir/IR/MLIRContext.h>

namespace NES::Nautilus::Backends::MLIR {
MLIRExecutable::MLIRExecutable(std::unique_ptr<mlir::ExecutionEngine> engine) : engine(std::move(engine)) {}

void* MLIRExecutable::getInvocableFunctionPtr(const std::string& member) {
    auto function = engine->lookup(member);
    if (auto error = function.takeError()) {
        NES_THROW_RUNTIME_ERROR("Error while looking up function");
    }
    return function.get();
}
bool MLIRExecutable::hasInvocableFunctionPtr() { return true; }
MLIRExecutable::~MLIRExecutable() { NES_DEBUG("~MLIRExecutable"); }

}// namespace NES::Nautilus::Backends::MLIR