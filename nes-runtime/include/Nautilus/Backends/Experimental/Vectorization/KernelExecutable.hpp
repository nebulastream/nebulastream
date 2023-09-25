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

#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_EXPERIMENTAL_VECTORIZATION_KERNELEXECUTABLE_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_EXPERIMENTAL_VECTORIZATION_KERNELEXECUTABLE_HPP_

#include <Nautilus/Backends/Executable.hpp>

#include <Compiler/DynamicObject.hpp>
#include <memory>

namespace NES::Nautilus::Backends {

// Kernel Signature:
// - void function
// - first parameter: void* for tuple buffer
// - second parameter: void* for handler
// - third parameter: uint64_t for worker id
using KernelInvocable = Executable::Invocable<void, void*, void*, uint64_t>;

/**
 * @brief Implements the kernel executable.
 */
class KernelExecutable : public Executable {
public:
    /**
     * @brief Constructor.
     * @param obj the shared object which we invoke at runtime
     * @param wrapperFunctionName the function name of the kernel wrapper
     */
    KernelExecutable(std::shared_ptr<Compiler::DynamicObject> obj, std::string wrapperFunctionName);

    ~KernelExecutable() override = default;

    KernelInvocable getKernelWrapperFunction();

private:
    void* getInvocableFunctionPtr(const std::string& member) override;
    bool hasInvocableFunctionPtr() override;

    std::shared_ptr<Compiler::DynamicObject> obj;
    std::string wrapperFunctionName;
};

} // namespace NES::Nautilus::Backends

#endif // NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_EXPERIMENTAL_VECTORIZATION_KERNELEXECUTABLE_HPP_
