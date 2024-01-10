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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_BACKENDS_CPP_CPPEXECUTABLE_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_BACKENDS_CPP_CPPEXECUTABLE_HPP_
#include <Compiler/DynamicObject.hpp>
#include <Nautilus/Backends/Executable.hpp>
#include <memory>
namespace NES::Nautilus::Backends::CPP {

/**
 * @brief Implements the executable for the cpp backend
 */
class CPPExecutable : public Executable {
  public:
    /**
     * Constructor to create a cpp executable.
     * @param obj the shared object, which we invoke at runtime.
     */
    explicit CPPExecutable(std::shared_ptr<Compiler::DynamicObject> obj);
    ~CPPExecutable() override = default;

  public:
    void* getInvocableFunctionPtr(const std::string& member) override;
    bool hasInvocableFunctionPtr() override;

  private:
    std::shared_ptr<Compiler::DynamicObject> obj;
};
}// namespace NES::Nautilus::Backends::CPP

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_BACKENDS_CPP_CPPEXECUTABLE_HPP_
