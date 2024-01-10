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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_BCINTERPRETER_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_BCINTERPRETER_HPP_
#include <Nautilus/Backends/BCInterpreter/ByteCode.hpp>
#include <Nautilus/Backends/Executable.hpp>
namespace NES::Nautilus::Backends::BC {

/**
 * @brief Implements the interpreter for an specific executable bytecode fragment
 */
class BCInterpreter : public Executable {
  public:
    /**
     * Constructor to create a bytecode interpreter.
     */
    BCInterpreter(Code code, RegisterFile registerFile);
    ~BCInterpreter() override = default;

  public:
    void* getInvocableFunctionPtr(const std::string& member) override;
    bool hasInvocableFunctionPtr() override;
    std::unique_ptr<GenericInvocable> getGenericInvocable(const std::string& string) override;
    std::any invokeGeneric(const std::vector<std::any>& arguments);

  private:
    int64_t execute(RegisterFile& regs) const;
    Code code;
    RegisterFile registerFile;
};
}// namespace NES::Nautilus::Backends::BC

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_BCINTERPRETER_HPP_
