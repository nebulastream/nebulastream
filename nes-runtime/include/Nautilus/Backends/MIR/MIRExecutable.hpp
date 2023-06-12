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

#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_FLOUNDER_MIREXECUTABLE_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_FLOUNDER_MIREXECUTABLE_HPP_

#include <Nautilus/Backends/Executable.hpp>
#include <mir.h>

namespace NES::Nautilus::Backends::MIR {

/**
 * @brief Executable that calls into a code fragment that is compiled with Flounder.
 */
class MIRExecutable : public Executable {
  public:
    explicit MIRExecutable(MIR_context_t ctx, void* func);
    ~MIRExecutable() noexcept override;

  protected:
    void* getInvocableFunctionPtr(const std::string& member) override;

  public:
    bool hasInvocableFunctionPtr() override;

  private:
    MIR_context_t ctx;
    void* func;
};
}// namespace NES::Nautilus::Backends::Flounder
#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_FLOUNDER_MIREXECUTABLE_HPP_
