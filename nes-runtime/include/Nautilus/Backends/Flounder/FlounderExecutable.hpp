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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_FLOUNDER_FLOUNDEREXECUTABLE_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_FLOUNDER_FLOUNDEREXECUTABLE_HPP_

#include <Nautilus/Backends/Executable.hpp>

namespace flounder {
class Executable;
}

namespace NES::Nautilus::Backends::Flounder {

/**
 * @brief Executable that calls into a code fragment that is compiled with Flounder.
 */
class FlounderExecutable : public Executable {
  public:
    explicit FlounderExecutable(std::unique_ptr<flounder::Executable> engine);
    ~FlounderExecutable() noexcept override;

  protected:
    void* getInvocableFunctionPtr(const std::string& member) override;

  public:
    bool hasInvocableFunctionPtr() override;

  private:
    std::unique_ptr<flounder::Executable> engine;
};
}// namespace NES::Nautilus::Backends::Flounder
#endif// NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_FLOUNDER_FLOUNDEREXECUTABLE_HPP_
