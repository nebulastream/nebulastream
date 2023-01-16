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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_EXECUTABLE_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_EXECUTABLE_HPP_
#include <any>
#include <functional>
#include <string>
namespace NES::Nautilus::Backends {

/**
 * @brief Represents an executable object, which enables the invocation of dynamically defined methods.
 */
class Executable {
  public:
    template<typename Function>
    Function getInvocableMember(const std::string& member) {
        return reinterpret_cast<Function>(getInvocableFunctionPtr(member));
    }

    template<typename R, typename... Args>
    requires std::is_void_v<R>
    void invoke(const std::string& member, Args... arguments) {
        if (hasInvocableFunctionPtr()) {
            auto function = getInvocableFunctionPtr(member);
            reinterpret_cast<void (*)(Args...)>(function)(std::forward<Args>(arguments)...);
        } else {
            std::vector<std::any> inputs_ = {arguments...};
            invokeGeneric(member, inputs_);
        }
    }

    template<typename R, typename... Args>
    requires (std::is_void_v<R> == false)
    R invoke(const std::string& member, Args... arguments) {
        if (hasInvocableFunctionPtr()) {
            auto function = getInvocableFunctionPtr(member);
            return reinterpret_cast<R (*)(Args...)>(function)(std::forward<Args>(arguments)...);
        } else {
            std::vector<std::any> inputs_ = {arguments...};
            auto res = invokeGeneric(member, inputs_);
            return std::any_cast<R>(res);
        }
    }

    virtual ~Executable() = default;

    /**
     * @brief Returns a untyped function pointer to a specific symbol.
     * @param member on the dynamic object, currently provided as a MangledName.
     * @return function ptr
     */
    [[nodiscard]] virtual void* getInvocableFunctionPtr(const std::string& member) = 0;
    virtual bool hasInvocableFunctionPtr() = 0;
    virtual std::any invokeGeneric(const std::string&, const std::vector<std::any>&) { return nullptr; };
};

}// namespace NES::Nautilus::Backends
#endif// NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_EXECUTABLE_HPP_
