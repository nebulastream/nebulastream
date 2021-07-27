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
#ifndef NES_INCLUDE_COMPILER_DYNAMICOBJECT_HPP_
#define NES_INCLUDE_COMPILER_DYNAMICOBJECT_HPP_
#include <string>
#include <utility>
namespace NES::Compiler {

/**
 * @brief Represents a dynamic object, which enables the invocation of dynamically defined methods.
 */
class DynamicObject {
  public:
    /**
     * @brief Returns an invocable member of the shared object and cast its to the provided template.
     * @tparam Function
     * @param member on the dynamic object, currently provided as a MangledName.
     * #TODO #2073 look up mangled name dynamically.
     * @return Function
     */
    template<typename Function>
    Function getInvocableMember(const std::string& member){
        return reinterpret_cast<Function>(getInvocableFunctionPtr(member));
    }
    /**
     * @brief Destructor.
     */
    virtual ~DynamicObject() = default;

  protected:
    /**
     * @brief Returns a untyped function pointer to a specific symbol.
     * @param member on the dynamic object, currently provided as a MangledName.
     * @return function ptr
     */
    [[nodiscard]] virtual void* getInvocableFunctionPtr(const std::string& member) = 0;


};

}// namespace NES::Compiler

#endif//NES_INCLUDE_COMPILER_DYNAMICOBJECT_HPP_
