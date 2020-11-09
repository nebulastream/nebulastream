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

#ifndef INCLUDE_COMPILED_CODE_HPP_
#define INCLUDE_COMPILED_CODE_HPP_
#include <memory>
#include <string>
namespace NES {

class CompiledCode;
typedef std::shared_ptr<CompiledCode> CompiledCodePtr;

class CompiledCode {
  public:
    virtual ~CompiledCode() = default;

    /**
     * @brief Finds the function by its mangled name and returns the typed function pointer.
     * @param mangled name
     */
    template<typename Function>
    Function getFunctionPointer(const std::string& name) {
        // INFO
        // http://www.trilithium.com/johan/2004/12/problem-with-dlsym/
        // No real solution in 2016.
        static_assert(sizeof(void*) == sizeof(Function), "Void pointer to function pointer conversion will not work!"
                                                         " If you encounter this, run!");

        union converter {
            void* v_ptr;
            Function f_ptr;
        };

        converter conv;
        conv.v_ptr = getFunctionPointerImpl(name);

        return conv.f_ptr;
    }

  protected:
    /**
     * @brief Gets a untyped function pointer by its mangled name and returns the typed function pointer.
     */
    virtual void* getFunctionPointerImpl(const std::string& name) = 0;
};
}// namespace NES

#endif//INCLUDE_COMPILED_CODE_HPP_
