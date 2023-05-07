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
#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BABELFISH_BABELFISHCODE_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BABELFISH_BABELFISHCODE_HPP_

#include <Compiler/DynamicObject.hpp>
#include <Nautilus/Backends/Executable.hpp>
#include <Nautilus/IR/Types/Stamp.hpp>
#include <memory>
#include <vector>
namespace NES::Nautilus::Backends::Babelfish {

enum Type { v, i8, i16, i32, i64, d, f };
class BabelfishCode {
  public:
    std::string code;
    IR::Types::StampPtr resultType;
    std::vector<Type> argTypes;
};

}// namespace NES::Nautilus::Backends::Babelfish

#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BABELFISH_BABELFISHCODE_HPP_
