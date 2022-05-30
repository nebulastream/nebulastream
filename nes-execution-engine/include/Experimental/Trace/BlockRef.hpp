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

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_BLOCKREF_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_BLOCKREF_HPP_

#include <Experimental/Trace/ValueRef.hpp>
#include <memory>
#include <vector>
namespace NES::ExecutionEngine::Experimental::Trace {
class ConstantValue;
class BlockRef {
  public:
    BlockRef(uint32_t block) : block(block){};
    uint32_t block;
    std::vector<ValueRef> arguments;
    friend std::ostream& operator<<(std::ostream& os, const ConstantValue& tag);
};
}// namespace NES::ExecutionEngine::Experimental::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_BLOCKREF_HPP_
