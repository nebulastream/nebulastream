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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_VALUEREF_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_VALUEREF_HPP_
#include <memory>
namespace NES::Interpreter{

class None {};

class ValueRef {
  public:
    ValueRef() : blockId(), operationId(){};
    ValueRef(uint32_t blockId, uint32_t operationId) : blockId(blockId), operationId(operationId){};
    ValueRef(const ValueRef& other) : blockId(other.blockId), operationId(other.operationId) {}
    ValueRef& operator=(const ValueRef& other) {
        this->operationId = other.operationId;
        this->blockId = other.blockId;
        return *this;
    }
    uint32_t blockId;
    uint32_t operationId;
    bool operator==(const ValueRef& rhs) const;
    bool operator!=(const ValueRef& rhs) const;
    friend std::ostream& operator<<(std::ostream& os, const ValueRef& tag);
};

struct ValueRefHasher {
    std::size_t operator()(const ValueRef& k) const {
        auto hasher = std::hash<uint64_t>();
        std::size_t hashVal = hasher(k.operationId) ^ hasher(k.blockId);
        return hashVal;
    }
};

ValueRef createNextRef();

}

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_VALUEREF_HPP_
