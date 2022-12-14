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

#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Nautilus/Tracing/ValueRef.hpp>
#include <utility>

namespace NES::Nautilus::Tracing {

ValueRef::ValueRef() : blockId(), operationId(){};

ValueRef::ValueRef(uint32_t blockId, uint32_t operationId, NES::Nautilus::IR::Types::StampPtr type)
    : blockId(blockId), operationId(operationId), type(std::move(type)){};

ValueRef::ValueRef(const ValueRef& other) : blockId(other.blockId), operationId(other.operationId), type(other.type){};
ValueRef::ValueRef(const ValueRef&& other)
    : blockId(other.blockId), operationId(other.operationId), type(std::move(other.type)){};

ValueRef& ValueRef::operator=(const ValueRef& other) {
    this->operationId = other.operationId;
    this->blockId = other.blockId;
    this->type = other.type;
    return *this;
}

ValueRef& ValueRef::operator=(const ValueRef&& other) {
    this->operationId = other.operationId;
    this->blockId = other.blockId;
    this->type = other.type;
    return *this;
}

ValueRef createNextRef(const NES::Nautilus::IR::Types::StampPtr& stamp) {
    if (auto ctx = Nautilus::Tracing::TraceContext::get()) {
        return ctx->createNextRef(stamp);
    }
    // create default value.
    return ValueRef(0 /* blockId */, 0 /*operationId */, NES::Nautilus::IR::Types::StampFactory::createVoidStamp());
}

std::ostream& operator<<(std::ostream& os, const ValueRef& valueRef) {
    os << "$" << valueRef.blockId << "_" << valueRef.operationId;
    return os;
}
bool ValueRef::operator==(const ValueRef& rhs) const { return blockId == rhs.blockId && operationId == rhs.operationId; }
bool ValueRef::operator!=(const ValueRef& rhs) const { return !(rhs == *this); }

}// namespace NES::Nautilus::Tracing
