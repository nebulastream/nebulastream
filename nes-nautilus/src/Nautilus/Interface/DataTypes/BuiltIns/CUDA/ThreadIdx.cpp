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

#include <Nautilus/Interface/DataTypes/BuiltIns/CUDA/ThreadIdx.hpp>
#include <Nautilus/Interface/DataTypes/BuiltIns/CUDA/FieldAccess.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus {

ThreadIdx::ThreadIdx()
    : BuiltInVariable(&type)
{

};

const std::string ThreadIdx::getIdentifier() const {
    return "threadIdx";
}

IR::Types::StampPtr ThreadIdx::getType() const {
    // https://docs.nvidia.com/cuda/cuda-c-programming-guide/index.html#built-in-vector-types
    NES_NOT_IMPLEMENTED();
    return nullptr;
}

std::shared_ptr<Any> ThreadIdx::copy() {
    return create<ThreadIdx>();
}

const Value<> ThreadIdx::getAsValue() const {
    NES_NOT_IMPLEMENTED();
    return nullptr;
}

std::shared_ptr<FieldAccess> ThreadIdx::x() {
    return std::make_shared<FieldAccess>(create<ThreadIdx>(), "x");
}

} // namespace NES::Nautilus
