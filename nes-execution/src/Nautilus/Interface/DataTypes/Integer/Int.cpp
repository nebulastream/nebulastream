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
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/DataTypes/InvocationPlugin.hpp>

namespace NES::Nautilus {

Int::Int(const TypeIdentifier* identifier) : TraceableType(identifier) {}
Int::~Int() {}

bool Int::isInteger(const Any& val) {
    return isa<Int8>(val) || isa<Int16>(val) || isa<Int32>(val) || isa<Int64>(val) || isa<UInt8>(val) || isa<UInt16>(val)
        || isa<UInt32>(val) || isa<UInt64>(val);
}

}// namespace NES::Nautilus