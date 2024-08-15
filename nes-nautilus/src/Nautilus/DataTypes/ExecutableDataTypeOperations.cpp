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

#include <Nautilus/DataTypes/ExecutableDataTypeOperations.hpp>
#include <nautilus/std/cstring.h>

namespace NES::Nautilus {


BooleanVal memEquals(MemRefVal ptr1, MemRefVal ptr2, const nautilus::val<uint64_t>& size) {
    // Somehow we have to write our own memEquals, as we otherwise get a error: 'llvm.call' op result type mismatch: '!llvm.ptr' != 'i32'
    // return nautilus::memcmp(ptr1, ptr2, size) == nautilus::val<uint64_t>(0);

    for (UInt64Val i(0); i < size; ++i) {
        const auto tmp1 = static_cast<Int8Val>(*(ptr1 + i));
        const auto tmp2 = static_cast<Int8Val>(*(ptr2 + i));
        if (tmp1 != tmp2) {
            return {false};
        }
    }

    return {true};
}

void memCopy(MemRefVal dest, MemRefVal src, const nautilus::val<size_t>& size) {
    nautilus::memcpy(dest, src, size);
}

}// namespace NES::Nautilus
