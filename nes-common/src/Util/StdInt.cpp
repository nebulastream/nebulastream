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

#include <Util/StdInt.hpp>

uint8_t operator"" _u8(unsigned long long value) { return static_cast<uint8_t>(value); };
uint16_t operator"" _u16(unsigned long long value) { return static_cast<uint16_t>(value); };
uint32_t operator"" _u32(unsigned long long value) { return static_cast<uint32_t>(value); };
uint64_t operator"" _u64(unsigned long long value) { return static_cast<uint64_t>(value); };

HelperStructLiterals<int8_t> operator"" _s8(unsigned long long value) { return HelperStructLiterals<int8_t>(value); };
HelperStructLiterals<int16_t> operator"" _s16(unsigned long long value) { return HelperStructLiterals<int16_t>(value); };
HelperStructLiterals<int32_t> operator"" _s32(unsigned long long value) { return HelperStructLiterals<int32_t>(value); };
HelperStructLiterals<int64_t> operator"" _s64(unsigned long long value) { return HelperStructLiterals<int64_t>(value); };