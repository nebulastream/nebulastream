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

#pragma once

#include <cstdint>
#include <span>
#include <string_view>

namespace NES
{
struct Arena;

struct UdfErrorHolder
{
    int8_t* errorPtr;
    uint64_t errorSize;
};

/// Fixed ABI used by the small compiled trampoline when the surrounding pipeline runs in interpreter mode.
struct PythonUdfAbiValue
{
    int8_t* data;
    uint64_t size;
    uint8_t isNull;
};

struct PythonUdfRuntimeSymbol
{
    std::string_view name;
    void* address;
};

void activatePythonUdfArena(Arena* arena);
std::span<const PythonUdfRuntimeSymbol> getPythonUdfRuntimeSymbols();
bool isPythonUdfRuntimeSymbol(std::string_view name);
}
