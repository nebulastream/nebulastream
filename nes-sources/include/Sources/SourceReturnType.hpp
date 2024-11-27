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

#include <functional>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/PinnedBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES::Sources::SourceReturnType
{
/// Todo #237: Improve error handling in sources
struct Error
{
    Exception ex;
};

struct Data
{
    NES::Memory::PinnedBuffer buffer;
};

struct EoS
{
};

enum class TryStopResult : uint8_t
{
    SUCCESS,
    TIMEOUT
};

using SourceReturnType = std::variant<Error, Data, EoS>;
using EmitFunction = std::function<void(const OriginId, SourceReturnType)>;

}
