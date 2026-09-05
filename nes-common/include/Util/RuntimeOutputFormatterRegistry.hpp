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
#include <unordered_map>

namespace NES
{
class RuntimeOutputFormatterRegistry final
{
public:
    void registerFieldDelimiterHandle(const std::uintptr_t handle) { fieldDelimiterHandle = handle; }

    void registerTupleDelimiterHandle(const std::uintptr_t handle) { tupleDelimiterHandle = handle; }

    void registerFieldNameHandle(const uint64_t fieldIndex, const std::uintptr_t handle) { fieldNameHandles[fieldIndex] = handle; }

    [[nodiscard]] std::uintptr_t getFieldDelimiterHandle() const { return fieldDelimiterHandle; }

    [[nodiscard]] std::uintptr_t getTupleDelimiterHandle() const { return tupleDelimiterHandle; }

    [[nodiscard]] std::uintptr_t getFieldNameHandle(const uint64_t fieldIndex) const
    {
        if (const auto fieldName = fieldNameHandles.find(fieldIndex); fieldName != fieldNameHandles.end())
        {
            return fieldName->second;
        }
        return 0;
    }

private:
    std::uintptr_t fieldDelimiterHandle = 0;
    std::uintptr_t tupleDelimiterHandle = 0;
    std::unordered_map<uint64_t, std::uintptr_t> fieldNameHandles;
};
}
