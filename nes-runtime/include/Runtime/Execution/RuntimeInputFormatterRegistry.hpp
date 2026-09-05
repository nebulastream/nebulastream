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

namespace NES
{
class RuntimeInputFormatterRegistry final
{
public:
    void registerInputFormatterHandle(const std::uintptr_t handle) { inputFormatterHandle = handle; }

    void registerIndexerMetaDataHandle(const std::uintptr_t handle) { indexerMetaDataHandle = handle; }

    void registerNullValuesHandle(const std::uintptr_t handle) { nullValuesHandle = handle; }

    [[nodiscard]] std::uintptr_t getInputFormatterHandle() const { return inputFormatterHandle; }

    [[nodiscard]] std::uintptr_t getIndexerMetaDataHandle() const { return indexerMetaDataHandle; }

    [[nodiscard]] std::uintptr_t getNullValuesHandle() const { return nullValuesHandle; }

private:
    std::uintptr_t inputFormatterHandle = 0;
    std::uintptr_t indexerMetaDataHandle = 0;
    std::uintptr_t nullValuesHandle = 0;
};
}
