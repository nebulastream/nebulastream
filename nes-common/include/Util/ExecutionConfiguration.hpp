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
/// Configuration for execution of a query plan.
class ExecutionConfiguration
{
public:
    enum class ExecutionMode : uint8_t
    {
        /// Uses the interpretation based execution mode.
        INTERPRETER,
        /// Uses the compilation based execution mode.
        COMPILER
    };

private:
    ExecutionMode executionMode = ExecutionMode::COMPILER;
    bool nautilusInlining = true;

public:
    ExecutionConfiguration() = default;

    [[nodiscard]] ExecutionConfiguration withExecutionMode(ExecutionMode mode) const
    {
        ExecutionConfiguration config = *this;
        config.executionMode = mode;
        return config;
    }

    [[nodiscard]] ExecutionConfiguration withNautilusInlining(bool nautilusInlining) const
    {
        ExecutionConfiguration config = *this;
        config.nautilusInlining = nautilusInlining;
        return config;
    }

    [[nodiscard]] ExecutionMode getExecutionMode() const { return executionMode; }

    [[nodiscard]] bool isNautilusInliningEnabled() const { return nautilusInlining; }
};
}
