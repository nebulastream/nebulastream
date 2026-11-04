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
#include <optional>
#include <string>
#include <vector>

namespace NES
{

/// An argument of a UDF call that runs in a separate process: a field of the batch, or a constant.
struct UdfProcessArgument
{
    /// Position of the field in the batch.
    std::optional<uint64_t> field;
    std::optional<std::string> constant;
    /// Name of the DataType::Type of a constant.
    std::string constantType;
};

/// What a process needs to know about a UDF call. The query decomposer describes the calls of a UDF operator with it, and the
/// worker generates the process's code from it.
struct UdfProcessCall
{
    /// Position of the field in the batch that receives the result.
    uint64_t resultField = 0;
    std::string name;
    /// "module.function"; empty if `body` is set.
    std::string entrypoint;
    std::optional<std::string> body;
    std::vector<std::string> parameterNames;
    std::vector<UdfProcessArgument> arguments;
};

[[nodiscard]] std::string serializeUdfProcessCalls(const std::vector<UdfProcessCall>& calls);
/// @throws InvalidConfigParameter if `calls` is not the output of serializeUdfProcessCalls
[[nodiscard]] std::vector<UdfProcessCall> deserializeUdfProcessCalls(const std::string& calls);

}
