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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Async/AsyncRecordLayout.hpp>

namespace NES
{

/// Reading `AsyncOperatorContext::config`, which is an opaque string map because a descriptor can
/// carry nothing else. Every executor needs the same four things from it -- a required value, an
/// optional one, a number and a field index -- and every one of them used to write its own
/// version. `executorName` only appears in the error messages, so a misconfigured query names the
/// operator the user wrote rather than the framework.
///
/// All of these throw `InvalidConfigParameter`, which fails the query at source-open time.

[[nodiscard]] std::optional<std::string> findOption(const AsyncOperatorContext& context, std::string_view key);

[[nodiscard]] std::string requiredOption(const AsyncOperatorContext& context, std::string_view key, std::string_view executorName);

/// `fallback` when the key is absent; throws when it is present but not a number.
[[nodiscard]] uint64_t
numericOption(const AsyncOperatorContext& context, std::string_view key, uint64_t fallback, std::string_view executorName);

/// Resolves a field name from the configuration against a layout, reading it as an SQL identifier
/// so the name as it was written in the query finds the field the schema stores.
[[nodiscard]] size_t
requiredFieldIndex(const AsyncRecordLayout& layout, std::string_view fieldName, std::string_view optionKey, std::string_view executorName);

}
