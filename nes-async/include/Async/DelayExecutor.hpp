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

#include <chrono>
#include <cstddef>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include <Async/AsyncOperatorExecutor.hpp>

namespace NES
{

/// A deterministic stand-in for a slow external call: waits a configured time per batch,
/// then writes the upper-cased input field into the output field.
///
/// It exists so the framework can be tested and measured without a model server. The
/// delay makes it behave like the thing it stands for — it is what proves that a blocking
/// executor no longer stalls the engine's worker threads.
///
/// It can also stand in for a filtering operator: a record whose input field starts with
/// `drop_prefix` is dropped, which is how the framework's handling of dropped records is
/// tested without a model.
///
/// Config keys: `delay_ms` (default 0), `input_field`, `output_field` (both required),
/// `drop_prefix` (default: drop nothing).
class DelayExecutor final : public AsyncOperatorExecutor
{
public:
    explicit DelayExecutor(AsyncOperatorContext context);

    std::vector<AsyncRecordResult> process(std::span<const AsyncRecordView> batch) override;

private:
    AsyncOperatorContext context;
    std::chrono::milliseconds delay;
    size_t inputFieldIndex;
    size_t outputFieldIndex;
    std::optional<std::string> dropPrefix;
};

}
