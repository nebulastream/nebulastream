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

#include <Async/SemanticFilterExecutor.hpp>

#include <cstddef>
#include <memory>
#include <span>
#include <vector>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Async/AsyncRecordLayout.hpp>
#include <SemanticExecutorCore.hpp>
#include <SemanticFilterCodec.hpp>

namespace NES
{

namespace detail
{

struct SemanticFilterExecutorState
{
    explicit SemanticFilterExecutorState(const AsyncOperatorContext& context) : core(context, "SEM_FILTER"), codec(core.getConfig()) { }

    SemanticExecutorCore core;
    SemanticFilterCodec codec;
};

}

SemanticFilterExecutor::SemanticFilterExecutor(AsyncOperatorContext context)
    : state(std::make_unique<detail::SemanticFilterExecutorState>(context))
{
}

SemanticFilterExecutor::~SemanticFilterExecutor() = default;

std::vector<AsyncRecordResult> SemanticFilterExecutor::process(const std::span<const AsyncRecordView> batch)
{
    std::vector<AsyncRecordResult> results(batch.size());
    if (batch.empty())
    {
        return results;
    }

    const auto rows = state->core.rowsOf(batch);
    const auto response = state->core.complete(state->codec.buildPrompt(rows));

    /// Never fails and always returns one entry per row; a row the answer does not affirm is dropped.
    const auto verdicts = state->codec.parse(response, rows);
    const auto& outputFieldIndices = state->core.getOutputFieldIndices();
    for (size_t record = 0; record < batch.size(); ++record)
    {
        auto& result = results[record];
        result.keep = verdicts[record].passed;
        if (!result.keep)
        {
            continue;
        }
        /// Only a fused filter has MAP steps, and with them columns to fill.
        result.fields.reserve(outputFieldIndices.size());
        for (size_t step = 0; step < outputFieldIndices.size(); ++step)
        {
            result.fields.emplace_back(AsyncFieldValue{.fieldIndex = outputFieldIndices[step], .value = verdicts[record].mapAnswers[step]});
        }
    }
    return results;
}

}
