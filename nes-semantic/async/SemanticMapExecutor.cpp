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

#include <Async/SemanticMapExecutor.hpp>

#include <cstddef>
#include <memory>
#include <span>
#include <vector>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Async/AsyncRecordLayout.hpp>
#include <SemanticExecutorCore.hpp>
#include <SemanticMapCodec.hpp>

namespace NES
{

namespace detail
{

struct SemanticMapExecutorState
{
    explicit SemanticMapExecutorState(const AsyncOperatorContext& context) : core(context, "SEM_MAP"), codec(core.getConfig()) { }

    SemanticExecutorCore core;
    SemanticMapCodec codec;
};

}

SemanticMapExecutor::SemanticMapExecutor(AsyncOperatorContext context) : state(std::make_unique<detail::SemanticMapExecutorState>(context))
{
}

SemanticMapExecutor::~SemanticMapExecutor() = default;

std::vector<AsyncRecordResult> SemanticMapExecutor::process(const std::span<const AsyncRecordView> batch)
{
    std::vector<AsyncRecordResult> results(batch.size());
    if (batch.empty())
    {
        return results;
    }

    const auto rows = state->core.rowsOf(batch);
    const auto response = state->core.complete(state->codec.buildPrompt(rows));

    /// Never fails and always returns one entry per row, each holding one answer per step.
    const auto answers = state->codec.parse(response, rows);
    const auto& outputFieldIndices = state->core.getOutputFieldIndices();
    for (size_t record = 0; record < batch.size(); ++record)
    {
        auto& fields = results[record].fields;
        fields.reserve(outputFieldIndices.size());
        for (size_t step = 0; step < outputFieldIndices.size(); ++step)
        {
            fields.emplace_back(AsyncFieldValue{.fieldIndex = outputFieldIndices[step], .value = answers[record][step]});
        }
    }
    return results;
}

}
