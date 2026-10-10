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

#include <Async/HttpCallExecutor.hpp>

#include <cstddef>
#include <span>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Async/AsyncOperatorOptions.hpp>
#include <Async/AsyncRecordLayout.hpp>
#include <Async/HttpOperatorExecutor.hpp>
#include <Http/HttpBackend.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
constexpr std::string_view Name = "HTTP_CALL";
}

HttpCallExecutor::HttpCallExecutor(AsyncOperatorContext context)
    : HttpOperatorExecutor(Name)
    , request(detail::httpRequestFromConfig(context, Name))
    , inputFieldIndex(requiredFieldIndex(*context.inputLayout, requiredOption(context, "input_field", Name), "input_field", Name))
    , outputFieldIndex(requiredFieldIndex(*context.outputLayout, requiredOption(context, "output_field", Name), "output_field", Name))
{
    /// One request carries one value, so there is nothing a larger batch could mean here. The rule
    /// that marks the operator always asks for one; a hand-written descriptor might not.
    if (context.batchSize != 1)
    {
        throw InvalidConfigParameter("HTTP_CALL sends one record per request, so it needs a batch size of 1, not {}", context.batchSize);
    }
    /// The body is the field's text, so say so rather than letting a server guess.
    request.headers.emplace_back("Content-Type", "text/plain; charset=utf-8");
}

std::pair<HttpRequest, std::monostate> HttpCallExecutor::buildRequest(const std::span<const AsyncRecordView> batch)
{
    INVARIANT(batch.size() == 1, "HTTP_CALL was handed a batch of {} records although it asked for one", batch.size());
    auto perRecord = request;
    perRecord.body = batch.front().readAsText(inputFieldIndex);
    return {std::move(perRecord), std::monostate{}};
}

std::vector<AsyncRecordResult>
HttpCallExecutor::decode(const HttpResponse& response, const std::monostate&, const std::span<const AsyncRecordView> batch)
{
    if (response.status < 200 || response.status >= 300)
    {
        /// An answer the service refused to give is not a value. Leaving the field at its default
        /// keeps the record, and the record count, which is what the framework guarantees.
        return std::vector<AsyncRecordResult>(batch.size());
    }

    std::vector<AsyncRecordResult> results;
    results.reserve(batch.size());
    for (size_t record = 0; record < batch.size(); ++record)
    {
        results.push_back(AsyncRecordResult{.fields = {AsyncFieldValue{.fieldIndex = outputFieldIndex, .value = response.body}}});
    }
    return results;
}

}
