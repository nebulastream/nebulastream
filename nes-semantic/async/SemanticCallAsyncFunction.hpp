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
#include <optional>
#include <string>
#include <vector>
#include <Sources/AsyncFunction.hpp>
#include <boost/asio/awaitable.hpp>
#include <AsyncFunctionRegistry.hpp>
#include <MockSemanticBackend.hpp>
#include <SemanticCallPayload.hpp>
#include <SemanticFilterCodec.hpp>
#include <SemanticHttpClient.hpp>
#include <SemanticJoinCodec.hpp>
#include <SemanticMapCodec.hpp>

namespace NES
{

/// Executes `SEM_CALL`: one prompt per row, built and decoded by the semantic codecs, sent over HTTP or answered by the
/// mock after a timer. Returns one value per step, a MAP step's answer or a FILTER step's `'true'`/`'false'`.
///
/// An unusable answer (no chat-completion content, unparseable JSON, a missing row) yields the step's default value, or
/// `'false'` for a filter. A failed exchange or a non-2xx status throws, so the Async source retries and then fails the
/// query.
class SemanticCallAsyncFunction final : public AsyncFunction
{
public:
    explicit SemanticCallAsyncFunction(SemanticCallPayload payload);
    /// The last instance of the process to go reports the run's latency and tokens (`SemanticCallStats`).
    ~SemanticCallAsyncFunction() override;
    SemanticCallAsyncFunction(const SemanticCallAsyncFunction&) = delete;
    SemanticCallAsyncFunction& operator=(const SemanticCallAsyncFunction&) = delete;
    SemanticCallAsyncFunction(SemanticCallAsyncFunction&&) = delete;
    SemanticCallAsyncFunction& operator=(SemanticCallAsyncFunction&&) = delete;

    /// The configuration is the encoded `SemanticCallPayload`.
    static AsyncFunctionRegistryReturnType create(AsyncFunctionRegistryArguments arguments);

    boost::asio::awaitable<AsyncCallResult> callValues(std::vector<std::string> arguments) override;

    /// The model's `LLM.MAX_CONCURRENCY`.
    [[nodiscard]] std::optional<size_t> maxConcurrency() const override { return payload.config.maxConcurrency; }

private:
    boost::asio::awaitable<std::string> complete(const std::string& prompt);
    [[nodiscard]] AsyncCallResult decode(const std::string& answer, const std::vector<RowPayload>& rows) const;

    SemanticCallPayload payload;
    /// Exactly one of the two codecs: the filter codec handles every step list with a FILTER step.
    std::optional<SemanticMapCodec> mapCodec;
    std::optional<SemanticFilterCodec> filterCodec;
    std::optional<SemanticJoinCodec> joinCodec;
    /// Exactly one of the two transports.
    std::optional<MockSemanticBackend> mock;
    std::optional<SemanticHttpClient> http;
};

}
