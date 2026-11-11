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
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <Sources/AsyncFunction.hpp>
#include <Sources/AsyncResourceLimiter.hpp>
#include <boost/asio/awaitable.hpp>
#include <nlohmann/json.hpp>
#include <AsyncFunctionRegistry.hpp>
#include <MockSemanticBackend.hpp>
#include <SemanticAggCodec.hpp>
#include <SemanticCallPayload.hpp>
#include <SemanticFilterCodec.hpp>
#include <SemanticHttpClient.hpp>
#include <SemanticImage.hpp>
#include <SemanticJoinCodec.hpp>
#include <SemanticKeyedCodec.hpp>
#include <SemanticLabelAnswer.hpp>
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

    /// SEM_MAP, SEM_FILTER, fused steps and SEM_JOIN: the rows (pairs) in one prompt, as `row1`..`rowN`. A row the answer
    /// does not mention gets the step's default value, as in llm_operator; with `LLM.RETRY_MISSING_ROWS` it has no result,
    /// so the Async source asks for it on its own.
    boost::asio::awaitable<AsyncBatchResult> callBatch(std::vector<std::vector<std::string>> rows) override;

    /// The model's `LLM.MAX_CONCURRENCY`.
    [[nodiscard]] std::optional<size_t> maxConcurrency() const override { return payload.config.maxConcurrency; }

    /// The model's `LLM.SHARED_CONCURRENCY`, shared by every SEM_CALL to the same endpoint and model.
    [[nodiscard]] std::optional<AsyncSharedLimit> sharedLimit() const override;

    /// The model's `LLM.BATCH_SIZE`. SEM_AGG sends one prompt per window and SEM_SCAN and SEM_WINDOW one per row, so theirs
    /// is 1.
    [[nodiscard]] size_t maxBatchSize() const override;

    /// Counts the Async source's retries for the latency line.
    void retryScheduled(std::chrono::milliseconds delay, bool transient) override;

    /// SEM_SCAN and SEM_WINDOW: the key, their first argument.
    [[nodiscard]] std::optional<size_t> orderingKey() const override;

private:
    /// Sends on `client`, or on the instance's own connection, with `images` attached to the message.
    boost::asio::awaitable<std::string>
    complete(std::string prompt, SemanticHttpClient* client = nullptr, std::vector<ImageAttachment> images = {});
    /// Posts a chat request on `client` (or the instance's own connection) and returns the answer's content, empty if the
    /// envelope has none, and the alternatives of its first token if the envelope lists logprobs. A failed exchange or a
    /// non-2xx status throws, a transient one as `AsyncTransientFailure`.
    boost::asio::awaitable<std::pair<std::string, TokenLogprobs>> exchange(nlohmann::json request, SemanticHttpClient* client);
    /// LLM.ANSWER_MODE LABEL: one row, the label prompt, one answer token (`SemanticLabelAnswer.hpp`).
    boost::asio::awaitable<AsyncCallResult> callLabel(std::vector<std::string> arguments);
    /// The label request itself: the value the answer names (if any) and its token's alternatives (with logprobs only
    /// when CONFIDENCE_FIELD is set or the call is a cascade's student).
    boost::asio::awaitable<std::pair<std::optional<std::string>, TokenLogprobs>> askLabel(std::vector<std::string> arguments);
    /// A cascade's student call: (verdict, route, p(true)), see `SemanticCallPayload::ProxyRole`.
    boost::asio::awaitable<AsyncCallResult> callProxyStudent(std::vector<std::string> arguments);
    /// A cascade's teacher call: the student's verdict for a "student" row, else the model's own.
    boost::asio::awaitable<AsyncCallResult> callCascadeTeacher(std::vector<std::string> arguments);
    [[nodiscard]] std::vector<AsyncCallResult> decode(const std::string& answer, const std::vector<RowPayload>& rows) const;
    /// The key's state folded with the row: (state, confidence).
    boost::asio::awaitable<AsyncCallResult> callScan(std::vector<std::string> arguments);
    /// The row assigned to a window of its key: (window id, description, start, continuity score, opened a window).
    boost::asio::awaitable<AsyncCallResult> callWindow(std::vector<std::string> arguments);
    struct WindowScoring;
    /// Scores the next unscored chunk of windows until none is left, on `client`.
    /// An `extra` worker only sends while it gets a permit of the shared limit at once.
    boost::asio::awaitable<void> scoreWindows(std::shared_ptr<WindowScoring> scoring, SemanticHttpClient* client, bool extra);

    SemanticCallPayload payload;
    /// Exactly one of the two codecs: the filter codec handles every step list with a FILTER step.
    std::optional<SemanticMapCodec> mapCodec;
    std::optional<SemanticFilterCodec> filterCodec;
    std::optional<SemanticJoinCodec> joinCodec;
    std::optional<SemanticAggCodec> aggCodec;
    std::optional<SemanticScanCodec> scanCodec;
    std::optional<SemanticWindowCodec> windowCodec;
    /// SEM_SCAN and SEM_WINDOW: shared with the other instances of the stage.
    std::shared_ptr<SemanticKeyedState> keyedState;
    /// Exactly one of the two transports.
    std::optional<MockSemanticBackend> mock;
    std::optional<SemanticHttpClient> http;
    /// SEM_WINDOW: the further connections a row's window requests go out on in parallel, as llm_operator's gather.
    std::vector<std::unique_ptr<SemanticHttpClient>> windowClients;
    std::optional<std::string> apiKey;
    /// The positions of `LLM.IMAGE_FIELDS` among the INPUT fields, in the order their images are attached.
    std::vector<size_t> imageFieldPositions;
    /// The limit of `LLM.SHARED_CONCURRENCY`, the same object the Async source takes its permits from.
    std::shared_ptr<AsyncResourceLimiter> sharedLimiter;
};

}
