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

#include <memory>
#include <span>
#include <vector>

#include <Async/AsyncOperatorExecutor.hpp>

namespace NES
{

namespace detail
{
struct SemanticMapExecutorState;
}

/// SEM_MAP on the asynchronous framework: one batch of records becomes one prompt, one
/// request and one response — handed to an event loop rather than waited for on a thread.
///
/// That is the whole reason it implements this interface and not the blocking one. A model
/// call takes about a second, and the configured concurrency is 256; with a thread per call
/// in flight that would be 256 threads whose only job is to wait. Here they all sit on one
/// event loop, and the framework runs no threads for this operator at all.
///
/// It is an adapter and nothing more. The prompt layout and the response decoding stay in
/// `SemanticMapCodec`, and the protocol in `ChatCompletionProtocol` — the same two pieces
/// the synchronous `SemanticMapPhysicalOperator` uses through `HttpSemanticBackend`, so both
/// execution modes produce the same results and remain comparable in a measurement.
///
/// A model answered by the mock backend makes no HTTP call at all; its configured delay is
/// waited out on the loop's timer, because sleeping on a loop thread would stall every other
/// call in flight.
///
/// Failure handling follows the synchronous operator rather than the framework's lenient
/// default: an unusable answer writes the step's default value, while a failed transport
/// throws and fails the query, because an unreachable endpoint is the deployment's problem
/// and not the record's.
///
/// Configuration arrives as one JSON string under `SemanticMapConfigKey`; see
/// `SemanticAsyncWiring.hpp`. The state is held behind a pointer so this header stays free
/// of the semantic module's own headers — the registry's generated glue translation unit
/// includes it and does not inherit those include paths.
class SemanticMapExecutor final : public EventLoopAsyncExecutor
{
public:
    explicit SemanticMapExecutor(AsyncOperatorContext context);
    ~SemanticMapExecutor() override;

    void submit(std::span<const AsyncRecordView> batch, AsyncBatchCompletion done) override;

private:
    std::unique_ptr<detail::SemanticMapExecutorState> state;
};

}
