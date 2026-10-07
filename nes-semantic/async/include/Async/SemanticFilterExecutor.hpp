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
struct SemanticFilterExecutorState;
}

/// SEM_FILTER on the asynchronous framework: one batch of records becomes one prompt, one request
/// and one response, and every record the answer does not affirm is dropped (`keep = false`).
///
/// The counterpart of `SemanticMapExecutor`, with `SemanticFilterCodec` in place of the map codec.
/// It also runs a fused step list (`SemanticFusionRule`) that contains MAP steps besides the
/// filter ones; their answers fill the declared OUTPUT fields of the records that pass.
///
/// Failure handling is the same as SEM_MAP's: an unusable answer drops the row — the reference's
/// falsy default — while a failed transport throws and fails the query.
///
/// The state is held behind a pointer so this header stays free of the semantic module's own
/// headers; see `SemanticMapExecutor`.
class SemanticFilterExecutor final : public AsyncOperatorExecutor
{
public:
    explicit SemanticFilterExecutor(AsyncOperatorContext context);
    ~SemanticFilterExecutor() override;

    std::vector<AsyncRecordResult> process(std::span<const AsyncRecordView> batch) override;

private:
    std::unique_ptr<detail::SemanticFilterExecutorState> state;
};

}
