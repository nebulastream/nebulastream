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

#include <Interface/RecordBuffer.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// Emit that hands the complete output of each unchunked input to the `RangeCoalescer` of its `CoalescingEmitOperatorHandler`, which
/// merges the outputs of adjacent inputs. An output that is chunked or spills over several buffers leaves as from `EmitPhysicalOperator`.
class CoalescingEmitPhysicalOperator final : public EmitPhysicalOperator
{
public:
    /// Takes over the id, buffer ref and handler id of `emit`. The handler must be a `CoalescingEmitOperatorHandler`.
    explicit CoalescingEmitPhysicalOperator(const EmitPhysicalOperator& emit) : EmitPhysicalOperator(emit) { }

    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;
    void terminate(ExecutionContext& ctx) const override;

protected:
    void onFullBufferEmitted(EmitState& state) const override;
};

}
