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

#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Pointers.hpp>
#include <DistributedLogicalPlan.hpp>

namespace NES
{

/// Splits every operator marked with `AsyncExecutionTrait` out of its plan, so that its slow
/// external call happens on a source's own thread instead of on an engine worker thread.
///
/// Runs after `QueryDecomposer`, on the already placed and decomposed plan. For each marked
/// operator it turns one local plan into two, both on the same worker:
///
///   … → child → HandoffSink   |   AsyncSource → parent → …
///
/// The marked operator itself disappears: its configuration moves into the source descriptor,
/// and the source runs it. The two halves meet through an in-process channel identified by a
/// shared id.
///
/// Unlike the network cut this needs no topology path, because it mints its own sink and source
/// descriptors rather than network ones — a same-worker cut is not expressible as a network
/// channel.
class AsyncOperatorSplitter
{
    SharedPtr<const SourceCatalog> sourceCatalog;
    SharedPtr<const SinkCatalog> sinkCatalog;

public:
    AsyncOperatorSplitter(SharedPtr<const SourceCatalog> sourceCatalog, SharedPtr<const SinkCatalog> sinkCatalog);

    /// Returns the plan with every asynchronous operator replaced by a sink/source pair. A plan
    /// without such operators is returned unchanged.
    [[nodiscard]] DistributedLogicalPlan split(const DistributedLogicalPlan& placedPlan) const;
};

}
