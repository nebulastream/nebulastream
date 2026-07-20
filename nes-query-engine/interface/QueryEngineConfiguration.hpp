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
#include <string>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/ConfigurationValidation.hpp>

namespace NES
{
class QueryEngineConfiguration final : public BaseConfiguration
{
    /// validators to prevent nonsensical values for the number of threads and task queue size
    static std::shared_ptr<ConfigurationValidation> numberOfThreadsValidator(std::string threadType);
    static std::shared_ptr<ConfigurationValidation> queueSizeValidator();
    static std::shared_ptr<ConfigurationValidation> pinThreadsValidator();
    static std::shared_ptr<ConfigurationValidation> invokeModeConfigurationValidator();
    static std::shared_ptr<ConfigurationValidation> ccxTopologyValidator();

public:
    QueryEngineConfiguration() = default;

    QueryEngineConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { }

    BoolOption pinThreads = {"pin_threads", "false", "Whether to pin worker and io threads", {pinThreadsValidator()}};
    UIntOption numberOfWorkerThreads
        = {"number_of_worker_threads", "4", "Number of worker threads used within the QueryEngine", {numberOfThreadsValidator("Worker")}};
    UIntOption numberOfIOThreads
        = {"number_of_io_threads", "1", "Number of io threads used within the async source runtime", {numberOfThreadsValidator("IO")}};
    UIntOption admissionQueueSize
        = {"admission_queue_size", "1000", "Size of the bounded admission queue used within the QueryEngine", {queueSizeValidator()}};
    StringOption invokeModeConfigurationPath
        = {"invoke_mode_configuration",
           "",
           "Path to YAML file configuring invoke modes (empty = all default)",
           {invokeModeConfigurationValidator()}};
    /// Master switch for nautilus' `mlir.inline_invoke_calls`: whether proxy `invoke` call sites are spliced with the
    /// callee's LLVM bitcode instead of emitting a call. GLOBAL and all-or-nothing -- it covers BOTH the accessor
    /// proxies (getMemArea, getBandDataProxy, the output write proxies) and the NAUTILUS_TAGGED_INLINE parse wrappers.
    /// It can only SUPPRESS inlining of functions whose bitcode was already emitted at BUILD time (NAUTILUS_INLINE +
    /// nautilus_inline(<target>) in CMake); it can never make a new function inlinable, and it cannot select per
    /// function -- that selection stays in testing/configurations/InlineTagConfig.yaml (build time). Default true =
    /// the previous hardcoded behavior.
    BoolOption inlineInvokeCalls = {"inline_invoke_calls", "true", "Inline nautilus proxy invoke calls into the JIT'd module (MLIR level)"};
    /// Master switch for making host-known constant bytes visible to the JIT: `embedConstantBytes` on the literal
    /// side (query string literals, CSV/JSON output glue) plus the call-free `bytesEqual` on the compare side.
    /// Both halves move together because neither pays off alone -- see Nautilus/TraceConstantBytesConfig.hpp.
    /// Default true = the previous hardcoded behavior; false restores the opaque-pointer + nautilus::memcmp shape.
    /// NOTE it interacts with inline_invoke_calls: bytesEqual is NAUTILUS_INLINE, so with inline_invoke_calls=false
    /// it stays a call and the compare side cannot fold regardless of this switch.
    BoolOption traceConstantBytes
        = {"trace_constant_bytes", "true", "Embed host-known constant bytes as JIT module constants and fold compares against them"};
    BoolOption ccxAwareTaskQueues
        = {"ccx_aware_task_queues",
           "false",
           "Shard the admission queue per L3/CCX: emitting threads enqueue into their own CCX's shard, workers prefer own-CCX tasks and "
           "steal from other shards only when idle. Requires pin_threads for effect (unpinned threads all map to shard 0). Each shard has "
           "the full admission_queue_size capacity.",
           {pinThreadsValidator()}};
    StringOption ccxTopology
        = {"ccx_topology",
           "",
           "Override the CCX topology used by ccx_aware_task_queues, e.g. '0-5,24-29;6-11,30-35' (one group per ';'). Empty = sysfs "
           "auto-detect. The NES_CCX_TOPOLOGY env var takes precedence over both.",
           {ccxTopologyValidator()}};

protected:
    std::vector<BaseOption*> getOptions() override
    {
        return {
            &pinThreads,
            &numberOfWorkerThreads,
            &numberOfIOThreads,
            &admissionQueueSize,
            &invokeModeConfigurationPath,
            &inlineInvokeCalls,
            &traceConstantBytes,
            &ccxAwareTaskQueues,
            &ccxTopology};
    }
};
}
