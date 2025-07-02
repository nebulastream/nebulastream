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
#include <Functions/PhysicalFunction.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <ExecutionContext.hpp>
#include <HashMapOptions.hpp>

namespace NES
{

/// Performs the second phase of the join. The tuples are joined via probing the previously built hash tables
class HJProbePhysicalOperator final : public StreamJoinProbePhysicalOperator
{
public:
    HJProbePhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        PhysicalFunction joinFunction,
        WindowMetaData windowMetaData,
        JoinSchema joinSchema,
        std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> leftMemoryProvider,
        std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> rightMemoryProvider,
        HashMapOptions leftHashMapBasedOptions,
        HashMapOptions rightHashMapBasedOptions);

    /// As the second phase gets triggered by the first phase, we receive a tuple buffer containing all information for performing the probe.
    /// Thus, we start a new pipeline and therefore, we create new Records from the built-up state.
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

private:
    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> leftMemoryProvider, rightMemoryProvider;
    HashMapOptions leftHashMapOptions, rightHashMapOptions;
};

}
