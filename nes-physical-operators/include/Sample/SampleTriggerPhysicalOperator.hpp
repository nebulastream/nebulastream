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
#include <WindowBuildPhysicalOperator.hpp>

namespace NES
{

/// Forces the current period's slice to exist on every tick; never writes a hashmap entry itself.
class SampleTriggerPhysicalOperator final : public WindowBuildPhysicalOperator
{
public:
    SampleTriggerPhysicalOperator(
        OperatorHandlerId operatorHandlerId, std::unique_ptr<TimeFunction> timeFunction, std::unique_ptr<SliceStoreRef> sliceStoreRef);

    void execute(ExecutionContext& ctx, Record& record) const override;
};

}
