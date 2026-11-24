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

#include <functional>
#include <string>
#include <vector>
#include <Functions/LogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{
/// The runtime implementation and logical inputs selected by a concrete operator's lowering step.
/// This is a compiler result, not a logical operator or an inheritance interface.
struct AsyncOperatorLowering
{
    AsyncOperator implementation;
    std::vector<LogicalFunction> arguments;
};

using AsyncOperatorLoweringFunction = std::function<AsyncOperatorLowering(LogicalOperator)>;

class AsyncOperatorLoweringRegistry final
    : public RuntimeRegistry<AsyncOperatorLoweringRegistry, std::string, AsyncOperatorLoweringFunction, false>
{
public:
    static AsyncOperatorLoweringRegistry& instance();
};

[[nodiscard]] AsyncOperatorLowering lowerAsyncOperator(const LogicalOperator& op);
}
