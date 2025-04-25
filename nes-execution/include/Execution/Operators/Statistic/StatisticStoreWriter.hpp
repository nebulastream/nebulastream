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

#include <Execution/Operators/ExecutableOperator.hpp>

namespace NES::Runtime::Execution::Operators
{

class StatisticStoreWriter final : public ExecutableOperator
{
public:
    explicit StatisticStoreWriter(
        uint64_t operatorHandlerIndex,
        const std::string& hashFieldName,
        const std::string& typeFieldName,
        const std::string& startTsFieldName,
        const std::string& endTsFieldName,
        const std::string& dataFieldName);

    /// Inserts the given statistic record into the StatisticStore
    void execute(ExecutionContext& executionCtx, Record& record) const override;

private:
    const uint64_t operatorHandlerIndex;
    const std::string hashFieldName;
    const std::string typeFieldName;
    const std::string startTsFieldName;
    const std::string endTsFieldName;
    const std::string dataFieldName;
};

}
