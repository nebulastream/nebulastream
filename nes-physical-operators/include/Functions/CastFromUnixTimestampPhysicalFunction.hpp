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

#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// Converts a unix timestamp in milliseconds (UINT64) to a human-readable UTC timestamp string (VARSIZED),
/// formatted as: YYYY-MM-DDTHH:MM:SS.mmmZ
class CastFromUnixTimestampPhysicalFunction final : public PhysicalFunctionConcept
{
public:
    explicit CastFromUnixTimestampPhysicalFunction(PhysicalFunction childFunction, DataType outputType);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const override;

private:
    DataType outputType;
    PhysicalFunction childFunction;
};

}
