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

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Phases/LogicalTypeLowering/PhysicalLayout.hpp>
#include <LogicalTypeLoweringRegistry.hpp>

namespace NES
{

LogicalTypeLoweringRegistryReturnType
LogicalTypeLoweringGeneratedRegistrar::RegisterPointLogicalTypeLowering(LogicalTypeLoweringRegistryArguments args)
{
    const auto nullable = args.logicalType.getNullable();
    const auto f64 = DataTypeProvider::provideDataType(DataType::Type::FLOAT64, nullable);
    /// Suffixes are uppercase to match the SQL parser, which uppercases all
    /// identifiers — a sink declaring `p_x F64` is stored as `P_X`, so the
    /// runtime spread of a Point named `P` must produce `P_X`, `P_Y`, `P_Z`.
    return PhysicalLayout{
        .components = {
            {.suffix = "_X", .physicalType = f64},
            {.suffix = "_Y", .physicalType = f64},
            {.suffix = "_Z", .physicalType = f64},
        }};
}

}
