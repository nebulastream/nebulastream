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

#include "Functions/CastToUnixTimestampPhysicalFunction.hpp"

#include <utility>

#include "DataTypes/DataType.hpp"
#include "ErrorHandling.hpp"
#include "ExecutionContext.hpp"
#include "Functions/PhysicalFunction.hpp"
#include "Nautilus/DataTypes/VarVal.hpp"
#include "Nautilus/Interface/Record.hpp"
#include "PhysicalFunctionRegistry.hpp"

namespace NES
    {
        CastToUnixTimestampPhysicalFunction::CastToUnixTimestampPhysicalFunction(PhysicalFunction childFunction, DataType outputType)
            : outputType(std::move(outputType)), childFunction(std::move(childFunction))
        {
        }

        VarVal CastToUnixTimestampPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
        {
            const auto value = childFunction.execute(record, arena);

            // Dummy: just use the generic cast mechanism for now.
            // Real implementation will likely interpret input values (string/date/timestamp) and compute unix epoch.
            return value.castToType(outputType.type);
        }

        PhysicalFunctionRegistryReturnType
        PhysicalFunctionGeneratedRegistrar::RegisterCastToUnixTsPhysicalFunction(
            PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
        {
            PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 1,
                         "CastToUnixTimestampPhysicalFunction must have exactly one child function");

            return CastToUnixTimestampPhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0],
                                                       physicalFunctionRegistryArguments.outputType);
        }

    } // namespace NES
