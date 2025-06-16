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

#include <utility>
#include <vector>
#include <Functions/BooleanFunctions/TemporalIntersectsPhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <MEOSWrapper.hpp>
#include <fmt/format.h>
#include <iostream>
#include <val.hpp>
#include <function.hpp>

namespace NES {

VarVal TemporalIntersectsPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto lonValue = leftPhysicalFunction.execute(record, arena);
    const auto latValue = rightPhysicalFunction.execute(record, arena);
    const auto tsValue = tsPhysicalFunction.execute(record, arena);
    
    std::cout << "TemporalIntersectsPhysicalFunction::execute called" << std::endl;
    
    // Extract nautilus::val<double> values from VarVal 
    auto lon_val = lonValue.cast<nautilus::val<double>>();
    auto lat_val = latValue.cast<nautilus::val<double>>();
    auto ts_val = tsValue.cast<nautilus::val<double>>();
    
    std::cout << "TemporalIntersectsPhysicalFunction processing values" << std::endl;
    
    // Use nautilus::invoke to call external MEOS function with raw values
    const auto result = nautilus::invoke(
        +[](double lon, double lat, double ts) -> bool {
            try {
                auto meos = MEOS::Meos();
                std::string str_pointbuffer = "SRID=4326;POINT(" + std::to_string(lon) + " " + std::to_string(lat) + ")@" + std::to_string(static_cast<long long>(ts));
                MEOS::Meos::TemporalInstant temporal1(str_pointbuffer);    

                // Create a second point slightly offset for intersection testing
                std::string str_pointbuffer2 = "SRID=4326;POINT(" + std::to_string(lon + 0.001) + " " + std::to_string(lat + 0.001) + ")@" + std::to_string(static_cast<long long>(ts + 1));
                MEOS::Meos::TemporalInstant temporal2(str_pointbuffer2);    
                
                bool intersection_result = temporal1.intersects(temporal2);
                std::cout << "MEOS intersection result: " << intersection_result << std::endl;
                return intersection_result;
            } catch (...) {
                std::cout << "MEOS error in spatial intersection" << std::endl;
                return false;
            }
        },
        lon_val, lat_val, ts_val
    );
    
    return VarVal(result);
}

TemporalIntersectsPhysicalFunction::TemporalIntersectsPhysicalFunction(PhysicalFunction leftPhysicalFunction, PhysicalFunction rightPhysicalFunction, PhysicalFunction tsPhysicalFunction)
    : leftPhysicalFunction(std::move(leftPhysicalFunction)), rightPhysicalFunction(std::move(rightPhysicalFunction)), tsPhysicalFunction(std::move(tsPhysicalFunction))
{
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterTemporalIntersectsPhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 3, "TemporalIntersects function must have exactly three sub-functions");
    return TemporalIntersectsPhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0], physicalFunctionRegistryArguments.childFunctions[1], physicalFunctionRegistryArguments.childFunctions[2]);
}

}
