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

#include <TPointEIntersectsPolygonPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <numbers>
#include <utility>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/FixedSizedData.hpp>
#include <Nautilus/DataTypes/StructData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val.hpp>
#include <std/cmath.h>

#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val_concepts.hpp>
#include <MEOSWrapper.hpp>

namespace NES
{

TPointEIntersectsPolygonPhysicalFunction::TPointEIntersectsPolygonPhysicalFunction(
    PhysicalFunction leftPhysicalFunction, PhysicalFunction rightPhysicalFunction)
    : leftPhysicalFunction(std::move(leftPhysicalFunction)), rightPhysicalFunction(std::move(rightPhysicalFunction))
{
}

VarVal TPointEIntersectsPolygonPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    /// Get the values of the moving point
    const auto leftPoint = leftPhysicalFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto lLon = leftPoint.at("lon").getRawValueAs<nautilus::val<double>>();
    const auto lLat = leftPoint.at("lat").getRawValueAs<nautilus::val<double>>();
    const auto lTs = leftPoint.at("ts").getRawValueAs<nautilus::val<uint64_t>>();

    /// Get the ptr and size of the vector of points defining the polygon
    const auto rightPolygon = rightPhysicalFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto verticesVector = rightPolygon.at("vertices").getRawValueAs<VarArrayData>();
    const nautilus::val<int8_t*> verticesPtr = verticesVector.getRawPtr();
    const nautilus::val<uint64_t> verticesSize = verticesVector.getTotalSizeInBytes();

    const auto result = nautilus::invoke(
        +[](double lLonVal,
            double lLatVal,
            uint64_t lTsVal,
            int8_t* rPtr,
            uint64_t rSize
            ) -> bool {
            try {
                MEOS::Meos::ensureMeosInitialized();

                /// Create temporal instant of moving point data
                MEOS::Meos::TemporalInstant lInstant(lLonVal, lLatVal, lTsVal);
                if (!lInstant.getGeometry()) {
                    std::cout << "TemporalEIntersectsGeometry: left temporal geometry is null" << std::endl;
                    return false;
                }
                /// Create the polygon geometry by using the wkb constructor
                MEOS::Meos::StaticGeometry rPolygon(3, rPtr, rSize);
                if (!rPolygon.getGeometry()) {
                    std::cout << "TemporalEIntersectsGeometry: right geometry is null" << std::endl;
                    return false;
                }

                // Call the safe eintersects function
                return static_cast<bool>(MEOS::Meos::safe_eintersects_tgeo_geo(lInstant.getGeometry(), rPolygon.getGeometry()));
            } catch (const std::exception& e) {
                std::cout << "MEOS exception in TemporalEIntersectsGeometry: " << e.what() << std::endl;
                return false;
            } catch (...) {
                std::cout << "Unknown error in TemporalEIntersectsGeometry" << std::endl;
                return false;
            }
        },
        lLon,
        lLat,
        lTs,
        verticesPtr,
        verticesSize);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterTEMPORAL_EINTERSECTS_GEOMETRY_MovingPoint_PolygonPhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2, "TEMPORAL_EINTERSECTS_GEOMETRY expects exactly two children functions");
    return TPointEIntersectsPolygonPhysicalFunction(arguments.childFunctions[0], arguments.childFunctions[1]);
}

}
