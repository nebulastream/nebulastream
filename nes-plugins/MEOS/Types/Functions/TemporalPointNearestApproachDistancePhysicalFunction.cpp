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

#include <TemporalPointNearestApproachDistancePhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <numbers>
#include <utility>
#include <DataTypes/FixedSizedData.hpp>
#include <DataTypes/StructData.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val.hpp>
#include <std/cmath.h>

#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <MEOSWrapper.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val_concepts.hpp>

namespace NES
{

TemporalPointNearestApproachDistancePhysicalFunction::TemporalPointNearestApproachDistancePhysicalFunction(
    PhysicalFunction leftPhysicalFunction, PhysicalFunction rightPhysicalFunction)
    : leftPhysicalFunction(std::move(leftPhysicalFunction)), rightPhysicalFunction(std::move(rightPhysicalFunction))
{
}

VarVal TemporalPointNearestApproachDistancePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto leftPoint = leftPhysicalFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto lLon = leftPoint.at("lon").getRawValueAs<nautilus::val<double>>();
    const auto lLat = leftPoint.at("lat").getRawValueAs<nautilus::val<double>>();
    const auto lTs = leftPoint.at("ts").getRawValueAs<nautilus::val<uint64_t>>();

    const auto rightPoint = rightPhysicalFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto rLon = rightPoint.at("lon").getRawValueAs<nautilus::val<double>>();
    const auto rLat = rightPoint.at("lat").getRawValueAs<nautilus::val<double>>();
    const auto rTs = rightPoint.at("ts").getRawValueAs<nautilus::val<uint64_t>>();

    const auto result = nautilus::invoke(
        +[](double lLonVal, double lLatVal, uint64_t lTsVal, double rLonVal, double rLatVal, uint64_t rTsVal) -> double
        {
            try
            {
                MEOS::Meos::ensureMeosInitialized();

                auto inRange = [](double lo, double la) { return lo >= -180.0 && lo <= 180.0 && la >= -90.0 && la <= 90.0; };
                if (!inRange(lLonVal, lLatVal) || !inRange(rLonVal, rLatVal))
                {
                    std::cout << "NearestApproachDistance: coordinates out of range" << std::endl;
                    return 0.0;
                }

                MEOS::Meos::TemporalInstant lInstant(lLonVal, lLatVal, lTsVal);
                if (!lInstant.getGeometry())
                {
                    std::cout << "NearestApproachDistance: left temporal geometry is null" << std::endl;
                    return 0.0;
                }
                MEOS::Meos::TemporalInstant rInstant(rLonVal, rLatVal, rTsVal);
                if (!rInstant.getGeometry())
                {
                    std::cout << "NearestApproachDistance: right temporal geometry is null" << std::endl;
                    return 0.0;
                }

                /// Call MEOS nearest approach distance function
                return MEOS::Meos::safe_nad_tgeo_tgeo(lInstant.getGeometry(), rInstant.getGeometry());
            }
            catch (const std::exception& e)
            {
                std::cout << "MEOS exception in NearestApproachDistance: " << e.what() << std::endl;
                return -1.0;
            }
            catch (...)
            {
                std::cout << "Unknown error in NearestApproachDistance" << std::endl;
                return -1.0;
            }
        },
        lLon,
        lLat,
        lTs,
        rLon,
        rLat,
        rTs);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType
TemporalPointNearestApproachDistancePhysicalFunction::createNEAREST_APPROACH_DISTANCE_TemporalPoint_TemporalPoint(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2, "NearestApproachDistance expects exactly two children functions");
    return TemporalPointNearestApproachDistancePhysicalFunction(arguments.childFunctions[0], arguments.childFunctions[1]);
}

}
