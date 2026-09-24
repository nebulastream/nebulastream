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

#include <TemporalPointSequenceNearestApproachDistancePhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <numbers>
#include <utility>
#include <DataTypes/FixedSizedData.hpp>
#include <DataTypes/StructData.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VectorData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <nautilus/std/vector.h>
#include <nautilus/val.hpp>
#include <std/cmath.h>

#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <MEOSWrapper.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val_concepts.hpp>

namespace NES
{

/// Iterate over the byte-aligned values of the TemporalPoint plugin and convert them into a TemporalSequence of the MEOSWrapper
MEOS::Meos::TemporalSequence constructTInstantVector(const int8_t* seqAddress, const uint64_t& seqElements)
{
    std::vector<MEOS::Meos::TemporalInstant> tInstantVector;
    tInstantVector.reserve(seqElements);
    tInstantVector.reserve(seqElements);
    /// A single temporal point consists of the doubles for lon and lat, and a uint64 for the timestamp
    constexpr size_t temporalPointSize = sizeof(double) * 2 + sizeof(uint64_t);
    for (size_t i = 0; i < seqElements; ++i)
    {
        const double lon = *reinterpret_cast<const double*>(seqAddress + (i * temporalPointSize));
        const double lat = *reinterpret_cast<const double*>(seqAddress + (i * temporalPointSize) + sizeof(double));
        const uint64_t ts = *reinterpret_cast<const uint64_t*>(seqAddress + (i * temporalPointSize) + (sizeof(double) * 2));
        tInstantVector.push_back(MEOS::Meos::TemporalInstant(lon, lat, ts));
    }
    /// Create a seperate vector for pointers to the tInstants
    std::vector<MEOS::Meos::TemporalInstant*> tInstantAddresses;
    tInstantAddresses.reserve(seqElements);
    for (MEOS::Meos::TemporalInstant& tInstant : tInstantVector)
    {
        tInstantAddresses.push_back(&tInstant);
    }
    return MEOS::Meos::TemporalSequence{tInstantAddresses};
}

TemporalPointSequenceNearestApproachDistancePhysicalFunction::TemporalPointSequenceNearestApproachDistancePhysicalFunction(
    PhysicalFunction leftPhysicalFunction, PhysicalFunction rightPhysicalFunction)
    : leftPhysicalFunction(std::move(leftPhysicalFunction)), rightPhysicalFunction(std::move(rightPhysicalFunction))
{
}

VarVal TemporalPointSequenceNearestApproachDistancePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    /// Extract the two sequences of points
    const auto leftSequenceStruct = leftPhysicalFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto lSequence = leftSequenceStruct.at("sequence").getRawValueAs<VectorData>();

    const auto rightSequenceStruct = rightPhysicalFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto rSequence = rightSequenceStruct.at("sequence").getRawValueAs<VectorData>();

    /// We will pass the raw pointer and size of the vectors into the function invoke.
    /// For now, we trust that the fields of TemporalPoint did not change and access the underlying struct field values
    /// via the pointer.
    const nautilus::val<double> result = nautilus::invoke(
        +[](const int8_t* lSeqAddress, const uint64_t lSeqElements, const int8_t* rSeqAddress, const uint64_t rSeqElements) -> double
        {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                /// Convert the left and right sequence into a TemporalSequence
                MEOS::Meos::TemporalSequence lSequenceWrapper = constructTInstantVector(lSeqAddress, lSeqElements);
                MEOS::Meos::TemporalSequence rSequenceWrapper = constructTInstantVector(rSeqAddress, rSeqElements);

                /// Call MEOS nearest approach distance function
                return MEOS::Meos::safe_nad_tgeo_tgeo(lSequenceWrapper.getGeometry(), rSequenceWrapper.getGeometry());
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
        lSequence.getRawPtr(),
        lSequence.getNumElements(),
        rSequence.getRawPtr(),
        rSequence.getNumElements());

    return VarVal{result};
}

PhysicalFunctionRegistryReturnType
TemporalPointSequenceNearestApproachDistancePhysicalFunction::createNEAREST_APPROACH_DISTANCE_TemporalPointSequence_TemporalPointSequence(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2, "NearestApproachDistance expects exactly two children functions");
    return TemporalPointSequenceNearestApproachDistancePhysicalFunction(arguments.childFunctions[0], arguments.childFunctions[1]);
}

}
