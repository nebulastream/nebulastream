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

#include <TpointSequenceAtStboxPhysicalFunction.hpp>

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
#include <nautilus/val.hpp>
#include <std/cmath.h>

#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <MEOSUtil.hpp>
#include <MEOSWrapper.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val_concepts.hpp>

namespace NES
{

void tgeo_proxy(
    const int8_t* lSeqAddress,
    const uint64_t lSeqElements,
    double xminVal,
    double xMaxVal,
    double yminVal,
    double yMaxVal,
    uint64_t tsminVal,
    uint64_t tsmaxVal,
    Arena* arena,
    int8_t* resultStructBuffer)
{
    try
    {
        MEOS::Meos::ensureMeosInitialized();

        /// Create a TemporalSequence out of the byte-aligned TemporalPoints in lSeqAddress
        MEOS::Meos::TemporalSequence lSequence = constructTemporalSequence(lSeqAddress, lSeqElements);
        if (!lSequence.getGeometry())
        {
            std::cout << "TgeoAtStbox: left temporal geometry is null" << std::endl;
            *reinterpret_cast<int8_t**>(resultStructBuffer) = nullptr;
            *reinterpret_cast<uint64_t*>(resultStructBuffer + sizeof(int8_t*)) = 0;
            return;
        }
        /// Create STBox
        const MEOS::Meos::SpatioTemporalBox stbox(xminVal, xMaxVal, yminVal, yMaxVal, tsminVal, tsmaxVal);
        if (!stbox.getBox())
        {
            std::cout << "TgeoAtStbox: right spatio-temporal box is null" << std::endl;
            *reinterpret_cast<int8_t**>(resultStructBuffer) = nullptr;
            *reinterpret_cast<uint64_t*>(resultStructBuffer + sizeof(int8_t*)) = 0;
            return;
        }

        /// Call tgeo_at_stbox of MEOS and hold result in a TemporalHolder (for now, border_inclusive is our default for tgeo_at_stbox
        const MEOS::Meos::TemporalHolder result(MEOS::Meos::safe_tgeo_at_stbox(lSequence.getGeometry(), stbox.getBox(), true));

        /// Allocate memory for result struct

        /// If the function returned a nullptr, the moving point was not inside of the spatio-temporal box
        if (result.get() == nullptr)
        {
            /// Not a single point of the tsequence was inside the box. Vector of the result Temporal Sequence will be empty
            *reinterpret_cast<int8_t**>(resultStructBuffer) = nullptr;
            *reinterpret_cast<uint64_t*>(resultStructBuffer + sizeof(int8_t*)) = 0;
            return;
        }
        MEOS::Meos::TemporalSequence resultSequence(reinterpret_cast<TSequence*>(result.get()));

        /// Get number of tinstants in the sequence and allocate memory for the vector
        const size_t sequenceSize = resultSequence.numInstants();
        const size_t sizeOfTemporalPoint = sizeof(double) * 2 + sizeof(uint64_t);
        int8_t* vectorPtr = reinterpret_cast<int8_t*>(arena->allocateMemory(sizeOfTemporalPoint * sequenceSize).data());

        for (size_t i = 0; i < sequenceSize; i++)
        {
            /// Get TemporalInstant at index i, get the raw x, y and ts values and write them into the vector space
            MEOS::Meos::TemporalInstant instant = resultSequence.instantAt(i);
            const MEOS::Meos::TemporalInstant::RawInstant rawInstant = instant.getRawInstant().value();
            *reinterpret_cast<double*>(vectorPtr + i * sizeOfTemporalPoint) = rawInstant.x;
            *reinterpret_cast<double*>(vectorPtr + i * sizeOfTemporalPoint + sizeof(double)) = rawInstant.y;
            *reinterpret_cast<uint64_t*>(vectorPtr + i * sizeOfTemporalPoint + sizeof(double) * 2) = rawInstant.timestamp;
        }
        /// Result struct memory points to the byte-aligned vector address and vector size in bytes
        *reinterpret_cast<int8_t**>(resultStructBuffer) = vectorPtr;
        *reinterpret_cast<uint64_t*>(resultStructBuffer + sizeof(int8_t*)) = sizeOfTemporalPoint * sequenceSize;
    }
    catch (...)
    {
        *reinterpret_cast<int8_t**>(resultStructBuffer) = nullptr;
        *reinterpret_cast<uint64_t*>(resultStructBuffer + sizeof(int8_t*)) = 0;
    }
}

TpointSequenceAtStboxPhysicalFunction::TpointSequenceAtStboxPhysicalFunction(
    PhysicalFunction leftPhysicalFunction, PhysicalFunction rightPhysicalFunction, DataType outputType)
    : leftPhysicalFunction(std::move(leftPhysicalFunction)), rightPhysicalFunction(std::move(rightPhysicalFunction)), outputType(outputType)
{
}

VarVal TpointSequenceAtStboxPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    /// Retrieve TemporalPointSequence field value
    const auto lefTpointSequenceStruct = leftPhysicalFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto lSequence = lefTpointSequenceStruct.at("sequence").getRawValueAs<VectorData>();

    /// Retrieve SpatioTemporalBox field values
    const auto stbox = rightPhysicalFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto xmin = stbox.at("xmin").getRawValueAs<nautilus::val<double>>();
    const auto xmax = stbox.at("xmax").getRawValueAs<nautilus::val<double>>();
    const auto ymin = stbox.at("ymin").getRawValueAs<nautilus::val<double>>();
    const auto ymax = stbox.at("ymax").getRawValueAs<nautilus::val<double>>();
    const auto tsmin = stbox.at("tsmin").getRawValueAs<nautilus::val<uint64_t>>();
    const auto tsmax = stbox.at("tsmax").getRawValueAs<nautilus::val<uint64_t>>();

    /// Allocate memory for result struct
    const nautilus::val<int8_t*> result = arena.allocateMemory(outputType.getSizeInBytesWithoutNull());
    nautilus::invoke(
        tgeo_proxy, lSequence.getRawPtr(), lSequence.getNumElements(), xmin, xmax, ymin, ymax, tsmin, tsmax, arena.getArena(), result);

    /// Create struct data wrapping the pointer to the TemporalPoint Vector address
    const StructData sequenceData{result, outputType.fields};
    return VarVal{sequenceData};
}

PhysicalFunctionRegistryReturnType TpointSequenceAtStboxPhysicalFunction::createTGEO_AT_STBOX_TemporalPointSequence_SpatioTemporalBox(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2, "TgeoAtStbox expects exactly two children functions");
    return TpointSequenceAtStboxPhysicalFunction(arguments.childFunctions[0], arguments.childFunctions[1], arguments.outputType);
}

}
