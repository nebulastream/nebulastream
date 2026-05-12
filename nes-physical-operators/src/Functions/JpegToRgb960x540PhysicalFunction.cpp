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

#include <Functions/JpegToRgb960x540PhysicalFunction.hpp>

#include <cstdint>
#include <utility>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <turbojpeg.h>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val_arith.hpp>

namespace NES
{

namespace
{
constexpr uint64_t TargetWidth = 960;
constexpr uint64_t TargetHeight = 540;
constexpr uint64_t TargetChannels = 3;
constexpr uint64_t TargetRgbBytes = TargetWidth * TargetHeight * TargetChannels;
}

JpegToRgb960x540PhysicalFunction::JpegToRgb960x540PhysicalFunction(PhysicalFunction childPhysicalFunction)
    : childPhysicalFunction(std::move(childPhysicalFunction))
{
}

namespace
{

uint64_t decodeJpegToRgb960x540(int8_t* inputPtr, uint64_t inputSize, int8_t* outputPtr)
{
    auto handle = tjInitDecompress();
    if (handle == nullptr)
    {
        return 0;
    }

    int width = 0;
    int height = 0;
    int subsamp = 0;
    int colorspace = 0;
    const auto headerStatus = tjDecompressHeader3(
        handle,
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        reinterpret_cast<const unsigned char*>(inputPtr),
        static_cast<unsigned long>(inputSize),
        &width,
        &height,
        &subsamp,
        &colorspace);

    if (headerStatus != 0 || width != static_cast<int>(TargetWidth) || height != static_cast<int>(TargetHeight))
    {
        tjDestroy(handle);
        return 0;
    }

    const auto decodeStatus = tjDecompress2(
        handle,
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        reinterpret_cast<const unsigned char*>(inputPtr),
        static_cast<unsigned long>(inputSize),
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        reinterpret_cast<unsigned char*>(outputPtr),
        width,
        0,
        height,
        TJPF_RGB,
        TJFLAG_ACCURATEDCT);

    tjDestroy(handle);
    return decodeStatus == 0 ? TargetRgbBytes : 0;
}

}

VarVal JpegToRgb960x540PhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto inputValue = childPhysicalFunction.execute(record, arena).getRawValueAs<VariableSizedData>();
    auto output = arena.allocateVariableSizedData(nautilus::val<uint64_t>(TargetRgbBytes));

    auto actualSize = nautilus::invoke(decodeJpegToRgb960x540, inputValue.getContent(), inputValue.getSize(), output.getContent());
    return VariableSizedData(output.getContent(), actualSize);
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterJPEG_TO_RGB_960X540PhysicalFunction(
    PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(
        physicalFunctionRegistryArguments.childFunctions.size() == 1,
        "JPEG_TO_RGB_960X540 function must have exactly one child function");
    return JpegToRgb960x540PhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0]);
}

}
