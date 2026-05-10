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

#include <ThermalFrameData.hpp>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/FixedSizedData.hpp>
#include <Nautilus/DataTypes/StructData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <nautilus/val.hpp>

namespace NES
{

namespace
{
std::vector<std::pair<std::string, DataType>> makeLayout(uint32_t pixelCount)
{
    /// Mirrors the registered ThermalFrame DataType: a single inline UINT16 array.
    /// Layout drift between this wrapper and the registered DataType would silently
    /// misread bytes, so keep the two in lockstep.
    DataType pixels{DataType::Type::FIXEDSIZED, DataType::NULLABLE::NOT_NULLABLE, DataType::Type::UINT16, pixelCount};
    std::vector<std::pair<std::string, DataType>> fields;
    fields.emplace_back("pixels", pixels);
    return fields;
}
}

ThermalFrameData::ThermalFrameData(const nautilus::val<int8_t*>& pixelBuffer, const uint32_t pixelCount)
    : inner(pixelBuffer, makeLayout(pixelCount)), pixelCount(pixelCount)
{
}

FixedSizedData ThermalFrameData::getPixels() const
{
    return inner.at("pixels").getRawValueAs<FixedSizedData>();
}

uint32_t ThermalFrameData::getPixelCount() const
{
    return pixelCount;
}

const StructData& ThermalFrameData::asStructData() const
{
    return inner;
}

}
