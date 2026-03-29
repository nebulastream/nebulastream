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

#include <cstdint>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypeRegistry.hpp>

/// PoC of the extensible-DataType pattern: a plugin builds a fully-populated
/// nominal STRUCT DataType for a domain-specific composite (here: thermal
/// camera Image / ThermalFrame). It registers under its own name via the
/// DataTypeRegistry, so callers can reach it through
/// `DataTypeProvider::provideDataType("Image")`.
namespace NES::DataTypeGeneratedRegistrar
{

namespace
{
/// Mono16 thermal frame — flattened pixel array. Real cameras produce
/// (width * height) pixels; for the PoC we pin to a small count so systest
/// fixtures stay readable. Tune up when wiring real cameras.
constexpr uint32_t THERMAL_PIXEL_COUNT = 16;

/// Demo-sized variant for the end-to-end Python demo: 64x64. Registered as a
/// separate `ThermalImage` STRUCT so the small ThermalFrame above stays valid
/// for systest fixtures.
constexpr uint32_t THERMAL_IMAGE_PIXEL_COUNT = 64 * 64;

DataType makeThermalFrame(DataType::NULLABLE nullable)
{
    const DataType pixels{DataType::Type::FIXEDSIZED, DataType::NULLABLE::NOT_NULLABLE, DataType::Type::UINT16, THERMAL_PIXEL_COUNT};
    std::vector<std::pair<std::string, DataType>> fields;
    fields.emplace_back("pixels", pixels);
    return DataType{DataType::Type::STRUCT, nullable, std::string{"ThermalFrame"}, std::move(fields)};
}

DataType makeThermalPixel(DataType::NULLABLE nullable)
{
    std::vector<std::pair<std::string, DataType>> fields;
    fields.emplace_back("value", DataTypeProvider::provideDataType(DataType::Type::UINT16, DataType::NULLABLE::NOT_NULLABLE));
    return DataType{DataType::Type::STRUCT, nullable, std::string{"ThermalPixel"}, std::move(fields)};
}

DataType makeThermalImage(DataType::NULLABLE nullable)
{
    /// Same shape as ThermalFrame, just larger — single FIXEDSIZED<UINT16> pixels field.
    /// The logical functions gate on this layout, not on the structName, so they
    /// accept either type without code change.
    const DataType pixels{DataType::Type::FIXEDSIZED, DataType::NULLABLE::NOT_NULLABLE, DataType::Type::UINT16, THERMAL_IMAGE_PIXEL_COUNT};
    std::vector<std::pair<std::string, DataType>> fields;
    fields.emplace_back("pixels", pixels);
    return DataType{DataType::Type::STRUCT, nullable, std::string{"ThermalImage"}, std::move(fields)};
}

DataType makeRGBFrame(DataType::NULLABLE nullable)
{
    /// Planar layout — `r`, `g`, `b` each occupy a contiguous UINT8 block of
    /// `THERMAL_PIXEL_COUNT` bytes. Matching the ThermalFrame pixel count
    /// keeps `to_rgb` a same-shape transformation.
    const DataType channel{DataType::Type::FIXEDSIZED, DataType::NULLABLE::NOT_NULLABLE, DataType::Type::UINT8, THERMAL_PIXEL_COUNT};
    std::vector<std::pair<std::string, DataType>> fields;
    fields.emplace_back("r", channel);
    fields.emplace_back("g", channel);
    fields.emplace_back("b", channel);
    return DataType{DataType::Type::STRUCT, nullable, std::string{"RGBFrame"}, std::move(fields)};
}
}

DataTypeRegistryReturnType RegisterThermalFrameDataType(DataTypeRegistryArguments args)
{
    return makeThermalFrame(args.nullable);
}

DataTypeRegistryReturnType RegisterThermalPixelDataType(DataTypeRegistryArguments args)
{
    return makeThermalPixel(args.nullable);
}

DataTypeRegistryReturnType RegisterThermalImageDataType(DataTypeRegistryArguments args)
{
    return makeThermalImage(args.nullable);
}

DataTypeRegistryReturnType RegisterRGBFrameDataType(DataTypeRegistryArguments args)
{
    return makeRGBFrame(args.nullable);
}

DataTypeRegistryReturnType RegisterImageDataType(DataTypeRegistryArguments args)
{
    std::vector<std::pair<std::string, DataType>> fields;
    fields.emplace_back("timestamp", DataTypeProvider::provideDataType(DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE));
    fields.emplace_back("cameraId", DataTypeProvider::provideDataType(DataType::Type::UINT32, DataType::NULLABLE::NOT_NULLABLE));
    fields.emplace_back("frame", makeThermalFrame(DataType::NULLABLE::NOT_NULLABLE));
    return DataType{DataType::Type::STRUCT, args.nullable, std::string{"Image"}, std::move(fields)};
}

}
