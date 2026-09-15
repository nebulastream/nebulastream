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

#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/ReflectionFwd.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

/// Preprocesses the SSC camera feed's raw i/cam field into the [1,9,3,64,64] float32 CHW tensor
/// BUCKET_SLOT_CNN_9 (the 9-slot batched sibling of BUCKET_SLOT_CNN, see
/// FtPreprocessAllSlotsPhysicalFunction) expects: strip the data-URI prefix -> base64 decode ->
/// decode JPEG ONCE, then for each of the 9 fixed HBW slot ROIs (same (x, y, w, h) pixel rects as
/// the 9 FT_PREPROCESS_SLOT calls in ssc-slot-anomaly.sql, order A1..C3) -> crop -> resize to
/// 64x64 bilinear -> normalize to [0,1] -> transpose HWC -> CHW, packing all 9 slots back to back
/// into one tensor. Unlike FT_PREPROCESS_SLOT, the ROIs are not query arguments: they are fixed
/// for the whole HBW layout, and baking them in lets one frame be decoded once instead of once per
/// slot (see FtPreprocessAllSlotsPhysicalFunction.cpp for the ROI table).
class FtPreprocessAllSlotsLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "FT_PREPROCESS_ALL_SLOTS";

    explicit FtPreprocessAllSlotsLogicalFunction(LogicalFunction image);
    /// Reconstructs from the flattened [image] child list (plan deserialization, registry dispatch).
    explicit FtPreprocessAllSlotsLogicalFunction(const std::vector<LogicalFunction>& children);

    [[nodiscard]] bool operator==(const FtPreprocessAllSlotsLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] FtPreprocessAllSlotsLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] FtPreprocessAllSlotsLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    static LogicalFunctionRegistryReturnType createFT_PREPROCESS_ALL_SLOTS(LogicalFunctionRegistryArguments arguments); /// NOLINT(readability-identifier-naming)

private:
    DataType dataType;
    LogicalFunction image;

    friend Reflector<FtPreprocessAllSlotsLogicalFunction>;
};

namespace detail
{
struct ReflectedFtPreprocessAllSlotsLogicalFunction
{
    std::vector<LogicalFunction> children;
};
}

template <>
struct Unreflector<FtPreprocessAllSlotsLogicalFunction>
{
    FtPreprocessAllSlotsLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

template <>
struct Reflector<FtPreprocessAllSlotsLogicalFunction>
{
    Reflected operator()(const FtPreprocessAllSlotsLogicalFunction& function, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<FtPreprocessAllSlotsLogicalFunction>);

}

FMT_OSTREAM(NES::FtPreprocessAllSlotsLogicalFunction);
