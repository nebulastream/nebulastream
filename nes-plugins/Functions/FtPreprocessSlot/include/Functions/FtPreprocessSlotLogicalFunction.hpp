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

/// Preprocesses the SSC camera feed's raw i/cam field -- a data-URI string
/// ("data:image/jpeg;base64,<...>"), NOT pre-decoded via FROM_BASE64 -- into the fixed [1,3,64,64]
/// float32 CHW tensor a slot-level anomaly-detection ONNX model expects: strip the data-URI prefix
/// -> base64 decode -> decode JPEG -> crop to the (x, y, w, h) pixel rect of one HBW slot -> resize
/// to 64x64 bilinear -> normalize to [0,1] -> transpose HWC -> CHW. Do the base64 decode here
/// rather than via a separate FROM_BASE64(...) call: FROM_BASE64 decodes the whole field
/// including the "data:image/jpeg;base64," prefix text, which is not valid base64 and corrupts
/// the image bytes (confirmed empirically against the live factory broker). x/y/w/h are per-slot
/// constants (the 9 ROIs from the training pipeline's rois.json), passed as query literals rather
/// than baked into the function so one physical function serves all 9 slots.
class FtPreprocessSlotLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "FT_PREPROCESS_SLOT";

    FtPreprocessSlotLogicalFunction(
        LogicalFunction image, LogicalFunction x, LogicalFunction y, LogicalFunction w, LogicalFunction h);
    /// Reconstructs from the flattened [image, x, y, w, h] child list (plan deserialization, registry dispatch).
    explicit FtPreprocessSlotLogicalFunction(const std::vector<LogicalFunction>& children);

    [[nodiscard]] bool operator==(const FtPreprocessSlotLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] FtPreprocessSlotLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] FtPreprocessSlotLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    static LogicalFunctionRegistryReturnType createFT_PREPROCESS_SLOT(LogicalFunctionRegistryArguments arguments); /// NOLINT(readability-identifier-naming)

private:
    DataType dataType;
    LogicalFunction image;
    LogicalFunction x;
    LogicalFunction y;
    LogicalFunction w;
    LogicalFunction h;

    friend Reflector<FtPreprocessSlotLogicalFunction>;
};

namespace detail
{
struct ReflectedFtPreprocessSlotLogicalFunction
{
    std::vector<LogicalFunction> children;
};
}

template <>
struct Unreflector<FtPreprocessSlotLogicalFunction>
{
    FtPreprocessSlotLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

template <>
struct Reflector<FtPreprocessSlotLogicalFunction>
{
    Reflected operator()(const FtPreprocessSlotLogicalFunction& function, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<FtPreprocessSlotLogicalFunction>);

}

FMT_OSTREAM(NES::FtPreprocessSlotLogicalFunction);
