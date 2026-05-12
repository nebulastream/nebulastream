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

#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Logical function that decodes JPEG bytes into raw 960x540 RGB bytes.
class JpegToRgb960x540LogicalFunction final
{
public:
    static constexpr std::string_view NAME = "JPEG_TO_RGB_960X540";

    /// NOLINTNEXTLINE(modernize-pass-by-value)
    explicit JpegToRgb960x540LogicalFunction(const LogicalFunction& child);

    [[nodiscard]] bool operator==(const JpegToRgb960x540LogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] JpegToRgb960x540LogicalFunction withDataType(const DataType& dataType) const;
    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] JpegToRgb960x540LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    LogicalFunction child;

    friend Reflector<JpegToRgb960x540LogicalFunction>;
};

static_assert(LogicalFunctionConcept<JpegToRgb960x540LogicalFunction>);

template <>
struct Reflector<JpegToRgb960x540LogicalFunction>
{
    Reflected operator()(const JpegToRgb960x540LogicalFunction& function) const;
};

template <>
struct Unreflector<JpegToRgb960x540LogicalFunction>
{
    JpegToRgb960x540LogicalFunction operator()(const Reflected& reflected) const;
};
}

namespace NES::detail
{
struct ReflectedJpegToRgb960x540LogicalFunction
{
    std::optional<LogicalFunction> child;
};
}

FMT_OSTREAM(NES::JpegToRgb960x540LogicalFunction);
