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
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

/// A zero-argument function that returns the answer to life, the universe, and everything (42).
/// Implemented in Rust via CXX FFI.
class RustAnswerLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "RustAnswer";

    RustAnswerLogicalFunction();

    [[nodiscard]] bool operator==(const RustAnswerLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] RustAnswerLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] RustAnswerLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;

    friend Reflector<RustAnswerLogicalFunction>;
};

template <>
struct Reflector<RustAnswerLogicalFunction>
{
    Reflected operator()(const RustAnswerLogicalFunction& function) const;
};

template <>
struct Unreflector<RustAnswerLogicalFunction>
{
    RustAnswerLogicalFunction operator()(const Reflected& reflected) const;
};

static_assert(LogicalFunctionConcept<RustAnswerLogicalFunction>);
}

FMT_OSTREAM(NES::RustAnswerLogicalFunction);
