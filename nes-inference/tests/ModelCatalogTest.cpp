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

#include <ModelCatalog.hpp>

#include <cstddef>
#include <filesystem>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <ranges>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>

#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

#include <Model.hpp>

namespace NES
{

namespace
{

DataType dt(DataType::Type type)
{
    return DataType{type, DataType::NULLABLE::NOT_NULLABLE};
}

/// Build an ordered field list with N auto-named fields of the given type.
Schema<UnqualifiedUnboundField, Ordered> fields(size_t count, DataType::Type type)
{
    std::vector<UnqualifiedUnboundField> fieldVec;
    fieldVec.reserve(count);
    for (size_t i = 0; i < count; ++i)
    {
        fieldVec.emplace_back(Identifier::parse(fmt::format("f{}", i)), dt(type));
    }
    return std::move(fieldVec) | std::ranges::to<Schema<UnqualifiedUnboundField, Ordered>>();
}

Schema<UnqualifiedUnboundField, Ordered> singleField(std::string_view name, DataType type)
{
    return std::vector{UnqualifiedUnboundField{Identifier::parse(std::string{name}), std::move(type)}}
    | std::ranges::to<Schema<UnqualifiedUnboundField, Ordered>>();
}

std::filesystem::path identityPath()
{
    /// tiny_identity.onnx: f32, input shape [1,100], output shape [1,100] — 100 elements each side.
    return std::filesystem::path(INFERENCE_TEST_DATA) / "tiny_identity.onnx";
}

}

class ModelCatalogTest : public ::testing::Test
{
};

/// NOLINTBEGIN(readability-magic-numbers)

TEST_F(ModelCatalogTest, RegistersModelWithMatchingFloat32Schema)
{
    ModelCatalog catalog;
    ASSERT_NO_THROW(catalog.registerModel(
        "identity",
        identityPath(),
        ModelSchema{.inputs = fields(100, DataType::Type::FLOAT32), .outputs = fields(100, DataType::Type::FLOAT32)}));
    EXPECT_TRUE(catalog.hasModel("identity"));
}

TEST_F(ModelCatalogTest, RegistersModelWithVarsizedSingleFieldOnBothSides)
{
    ModelCatalog catalog;
    ASSERT_NO_THROW(catalog.registerModel(
        "identity-varsized",
        identityPath(),
        ModelSchema{
            .inputs = singleField("blob_in", dt(DataType::Type::VARSIZED)),
            .outputs = singleField("blob_out", dt(DataType::Type::VARSIZED))}));
    EXPECT_TRUE(catalog.hasModel("identity-varsized"));
}

/// NOLINTEND(readability-magic-numbers)

}
