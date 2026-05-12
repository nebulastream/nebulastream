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

#include <BaseUnitTest.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

DataType dt(DataType::Type type)
{
    return DataType{type, DataType::NULLABLE::NOT_NULLABLE};
}

/// Build a Schema with N auto-named fields of the given type.
Schema fields(size_t count, DataType::Type type)
{
    Schema schema;
    for (size_t i = 0; i < count; ++i)
    {
        schema = schema.addField(fmt::format("f{}", i), dt(type));
    }
    return schema;
}

std::filesystem::path identityPath()
{
    /// tiny_identity.onnx: f32, input shape [1,100], output shape [1,100] — 100 elements each side.
    return std::filesystem::path(INFERENCE_TEST_DATA) / "tiny_identity.onnx";
}

std::filesystem::path vehicleCountPath()
{
    return std::filesystem::path(INFERENCE_TEST_DATA) / "vehicle_count_model.onnx";
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
            .inputs = Schema{}.addField("blob_in", dt(DataType::Type::VARSIZED)),
            .outputs = Schema{}.addField("blob_out", dt(DataType::Type::VARSIZED))}));
    EXPECT_TRUE(catalog.hasModel("identity-varsized"));
}

TEST_F(ModelCatalogTest, RegistersVehicleCountModelWithVarsizedInputAndInt64Output)
{
    ModelCatalog catalog;
    ASSERT_NO_THROW(catalog.registerModel(
        "vehicle-count",
        vehicleCountPath(),
        ModelSchema{
            .inputs = Schema{}.addField("image", dt(DataType::Type::VARSIZED)),
            .outputs = Schema{}.addField("vehicle_count", dt(DataType::Type::INT64))}));
    EXPECT_TRUE(catalog.hasModel("vehicle-count"));
}

TEST_F(ModelCatalogTest, RejectsNonFloat32NonVarsizedInputType)
{
    ModelCatalog catalog;
    auto ins = fields(100, DataType::Type::FLOAT32);
    ASSERT_TRUE(ins.replaceTypeOfField("f0", dt(DataType::Type::INT32)));
    ASSERT_EXCEPTION_ERRORCODE(
        catalog.registerModel("m", identityPath(), ModelSchema{.inputs = ins, .outputs = fields(100, DataType::Type::FLOAT32)}),
        NES::ErrorCode::CannotLoadModel);
}

TEST_F(ModelCatalogTest, RejectsNonFloat32NonVarsizedOutputType)
{
    ModelCatalog catalog;
    auto outs = fields(100, DataType::Type::FLOAT32);
    ASSERT_TRUE(outs.replaceTypeOfField("f0", dt(DataType::Type::INT64)));
    ASSERT_EXCEPTION_ERRORCODE(
        catalog.registerModel("m", identityPath(), ModelSchema{.inputs = fields(100, DataType::Type::FLOAT32), .outputs = outs}),
        NES::ErrorCode::CannotLoadModel);
}

TEST_F(ModelCatalogTest, RejectsVarsizedMixedWithSiblings)
{
    ModelCatalog catalog;
    ASSERT_EXCEPTION_ERRORCODE(
        catalog.registerModel(
            "m",
            identityPath(),
            ModelSchema{
                .inputs = Schema{}.addField("blob", dt(DataType::Type::VARSIZED)).addField("tail", dt(DataType::Type::FLOAT32)),
                .outputs = fields(100, DataType::Type::FLOAT32)}),
        NES::ErrorCode::CannotLoadModel);
}

TEST_F(ModelCatalogTest, RejectsInputFieldCountMismatch)
{
    ModelCatalog catalog;
    ASSERT_EXCEPTION_ERRORCODE(
        catalog.registerModel(
            "m",
            identityPath(),
            /// model expects 100 elements
            ModelSchema{.inputs = fields(99, DataType::Type::FLOAT32), .outputs = fields(100, DataType::Type::FLOAT32)}),
        NES::ErrorCode::CannotLoadModel);
}

TEST_F(ModelCatalogTest, RejectsOutputFieldCountMismatch)
{
    ModelCatalog catalog;
    ASSERT_EXCEPTION_ERRORCODE(
        catalog.registerModel(
            "m",
            identityPath(),
            /// model produces 100 elements
            ModelSchema{.inputs = fields(100, DataType::Type::FLOAT32), .outputs = fields(10, DataType::Type::FLOAT32)}),
        NES::ErrorCode::CannotLoadModel);
}

/// NOLINTEND(readability-magic-numbers)

}
