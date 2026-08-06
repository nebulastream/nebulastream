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

#include <ranges>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

DataType dt(DataType::Type type)
{
    return DataType{type, DataType::NULLABLE::NOT_NULLABLE};
}

/// Build a ModelFieldList with N auto-named fields of the given type.
ModelFieldList fields(size_t count, DataType::Type type)
{
    std::vector<UnqualifiedUnboundField> fieldVec;
    fieldVec.reserve(count);
    for (size_t i = 0; i < count; ++i)
    {
        fieldVec.emplace_back(Identifier::parse(fmt::format("f{}", i)), dt(type));
    }
    return std::move(fieldVec) | std::ranges::to<ModelFieldList>();
}

ModelFieldList singleField(std::string_view name, DataType type)
{
    return std::vector{UnqualifiedUnboundField{Identifier::parse(std::string{name}), std::move(type)}} | std::ranges::to<ModelFieldList>();
}

/// Returns a copy of `schema` with the first field replaced by one of the same name but `replacementType`.
/// Schema is immutable, so we rebuild from scratch.
ModelFieldList replaceFirstFieldType(const ModelFieldList& schema, DataType replacementType)
{
    auto fields = schema | std::ranges::to<std::vector>();
    fields.front() = UnqualifiedUnboundField{fields.front().getFullyQualifiedName(), std::move(replacementType)};
    return std::move(fields) | std::ranges::to<ModelFieldList>();
}

std::filesystem::path identityPath()
{
    /// tiny_identity.onnx: f32, input shape [1,100], output shape [1,100] — 100 elements each side.
    return std::filesystem::path(INFERENCE_TEST_DATA) / "tiny_identity.onnx";
}

std::filesystem::path dynamicBatchPath()
{
    /// tiny_dynamic_batch.onnx: f32 identity with shape ["batch",100] on each side.
    return std::filesystem::path(INFERENCE_TEST_DATA) / "tiny_dynamic_batch.onnx";
}

std::filesystem::path fixedBatchPath()
{
    /// tiny_fixed_batch.onnx: f32 identity with shape [4,100] on each side.
    return std::filesystem::path(INFERENCE_TEST_DATA) / "tiny_fixed_batch.onnx";
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

TEST_F(ModelCatalogTest, RejectsNonFloat32NonVarsizedInputType)
{
    ModelCatalog catalog;
    auto ins = replaceFirstFieldType(fields(100, DataType::Type::FLOAT32), dt(DataType::Type::INT32));
    ASSERT_EXCEPTION_ERRORCODE(
        catalog.registerModel("m", identityPath(), ModelSchema{.inputs = ins, .outputs = fields(100, DataType::Type::FLOAT32)}),
        NES::ErrorCode::CannotLoadModel);
}

TEST_F(ModelCatalogTest, RejectsNonFloat32NonVarsizedOutputType)
{
    ModelCatalog catalog;
    auto outs = replaceFirstFieldType(fields(100, DataType::Type::FLOAT32), dt(DataType::Type::INT64));
    ASSERT_EXCEPTION_ERRORCODE(
        catalog.registerModel("m", identityPath(), ModelSchema{.inputs = fields(100, DataType::Type::FLOAT32), .outputs = outs}),
        NES::ErrorCode::CannotLoadModel);
}

TEST_F(ModelCatalogTest, RejectsVarsizedMixedWithSiblings)
{
    ModelCatalog catalog;
    auto mixedInputs
        = std::
              vector{UnqualifiedUnboundField{Identifier::parse("blob"), dt(DataType::Type::VARSIZED)}, UnqualifiedUnboundField{Identifier::parse("tail"), dt(DataType::Type::FLOAT32)}}
        | std::ranges::to<ModelFieldList>();
    ASSERT_EXCEPTION_ERRORCODE(
        catalog.registerModel("m", identityPath(), ModelSchema{.inputs = mixedInputs, .outputs = fields(100, DataType::Type::FLOAT32)}),
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

/// A dynamic batch dimension resolves to 1, so the model registers against a schema
/// describing a single sample.
TEST_F(ModelCatalogTest, RegistersModelWithDynamicBatchDimension)
{
    ModelCatalog catalog;
    ASSERT_NO_THROW(catalog.registerModel(
        "m",
        dynamicBatchPath(),
        ModelSchema{.inputs = fields(100, DataType::Type::FLOAT32), .outputs = fields(100, DataType::Type::FLOAT32)}));
}

/// The declared schema is still validated against the resolved shape: a schema written
/// for a batch of 4 no longer matches once the dynamic dimension has become 1.
TEST_F(ModelCatalogTest, RejectsSchemaThatDoesNotMatchResolvedBatchDimension)
{
    ModelCatalog catalog;
    ASSERT_EXCEPTION_ERRORCODE(
        catalog.registerModel(
            "m",
            dynamicBatchPath(),
            ModelSchema{.inputs = fields(400, DataType::Type::FLOAT32), .outputs = fields(400, DataType::Type::FLOAT32)}),
        NES::ErrorCode::CannotLoadModel);
}

/// A model wanting several samples per invocation cannot be driven one tuple at a time.
TEST_F(ModelCatalogTest, RejectsModelWithFixedBatchDimension)
{
    ModelCatalog catalog;
    ASSERT_EXCEPTION_ERRORCODE(
        catalog.registerModel(
            "m",
            fixedBatchPath(),
            ModelSchema{.inputs = fields(400, DataType::Type::FLOAT32), .outputs = fields(400, DataType::Type::FLOAT32)}),
        NES::ErrorCode::CannotLoadModel);
}

/// NOLINTEND(readability-magic-numbers)

}
