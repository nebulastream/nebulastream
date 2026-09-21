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

#include <UdfCatalog.hpp>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <BaseUnitTest.hpp>

#include <DataTypes/DataType.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

namespace
{

DataType dt(const DataType::Type type)
{
    return DataType{type, DataType::NULLABLE::NOT_NULLABLE};
}

}

/// Owns a real on-disk placeholder .so: registration only checks the path exists, so an empty file suffices.
class UdfCatalogTest : public ::testing::Test
{
protected:
    std::filesystem::path libPath;

    void SetUp() override
    {
        libPath = std::filesystem::temp_directory_path() / "nes_udf_catalog_test_lib.so";
        std::ofstream{libPath} << "placeholder";
    }

    void TearDown() override
    {
        std::error_code ec;
        std::filesystem::remove(libPath, ec);
    }
};

TEST_F(UdfCatalogTest, RegistersAndFindsUdf)
{
    UdfCatalog catalog;
    ASSERT_NO_THROW(catalog.registerUdf(
        "to_euro", libPath, "currency.to_euro", {dt(DataType::Type::FLOAT64), dt(DataType::Type::VARSIZED)}, dt(DataType::Type::FLOAT64)));
    EXPECT_TRUE(catalog.hasUdf("to_euro"));
    EXPECT_FALSE(catalog.hasUdf("unregistered"));
}

TEST_F(UdfCatalogTest, LoadReturnsRegisteredMetadata)
{
    UdfCatalog catalog;
    catalog.registerUdf(
        "to_euro", libPath, "currency.to_euro", {dt(DataType::Type::FLOAT64), dt(DataType::Type::VARSIZED)}, dt(DataType::Type::FLOAT64));

    const auto descriptor = catalog.load("to_euro");
    EXPECT_EQ(descriptor.getName(), "to_euro");
    EXPECT_EQ(descriptor.getPath(), libPath);
    EXPECT_EQ(descriptor.getEntrypoint(), "currency.to_euro");
    ASSERT_EQ(descriptor.getArgTypes().size(), 2U);
    EXPECT_EQ(descriptor.getArgTypes().at(0), dt(DataType::Type::FLOAT64));
    EXPECT_EQ(descriptor.getArgTypes().at(1), dt(DataType::Type::VARSIZED));
    EXPECT_EQ(descriptor.getReturnType(), dt(DataType::Type::FLOAT64));
}

TEST_F(UdfCatalogTest, LoadUnknownUdfThrows)
{
    const UdfCatalog catalog;
    ASSERT_EXCEPTION_ERRORCODE((void)catalog.load("nope"), NES::ErrorCode::UnknownUdf);
}

TEST_F(UdfCatalogTest, AcceptsAllSupportedTypes)
{
    UdfCatalog catalog;
    ASSERT_NO_THROW(catalog.registerUdf(
        "wide",
        libPath,
        "m.f",
        {dt(DataType::Type::BOOLEAN),
         dt(DataType::Type::INT8),
         dt(DataType::Type::INT64),
         dt(DataType::Type::UINT32),
         dt(DataType::Type::FLOAT32),
         dt(DataType::Type::VARSIZED)},
        dt(DataType::Type::INT32)));
    EXPECT_TRUE(catalog.hasUdf("wide"));
}

TEST_F(UdfCatalogTest, RejectsUnsupportedArgumentType)
{
    UdfCatalog catalog;
    ASSERT_EXCEPTION_ERRORCODE(
        catalog.registerUdf("bad", libPath, "m.f", {dt(DataType::Type::CHAR)}, dt(DataType::Type::FLOAT64)), NES::ErrorCode::CannotLoadUdf);
}

TEST_F(UdfCatalogTest, RejectsUnsupportedReturnType)
{
    UdfCatalog catalog;
    ASSERT_EXCEPTION_ERRORCODE(
        catalog.registerUdf("bad", libPath, "m.f", {dt(DataType::Type::INT32)}, dt(DataType::Type::UNDEFINED)),
        NES::ErrorCode::CannotLoadUdf);
}

TEST_F(UdfCatalogTest, RejectsMissingLibraryPath)
{
    UdfCatalog catalog;
    ASSERT_EXCEPTION_ERRORCODE(
        catalog.registerUdf(
            "missing", std::filesystem::temp_directory_path() / "does_not_exist_nes_udf.so", "m.f", {}, dt(DataType::Type::INT32)),
        NES::ErrorCode::InvalidStatement);
}

TEST_F(UdfCatalogTest, RemoveUdf)
{
    UdfCatalog catalog;
    catalog.registerUdf("temp", libPath, "m.f", {}, dt(DataType::Type::INT32));
    ASSERT_TRUE(catalog.hasUdf("temp"));
    catalog.removeUdf("temp");
    EXPECT_FALSE(catalog.hasUdf("temp"));
}

TEST_F(UdfCatalogTest, ListsRegisteredUdfs)
{
    UdfCatalog catalog;
    catalog.registerUdf("a", libPath, "m.a", {}, dt(DataType::Type::INT32));
    catalog.registerUdf("b", libPath, "m.b", {}, dt(DataType::Type::INT32));
    EXPECT_EQ(catalog.getUdfNames().size(), 2U);
    EXPECT_EQ(catalog.getRegisteredUdfs().size(), 2U);
}

TEST_F(UdfCatalogTest, DescriptorSurvivesReflectionRoundTrip)
{
    UdfCatalog catalog;
    catalog.registerUdf(
        "to_euro", libPath, "currency.to_euro", {dt(DataType::Type::FLOAT64), dt(DataType::Type::VARSIZED)}, dt(DataType::Type::FLOAT64));
    const auto original = catalog.load("to_euro");

    const ReflectionContext context;
    const Reflected reflected = context.reflect(original);
    const auto roundTripped = context.unreflect<UdfDescriptor>(reflected);

    EXPECT_EQ(original, roundTripped);
}


TEST_F(UdfCatalogTest, RegistersCodonUdfWithoutLibraryPath)
{
    UdfCatalog catalog;
    ASSERT_NO_THROW(catalog.registerUdf(
        "discount", {}, "discount.apply_discount", {dt(DataType::Type::FLOAT64)}, dt(DataType::Type::FLOAT64), UdfExecution::Codon));
    const auto descriptor = catalog.load("discount");
    EXPECT_EQ(descriptor.getExecution(), UdfExecution::Codon);
    EXPECT_EQ(descriptor.getEntrypoint(), "discount.apply_discount");
    EXPECT_TRUE(descriptor.getPath().empty());
}

TEST_F(UdfCatalogTest, InProcessUdfKeepsInProcessExecution)
{
    UdfCatalog catalog;
    catalog.registerUdf("to_euro", libPath, "currency.to_euro", {dt(DataType::Type::FLOAT64)}, dt(DataType::Type::FLOAT64));
    EXPECT_EQ(catalog.load("to_euro").getExecution(), UdfExecution::InProcess);
}

TEST_F(UdfCatalogTest, RejectsCodonEntrypointWithoutModule)
{
    UdfCatalog catalog;
    for (const auto* entrypoint : {"apply_discount", ".apply_discount", "discount.", ""})
    {
        ASSERT_EXCEPTION_ERRORCODE(
            catalog.registerUdf("bad", {}, entrypoint, {dt(DataType::Type::FLOAT64)}, dt(DataType::Type::FLOAT64), UdfExecution::Codon),
            NES::ErrorCode::InvalidStatement);
    }
}

TEST_F(UdfCatalogTest, RejectsCodonUdfWithoutArguments)
{
    UdfCatalog catalog;
    ASSERT_EXCEPTION_ERRORCODE(
        catalog.registerUdf("bad", {}, "m.f", {}, dt(DataType::Type::FLOAT64), UdfExecution::Codon), NES::ErrorCode::InvalidStatement);
}

TEST_F(UdfCatalogTest, CodonDescriptorSurvivesReflectionRoundTrip)
{
    UdfCatalog catalog;
    catalog.registerUdf(
        "discount", {}, "pkg.discount.apply_discount", {dt(DataType::Type::FLOAT64)}, dt(DataType::Type::FLOAT64), UdfExecution::Codon);
    const auto original = catalog.load("discount");

    const ReflectionContext context;
    const auto roundTripped = context.unreflect<UdfDescriptor>(context.reflect(original));

    EXPECT_EQ(original, roundTripped);
    EXPECT_EQ(roundTripped.getExecution(), UdfExecution::Codon);
}

TEST(UdfEntrypointTest, SplitsAtTheLastDot)
{
    const auto simple = splitEntrypoint("currency.to_euro");
    ASSERT_TRUE(simple.has_value());
    EXPECT_EQ(simple->first, "currency");
    EXPECT_EQ(simple->second, "to_euro");

    const auto package = splitEntrypoint("pkg.sub.fn");
    ASSERT_TRUE(package.has_value());
    EXPECT_EQ(package->first, "pkg.sub");
    EXPECT_EQ(package->second, "fn");

    EXPECT_FALSE(splitEntrypoint("nodot").has_value());
    EXPECT_FALSE(splitEntrypoint(".fn").has_value());
    EXPECT_FALSE(splitEntrypoint("module.").has_value());
}
}
