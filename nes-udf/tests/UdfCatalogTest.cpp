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

/// Fixture that owns a real on-disk placeholder for the UDF `.so`. Registration
/// only checks that the path names an existing regular file (loading is deferred
/// to lowering), so an empty temp file is a sufficient stand-in.
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

    const Reflected reflected = Reflector<UdfDescriptor>{}(original);
    const ReflectionContext context;
    const auto roundTripped = context.unreflect<UdfDescriptor>(reflected);

    EXPECT_EQ(original, roundTripped);
}

}
