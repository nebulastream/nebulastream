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
#include <system_error>
#include <vector>
#include <unistd.h>

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

/// Owns a real on-disk placeholder .so: registration only checks the path exists, so an empty file suffices. Also owns a
/// directory with the Python module `discount` (and the package `pkg.discount`), which Inline UDFs import when the catalog
/// compiles them at registration.
/// NOLINTNEXTLINE(misc-use-internal-linkage)
class UdfCatalogTest : public ::testing::Test
{
protected:
    std::filesystem::path libPath;
    std::filesystem::path moduleDirectory;

    void SetUp() override
    {
        libPath = std::filesystem::temp_directory_path() / "nes_udf_catalog_test_lib.so";
        std::ofstream{libPath} << "placeholder";
        /// Per process and test: ctest runs the tests in parallel.
        moduleDirectory = std::filesystem::temp_directory_path()
            / ("nes_udf_catalog_test_modules_" + std::to_string(::getpid()) + "_"
               + ::testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::create_directories(moduleDirectory / "pkg");
        const auto* const module = "def apply_discount(price: float) -> float:\n    return price * 0.9\n";
        std::ofstream{moduleDirectory / "discount.py"} << module;
        std::ofstream{moduleDirectory / "pkg" / "__init__.py"} << "";
        std::ofstream{moduleDirectory / "pkg" / "discount.py"} << module;
    }

    void TearDown() override
    {
        std::error_code errorCode;
        std::filesystem::remove(libPath, errorCode);
        std::filesystem::remove_all(moduleDirectory, errorCode);
    }

    [[nodiscard]] UdfCatalog catalogWithModules() const { return UdfCatalog{{moduleDirectory.string()}}; }
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
         dt(DataType::Type::VARSIZED),
         dt(DataType::Type::CHAR)},
        dt(DataType::Type::INT32)));
    EXPECT_TRUE(catalog.hasUdf("wide"));
}

TEST_F(UdfCatalogTest, RejectsUnsupportedArgumentType)
{
    UdfCatalog catalog;
    ASSERT_EXCEPTION_ERRORCODE(
        catalog.registerUdf("bad", libPath, "m.f", {dt(DataType::Type::UNDEFINED)}, dt(DataType::Type::FLOAT64)),
        NES::ErrorCode::CannotLoadUdf);
}

TEST_F(UdfCatalogTest, RejectsUnsupportedReturnType)
{
    UdfCatalog catalog;
    ASSERT_EXCEPTION_ERRORCODE(
        catalog.registerUdf("bad", libPath, "m.f", {dt(DataType::Type::INT32)}, dt(DataType::Type::UNDEFINED)),
        NES::ErrorCode::CannotLoadUdf);
}

TEST_F(UdfCatalogTest, AcceptsLibraryPathMissingLocally)
{
    UdfCatalog catalog;
    ASSERT_NO_THROW(catalog.registerUdf(
        "missing", std::filesystem::temp_directory_path() / "does_not_exist_nes_udf.so", "m.f", {}, dt(DataType::Type::INT32)));
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
    auto catalog = catalogWithModules();
    ASSERT_NO_THROW(catalog.registerUdf(
        "discount", {}, "discount.apply_discount", {dt(DataType::Type::FLOAT64)}, dt(DataType::Type::FLOAT64), UdfExecution::Inline));
    const auto descriptor = catalog.load("discount");
    EXPECT_EQ(descriptor.getExecution(), UdfExecution::Inline);
    EXPECT_EQ(descriptor.getEntrypoint(), "discount.apply_discount");
    EXPECT_TRUE(descriptor.getPath().empty());
    /// Compiled at registration.
    ASSERT_TRUE(descriptor.getCompiled().has_value());
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    EXPECT_FALSE(descriptor.getCompiled()->pipelineBitcode.empty());
}

TEST_F(UdfCatalogTest, RejectsCodonUdfWhoseModuleIsMissing)
{
    UdfCatalog catalog;
    ASSERT_EXCEPTION_ERRORCODE(
        catalog.registerUdf(
            "missing", {}, "no_such_module.f", {dt(DataType::Type::FLOAT64)}, dt(DataType::Type::FLOAT64), UdfExecution::Inline),
        NES::ErrorCode::QueryCompilerError);
    EXPECT_FALSE(catalog.hasUdf("missing"));
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
            catalog.registerUdf("bad", {}, entrypoint, {dt(DataType::Type::FLOAT64)}, dt(DataType::Type::FLOAT64), UdfExecution::Inline),
            NES::ErrorCode::InvalidStatement);
    }
}

TEST_F(UdfCatalogTest, RejectsCodonUdfWithoutArguments)
{
    UdfCatalog catalog;
    ASSERT_EXCEPTION_ERRORCODE(
        catalog.registerUdf("bad", {}, "m.f", {}, dt(DataType::Type::FLOAT64), UdfExecution::Inline), NES::ErrorCode::InvalidStatement);
}

TEST_F(UdfCatalogTest, CodonDescriptorSurvivesReflectionRoundTrip)
{
    auto catalog = catalogWithModules();
    catalog.registerUdf(
        "discount", {}, "pkg.discount.apply_discount", {dt(DataType::Type::FLOAT64)}, dt(DataType::Type::FLOAT64), UdfExecution::Inline);
    const auto original = catalog.load("discount");

    const ReflectionContext context;
    const auto roundTripped = context.unreflect<UdfDescriptor>(context.reflect(original));

    EXPECT_EQ(original, roundTripped);
    EXPECT_EQ(roundTripped.getExecution(), UdfExecution::Inline);
}

TEST_F(UdfCatalogTest, InlineSourceSurvivesReflectionRoundTrip)
{
    UdfCatalog catalog;
    catalog.registerUdf(
        "twice",
        {},
        {},
        {dt(DataType::Type::INT64)},
        dt(DataType::Type::INT64),
        UdfExecution::Inline,
        InlineUdfSource{.parameterNames = {"x"}, .body = "return x * 2"});
    const auto original = catalog.load("twice");
    ASSERT_TRUE(original.getSource().has_value());

    const ReflectionContext context;
    const auto roundTripped = context.unreflect<UdfDescriptor>(context.reflect(original));

    EXPECT_EQ(original, roundTripped);
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    EXPECT_EQ(roundTripped.getSource()->body, "return x * 2");
}

TEST_F(UdfCatalogTest, InlineSourceWithWrongParameterCountIsRejected)
{
    UdfCatalog catalog;
    ASSERT_EXCEPTION_ERRORCODE(
        catalog.registerUdf(
            "bad",
            {},
            {},
            {dt(DataType::Type::INT64)},
            dt(DataType::Type::INT64),
            UdfExecution::Inline,
            InlineUdfSource{.parameterNames = {"x", "y"}, .body = "return x"}),
        NES::ErrorCode::InvalidStatement);
}

TEST_F(UdfCatalogTest, FromSourceBuildsADescriptorWithoutTheCatalog)
{
    const auto descriptor = UdfDescriptor::fromSource(
        "internal",
        {},
        InlineUdfSource{.parameterNames = {"x"}, .body = "return x"},
        {dt(DataType::Type::INT64)},
        dt(DataType::Type::INT64),
        UdfExecution::Inline);
    EXPECT_TRUE(descriptor.getEntrypoint().empty());
    EXPECT_TRUE(descriptor.getSource().has_value());
}

TEST(UdfEntrypointTest, SplitsAtTheLastDot)
{
    const auto simple = splitEntrypoint("currency.to_euro");
    ASSERT_TRUE(simple.has_value());
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    EXPECT_EQ(simple->first, "currency");
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    EXPECT_EQ(simple->second, "to_euro");

    const auto package = splitEntrypoint("pkg.sub.fn");
    ASSERT_TRUE(package.has_value());
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    EXPECT_EQ(package->first, "pkg.sub");
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    EXPECT_EQ(package->second, "fn");

    EXPECT_FALSE(splitEntrypoint("nodot").has_value());
    EXPECT_FALSE(splitEntrypoint(".fn").has_value());
    EXPECT_FALSE(splitEntrypoint("module.").has_value());
}
}
