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

#include <SourceCatalogs/SourceCatalog.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>

#include <BaseUnitTest.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <DataTypes/DataType.hpp>
#include <gtest/gtest.h>

class SourceCatalogTest : public NES::Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        NES::Logger::setupLogging("QueryEngineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup QueryEngineTest test class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

TEST_F(SourceCatalogTest, AddInspectLogicalSource)
{
    auto sourceCatalog = NES::Catalogs::Source::SourceCatalog{};
    auto schema = NES::Schema{};
    schema.addField("stringField", NES::DataType::Type::VARSIZED);
    schema.addField("intField", NES::DataType::Type::INT32);

    const auto *const sourceName = "testSource";

    const auto sourceOpt = sourceCatalog.addLogicalSource(sourceName, schema);
    ASSERT_TRUE(sourceOpt.has_value());
    auto& source = *sourceOpt;

    ASSERT_TRUE(sourceCatalog.containsLogicalSource(source));


}
