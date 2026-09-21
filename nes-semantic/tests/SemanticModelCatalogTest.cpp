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

#include <SemanticModelCatalog.hpp>

#include <cstdlib>
#include <optional>
#include <ranges>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifiers.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <SemanticModelConfig.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>

namespace NES
{

namespace
{

SemanticModelConfig validConfig()
{
    return SemanticModelConfig{
        .baseUrl = "http://host.docker.internal:11434/v1",
        .model = "gemma3:27b",
        .apiKeyEnv = std::nullopt,
        .steps = {SemanticStep{
            .kind = SemanticStep::Kind::MAP,
            .prompt = "Classify the sentiment as POSITIVE or NEGATIVE",
            .outputValues = {},
            .defaultValue = ""}}};
}

SemanticModelSchema validSchema()
{
    return SemanticModelSchema{
        .inputs = std::vector{UnqualifiedUnboundField{
                      Identifier::parse("description"), DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}}}
            | std::ranges::to<SemanticModelFieldList>(),
        .outputs = std::vector{UnqualifiedUnboundField{
                       Identifier::parse("sentiment"), DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}}}
            | std::ranges::to<SemanticModelFieldList>()};
}

}

class SemanticModelCatalogTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("SemanticModelCatalogTest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(SemanticModelCatalogTest, RegistersModelWithValidConfig)
{
    SemanticModelCatalog catalog;
    ASSERT_NO_THROW(catalog.registerModel("sentiment", validConfig(), validSchema()));
    EXPECT_TRUE(catalog.hasModel("sentiment"));
}

TEST_F(SemanticModelCatalogTest, RejectsInvalidBaseUrl)
{
    SemanticModelCatalog catalog;
    auto config = validConfig();
    config.baseUrl = "not a url";
    ASSERT_EXCEPTION_ERRORCODE(catalog.registerModel("m", config, validSchema()), NES::ErrorCode::CannotLoadModel);
}

TEST_F(SemanticModelCatalogTest, RejectsEmptyModel)
{
    SemanticModelCatalog catalog;
    auto config = validConfig();
    config.model = "";
    ASSERT_EXCEPTION_ERRORCODE(catalog.registerModel("m", config, validSchema()), NES::ErrorCode::CannotLoadModel);
}

TEST_F(SemanticModelCatalogTest, RejectsEmptyPrompt)
{
    SemanticModelCatalog catalog;
    auto config = validConfig();
    config.steps.front().prompt = "";
    ASSERT_EXCEPTION_ERRORCODE(catalog.registerModel("m", config, validSchema()), NES::ErrorCode::CannotLoadModel);
}

TEST_F(SemanticModelCatalogTest, RejectsStepOutputArityMismatch)
{
    SemanticModelCatalog catalog;
    auto config = validConfig();
    config.steps.push_back(
        SemanticStep{.kind = SemanticStep::Kind::MAP, .prompt = "second step", .outputValues = {}, .defaultValue = ""});
    ASSERT_EXCEPTION_ERRORCODE(catalog.registerModel("m", config, validSchema()), NES::ErrorCode::CannotLoadModel);
}

TEST_F(SemanticModelCatalogTest, RejectsApiKeyEnvThatIsNotSet)
{
    SemanticModelCatalog catalog;
    auto config = validConfig();
    config.apiKeyEnv = "NES_SEMANTIC_TEST_DEFINITELY_UNSET_VAR";
    ASSERT_EQ(std::getenv(config.apiKeyEnv->c_str()), nullptr);
    ASSERT_EXCEPTION_ERRORCODE(catalog.registerModel("m", config, validSchema()), NES::ErrorCode::CannotLoadModel);
}

TEST_F(SemanticModelCatalogTest, AcceptsApiKeyEnvThatIsSet)
{
    ASSERT_EQ(setenv("NES_SEMANTIC_TEST_SET_VAR", "secret", 1), 0);
    SemanticModelCatalog catalog;
    auto config = validConfig();
    config.apiKeyEnv = "NES_SEMANTIC_TEST_SET_VAR";
    ASSERT_NO_THROW(catalog.registerModel("m", config, validSchema()));
    unsetenv("NES_SEMANTIC_TEST_SET_VAR");
}

TEST_F(SemanticModelCatalogTest, RejectsBatchSizeGreaterThanOne)
{
    SemanticModelCatalog catalog;
    auto config = validConfig();
    config.batchSize = 2;
    ASSERT_EXCEPTION_ERRORCODE(catalog.registerModel("m", config, validSchema()), NES::ErrorCode::InvalidConfigParameter);
}

TEST_F(SemanticModelCatalogTest, RejectsDuplicateModelName)
{
    SemanticModelCatalog catalog;
    catalog.registerModel("sentiment", validConfig(), validSchema());
    ASSERT_EXCEPTION_ERRORCODE(
        catalog.registerModel("sentiment", validConfig(), validSchema()), NES::ErrorCode::ModelAlreadyExists);
}

TEST_F(SemanticModelCatalogTest, DoesNotContainUnregisteredModel)
{
    SemanticModelCatalog catalog;
    EXPECT_FALSE(catalog.hasModel("does-not-exist"));
}

TEST_F(SemanticModelCatalogTest, RemoveModelMakesItUnknown)
{
    SemanticModelCatalog catalog;
    catalog.registerModel("sentiment", validConfig(), validSchema());
    catalog.removeModel("sentiment");
    EXPECT_FALSE(catalog.hasModel("sentiment"));
}

TEST_F(SemanticModelCatalogTest, LoadThrowsForUnknownModel)
{
    SemanticModelCatalog catalog;
    ASSERT_EXCEPTION_ERRORCODE((void)catalog.load("does-not-exist"), NES::ErrorCode::UnknownModelName);
}

TEST_F(SemanticModelCatalogTest, LoadReturnsRegisteredConfig)
{
    SemanticModelCatalog catalog;
    catalog.registerModel("sentiment", validConfig(), validSchema());
    const auto loaded = catalog.load("sentiment");
    EXPECT_EQ(loaded.getName(), "sentiment");
    EXPECT_EQ(loaded.getConfig(), validConfig());
}

TEST_F(SemanticModelCatalogTest, GetModelNamesListsAllRegisteredModels)
{
    SemanticModelCatalog catalog;
    catalog.registerModel("a", validConfig(), validSchema());
    catalog.registerModel("b", validConfig(), validSchema());
    const auto names = catalog.getModelNames();
    EXPECT_EQ(names.size(), 2);
}

}
