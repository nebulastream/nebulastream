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

#include <string>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <rfl/Generic.hpp>
#include <rfl/json.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

SemanticModelConfig validConfig()
{
    SemanticModelConfig config;
    config.endpoint = "http://localhost:11434/v1";
    config.modelName = "llama3.1:8b";
    config.steps = {SemanticStep{
        .kind = SemanticStep::Kind::MAP,
        .prompt = "Classify the sentiment as POSITIVE or NEGATIVE",
        .outputColumn = "SENTIMENT",
        .outputValues = {"POSITIVE", "NEGATIVE"},
        .defaultValue = ""}};
    return config;
}

SemanticModelSchema validSchema()
{
    return SemanticModelSchema{
        .inputs = SemanticFieldList{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}},
        .outputs = SemanticFieldList{UnqualifiedUnboundField{Identifier::parse("sentiment"), DataType::Type::VARSIZED}}};
}

SemanticModelConfig filterConfig(std::string prompt = "The review is positive")
{
    auto config = validConfig();
    config.steps = {SemanticStep{
        .kind = SemanticStep::Kind::FILTER, .prompt = std::move(prompt), .outputColumn = {}, .outputValues = {}, .defaultValue = {}}};
    config.fusion = true;
    return config;
}

SemanticModelSchema filterSchema()
{
    return SemanticModelSchema{
        .inputs = SemanticFieldList{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}}, .outputs = {}};
}

}

class SemanticModelCatalogTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("SemanticModelCatalogTest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(SemanticModelCatalogTest, RegistersAndLoadsModel)
{
    SemanticModelCatalog catalog;
    catalog.registerModel("sentiment", validConfig(), validSchema());
    ASSERT_TRUE(catalog.hasModel("sentiment"));
    const auto loaded = catalog.load("sentiment");
    EXPECT_EQ(loaded.getName(), "sentiment");
    EXPECT_EQ(loaded.getConfig(), validConfig());
    EXPECT_EQ(loaded.getSchema(), validSchema());
}

TEST_F(SemanticModelCatalogTest, RejectsDuplicateName)
{
    SemanticModelCatalog catalog;
    catalog.registerModel("sentiment", validConfig(), validSchema());
    auto changed = validConfig();
    changed.modelName = "other";
    ASSERT_EXCEPTION_ERRORCODE(catalog.registerModel("sentiment", changed, validSchema()), ErrorCode::SemanticModelAlreadyExists);
    /// The first registration is kept, not overwritten.
    EXPECT_EQ(catalog.load("sentiment").getConfig().modelName, "llama3.1:8b");
}

TEST_F(SemanticModelCatalogTest, UnknownNamesThrow)
{
    SemanticModelCatalog catalog;
    EXPECT_FALSE(catalog.hasModel("missing"));
    ASSERT_EXCEPTION_ERRORCODE((void)catalog.load("missing"), ErrorCode::UnknownSemanticModelName);
    ASSERT_EXCEPTION_ERRORCODE(catalog.removeModel("missing"), ErrorCode::UnknownSemanticModelName);
}

TEST_F(SemanticModelCatalogTest, RemoveMakesModelUnknown)
{
    SemanticModelCatalog catalog;
    catalog.registerModel("a", validConfig(), validSchema());
    catalog.registerModel("b", validConfig(), validSchema());
    EXPECT_EQ(catalog.getModelNames().size(), 2);
    catalog.removeModel("a");
    EXPECT_FALSE(catalog.hasModel("a"));
    EXPECT_EQ(catalog.getRegisteredModels().size(), 1);
}

TEST_F(SemanticModelCatalogTest, RejectsInvalidConfigurations)
{
    struct Case
    {
        std::string description;
        SemanticModelConfig config;
    };

    std::vector<Case> cases;
    const auto addCase = [&cases](std::string description, auto mutate)
    {
        auto config = validConfig();
        mutate(config);
        cases.push_back(Case{.description = std::move(description), .config = std::move(config)});
    };
    addCase("empty endpoint", [](SemanticModelConfig& config) { config.endpoint.clear(); });
    addCase("empty model name", [](SemanticModelConfig& config) { config.modelName.clear(); });
    addCase("empty prompt", [](SemanticModelConfig& config) { config.steps.front().prompt.clear(); });
    addCase("unknown backend", [](SemanticModelConfig& config) { config.backend = "grpc"; });
    addCase("unknown mock behaviour", [](SemanticModelConfig& config) { config.backend = "mock"; });
    addCase("batch size zero", [](SemanticModelConfig& config) { config.batchSize = 0; });
    addCase("batch size above one", [](SemanticModelConfig& config) { config.batchSize = 2; });
    addCase("max concurrency zero", [](SemanticModelConfig& config) { config.maxConcurrency = 0; });
    addCase(
        "more steps than outputs",
        [](SemanticModelConfig& config)
        {
            config.steps.push_back(config.steps.front());
            config.steps.back().outputColumn = "OTHER";
        });

    for (const auto& [description, config] : cases)
    {
        SCOPED_TRACE(description);
        SemanticModelCatalog catalog;
        ASSERT_EXCEPTION_ERRORCODE(catalog.registerModel("m", config, validSchema()), ErrorCode::InvalidSemanticModel);
        EXPECT_FALSE(catalog.hasModel("m"));
    }
}

/// A model without OUTPUT is a filter model: one FILTER step and no column.
TEST_F(SemanticModelCatalogTest, RegistersFilterModelWithoutOutputs)
{
    SemanticModelCatalog catalog;
    catalog.registerModel("positive", filterConfig(), filterSchema());
    const auto loaded = catalog.load("positive");
    EXPECT_TRUE(loaded.hasFilterStep());
    EXPECT_EQ(loaded.getSchema().outputs.size(), 0);
    EXPECT_FALSE(catalog.load("positive").getConfig().steps.empty());

    catalog.registerModel("sentiment", validConfig(), validSchema());
    EXPECT_FALSE(catalog.load("sentiment").hasFilterStep());
}

TEST_F(SemanticModelCatalogTest, RejectsInvalidFilterModels)
{
    const auto expectRejected = [](const std::string& description, const SemanticModelConfig& config, const SemanticModelSchema& schema)
    {
        SCOPED_TRACE(description);
        SemanticModelCatalog catalog;
        ASSERT_EXCEPTION_ERRORCODE(catalog.registerModel("m", config, schema), ErrorCode::InvalidSemanticModel);
    };

    auto withValues = filterConfig();
    withValues.steps.front().outputValues = {"YES"};
    expectRejected("filter with output values", withValues, filterSchema());

    auto withDefault = filterConfig();
    withDefault.steps.front().defaultValue = "NO";
    expectRejected("filter with default value", withDefault, filterSchema());

    expectRejected("filter with an output field", filterConfig(), validSchema());
    expectRejected("map without an output field", validConfig(), filterSchema());

    auto noSteps = filterConfig();
    noSteps.steps.clear();
    expectRejected("no steps at all", noSteps, filterSchema());

    auto emptyPrompt = filterConfig("");
    expectRejected("filter with empty prompt", emptyPrompt, filterSchema());
}

TEST_F(SemanticModelCatalogTest, RejectsNonTextFields)
{
    SemanticModelCatalog catalog;
    auto numericInput = validSchema();
    numericInput.inputs = SemanticFieldList{UnqualifiedUnboundField{Identifier::parse("rating"), DataType::Type::UINT64}};
    ASSERT_EXCEPTION_ERRORCODE(catalog.registerModel("m", validConfig(), numericInput), ErrorCode::InvalidSemanticModel);

    auto numericOutput = validSchema();
    numericOutput.outputs = SemanticFieldList{UnqualifiedUnboundField{Identifier::parse("score"), DataType::Type::FLOAT32}};
    ASSERT_EXCEPTION_ERRORCODE(catalog.registerModel("m", validConfig(), numericOutput), ErrorCode::InvalidSemanticModel);
}

TEST_F(SemanticModelCatalogTest, MockBackendNeedsAKnownBehaviour)
{
    SemanticModelCatalog catalog;
    auto config = validConfig();
    config.backend = "mock";
    for (const auto* behaviour : {"echo", "label:POSITIVE", "unparseable", "fail"})
    {
        config.endpoint = behaviour;
        catalog.registerModel(behaviour, config, validSchema());
    }
    EXPECT_EQ(catalog.getModelNames().size(), 4);
}

TEST_F(SemanticModelCatalogTest, ReflectionRoundTrip)
{
    SemanticModelCatalog catalog;
    auto config = validConfig();
    config.payloadFormat = PayloadFormat::JSON_OBJECT;
    config.datasetPrompt = "Movie reviews from Rotten Tomatoes";
    config.apiKeyEnvVar = "OPENAI_API_KEY";
    catalog.registerModel("sentiment", config, validSchema());
    const auto model = catalog.load("sentiment");

    const ReflectionContext context;
    EXPECT_EQ(context.unreflect<RegisteredSemanticModel>(context.reflect(model)), model);
}

TEST_F(SemanticModelCatalogTest, ReflectionRoundTripKeepsFilterStepsAndFusion)
{
    SemanticModelCatalog catalog;
    catalog.registerModel("positive", filterConfig(), filterSchema());
    const auto model = catalog.load("positive");
    ASSERT_TRUE(model.getConfig().fusion);

    const ReflectionContext context;
    const auto roundTripped = context.unreflect<RegisteredSemanticModel>(context.reflect(model));
    EXPECT_EQ(roundTripped, model);
    EXPECT_TRUE(roundTripped.getConfig().fusion);
    EXPECT_EQ(roundTripped.getConfig().steps.front().kind, SemanticStep::Kind::FILTER);
}

/// Steps concatenate upstream first, filters are renumbered across both models, outputs concatenate,
/// and inputs and transport come from the upstream model.
TEST_F(SemanticModelCatalogTest, FuseConcatenatesStepsAndRenumbersFilters)
{
    SemanticModelCatalog catalog;
    catalog.registerModel("positive", filterConfig("p"), filterSchema());
    catalog.registerModel("acting", filterConfig("q"), filterSchema());
    auto mapConfig = validConfig();
    mapConfig.fusion = true;
    catalog.registerModel("sentiment", mapConfig, validSchema());

    const auto filters = fuseSemanticModels(catalog.load("positive"), catalog.load("acting"));
    EXPECT_EQ(filters.getName(), "positive+acting");
    ASSERT_EQ(filters.getConfig().steps.size(), 2);
    EXPECT_EQ(filters.getConfig().steps[0].outputColumn, "__filter_0");
    EXPECT_EQ(filters.getConfig().steps[1].outputColumn, "__filter_1");
    EXPECT_EQ(filters.getConfig().steps[1].prompt, "q");
    EXPECT_EQ(filters.getSchema().outputs.size(), 0);
    EXPECT_TRUE(filters.hasFilterStep());

    const auto chain = fuseSemanticModels(catalog.load("sentiment"), filters);
    EXPECT_EQ(chain.getName(), "sentiment+positive+acting");
    ASSERT_EQ(chain.getConfig().steps.size(), 3);
    EXPECT_EQ(chain.getConfig().steps[0].kind, SemanticStep::Kind::MAP);
    EXPECT_EQ(chain.getConfig().steps[0].outputColumn, "SENTIMENT");
    EXPECT_EQ(chain.getConfig().steps[2].outputColumn, "__filter_1");
    EXPECT_EQ(chain.getSchema().outputs, validSchema().outputs);
    EXPECT_EQ(chain.getSchema().inputs, validSchema().inputs);
    EXPECT_EQ(chain.getConfig().endpoint, mapConfig.endpoint);
}

TEST_F(SemanticModelCatalogTest, FuseRejectsCollidingOutputs)
{
    SemanticModelCatalog catalog;
    catalog.registerModel("a", validConfig(), validSchema());
    catalog.registerModel("b", validConfig(), validSchema());
    ASSERT_EXCEPTION_ERRORCODE((void)fuseSemanticModels(catalog.load("a"), catalog.load("b")), ErrorCode::InvalidSemanticModel);
}

/// Wire payloads are untrusted: an out-of-range enum value must fail deserialization rather than
/// become an invalid enumerator.
TEST_F(SemanticModelCatalogTest, UnreflectRejectsOutOfRangeEnums)
{
    SemanticModelCatalog catalog;
    catalog.registerModel("sentiment", validConfig(), validSchema());
    const ReflectionContext context;
    const auto json = rfl::json::write(*context.reflect(catalog.load("sentiment")));

    const auto tampered = [&json](const std::string& field, const std::string& value)
    {
        const auto key = fmt::format(R"("{}":0)", field);
        const auto position = json.find(key);
        EXPECT_NE(position, std::string::npos) << key << " not in " << json;
        auto result = json;
        result.replace(position, key.size(), fmt::format(R"("{}":{})", field, value));
        return Reflected{rfl::json::read<rfl::Generic>(result).value()};
    };

    ASSERT_EXCEPTION_ERRORCODE(
        (void)context.unreflect<RegisteredSemanticModel>(tampered("payloadFormat", "7")), ErrorCode::CannotDeserialize);
    ASSERT_EXCEPTION_ERRORCODE((void)context.unreflect<RegisteredSemanticModel>(tampered("kind", "-1")), ErrorCode::CannotDeserialize);
}

}
