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

#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SemanticMapNameLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statements/StatementHandler.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

/// Covers SEM_MAP up to plan construction: CREATE/SHOW/DROP SEMANTIC MODEL through binder,
/// handler and catalog, and the logical plan the parser builds for SEM_MAP queries. Resolution
/// of the placeholder against the catalog is the optimizer's job and is not exercised here.
/// Field names follow the Rotten Tomatoes reviews dataset used by the Python reference (d1).
class SemanticMapStatementTest : public Testing::BaseUnitTest
{
public:
    std::shared_ptr<SourceCatalog> sourceCatalog;
    std::shared_ptr<StatementBinder> binder;
    std::shared_ptr<SemanticModelCatalog> semanticModelCatalog;
    std::shared_ptr<SemanticModelStatementHandler> handler;

    static void SetUpTestSuite() { Logger::setupLogging("SemanticMapStatementTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        sourceCatalog = std::make_shared<SourceCatalog>();
        binder = std::make_shared<StatementBinder>(
            sourceCatalog,
            [](auto&& queryContext)
            { return AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(queryContext)>(queryContext)); });
        semanticModelCatalog = std::make_shared<SemanticModelCatalog>();
        handler = std::make_shared<SemanticModelStatementHandler>(semanticModelCatalog);
    }

    static constexpr auto SentimentModel
        = "CREATE SEMANTIC MODEL sentiment_clf "
          "INPUT (reviewText VARSIZED) OUTPUT (sentiment VARSIZED) "
          "SET ('Determine if the review is positive or negative' AS LLM.PROMPT, "
          "     'http://localhost:8000/v1' AS LLM.ENDPOINT, "
          "     'meta-llama/Llama-3.3-70B-Instruct' AS LLM.MODEL_NAME, "
          "     'POSITIVE, NEGATIVE,' AS LLM.OUTPUT_VALUES, "
          "     10 AS LLM.BATCH_SIZE, "
          "     'JSON_OBJECT' AS LLM.PAYLOAD_FORMAT, "
          "     'OPENAI_API_KEY' AS LLM.API_KEY_ENV, "
          "     'mock' AS LLM.BACKEND)";

    /// Binds a CREATE SEMANTIC MODEL statement and runs it through the handler.
    std::expected<CreateSemanticModelStatementResult, Exception> create(const std::string& sql) const
    {
        const auto statement = binder->parseAndBindSingle(sql);
        EXPECT_TRUE(statement.has_value()) << sql;
        EXPECT_TRUE(std::holds_alternative<CreateSemanticModelStatement>(*statement)) << sql;
        return handler->apply(std::get<CreateSemanticModelStatement>(*statement));
    }

    /// A minimal, valid statement with one SET entry replaced or added.
    static std::string withOptions(const std::string& output, const std::string& extraOptions)
    {
        return "CREATE SEMANTIC MODEL m INPUT (reviewText VARSIZED) OUTPUT (" + output
            + ") SET ('Classify' AS LLM.PROMPT, 'http://x' AS LLM.ENDPOINT, 'm' AS LLM.MODEL_NAME" + extraOptions + ")";
    }

    LogicalPlan bindQuery(const std::string& sql) const
    {
        const auto statement = binder->parseAndBindSingle(sql);
        EXPECT_TRUE(statement.has_value()) << sql;
        EXPECT_TRUE(std::holds_alternative<QueryStatement>(*statement)) << sql;
        return std::get<QueryStatement>(*statement).plan;
    }
};

TEST_F(SemanticMapStatementTest, BindCreateSemanticModelKeepsOptionsAsFlatStrings)
{
    const auto statement = binder->parseAndBindSingle(SentimentModel);
    ASSERT_TRUE(statement.has_value());
    ASSERT_TRUE(std::holds_alternative<CreateSemanticModelStatement>(*statement));

    const auto& create = std::get<CreateSemanticModelStatement>(*statement);
    EXPECT_EQ(create.name, "SENTIMENT_CLF");
    EXPECT_EQ(create.inputs.size(), 1);
    EXPECT_EQ(create.outputs.size(), 1);
    EXPECT_EQ(create.config.size(), 8);
    EXPECT_EQ(create.config.at(Identifier::parse("PROMPT")), "Determine if the review is positive or negative");
    EXPECT_EQ(create.config.at(Identifier::parse("BATCH_SIZE")), "10");
}

TEST_F(SemanticMapStatementTest, CreateRegistersTypedConfigInCatalog)
{
    const auto result = create(SentimentModel);
    ASSERT_TRUE(result.has_value()) << result.error().what();
    EXPECT_EQ(result->name, "SENTIMENT_CLF");
    EXPECT_EQ(result->endpoint, "http://localhost:8000/v1");
    EXPECT_EQ(result->modelName, "meta-llama/Llama-3.3-70B-Instruct");
    EXPECT_EQ(result->prompt, "Determine if the review is positive or negative");

    const auto model = semanticModelCatalog->load("SENTIMENT_CLF");
    const auto& config = model.getConfig();
    ASSERT_EQ(config.steps.size(), 1);
    const auto& step = config.steps.front();
    EXPECT_EQ(step.kind, SemanticStep::Kind::MAP);
    EXPECT_EQ(step.outputColumn, "SENTIMENT");
    /// Whitespace trimmed, trailing comma ignored.
    EXPECT_EQ(step.outputValues, (std::vector<std::string>{"POSITIVE", "NEGATIVE"}));
    EXPECT_EQ(step.defaultValue, "");
    EXPECT_EQ(config.payloadFormat, PayloadFormat::JSON_OBJECT);
    EXPECT_EQ(config.batchSize, 10);
    EXPECT_EQ(config.backend, "mock");
    ASSERT_TRUE(config.apiKeyEnvVar.has_value());
    EXPECT_EQ(config.apiKeyEnvVar.value(), "OPENAI_API_KEY");
}

TEST_F(SemanticMapStatementTest, CreateAppliesPythonReferenceDefaults)
{
    const auto result = create(withOptions("summary VARSIZED", ""));
    ASSERT_TRUE(result.has_value()) << result.error().what();

    const auto& config = semanticModelCatalog->load("M").getConfig();
    EXPECT_EQ(config.payloadFormat, PayloadFormat::SPACE_JOINED);
    EXPECT_EQ(config.batchSize, 1);
    EXPECT_EQ(config.maxConcurrency, 10);
    EXPECT_EQ(config.maxRetries, 2);
    EXPECT_EQ(config.maxWaitTime, std::chrono::milliseconds{1000});
    EXPECT_EQ(config.requestTimeout, std::chrono::seconds{600});
    EXPECT_EQ(config.backend, "http");
    EXPECT_FALSE(config.apiKeyEnvVar.has_value());
    EXPECT_TRUE(config.steps.front().outputValues.empty());
}

TEST_F(SemanticMapStatementTest, OptionNamesAreCaseInsensitive)
{
    const auto result = create(
        "CREATE SEMANTIC MODEL m INPUT (reviewText VARSIZED) OUTPUT (sentiment VARSIZED) "
        "SET ('Classify' AS llm.prompt, 'http://x' AS llm.endpoint, 'm' AS llm.model_name)");
    ASSERT_TRUE(result.has_value()) << result.error().what();
}

TEST_F(SemanticMapStatementTest, ShowAndDropSemanticModels)
{
    ASSERT_TRUE(create(SentimentModel).has_value());
    ASSERT_TRUE(create(withOptions("summary VARSIZED", "")).has_value());

    const auto show = binder->parseAndBindSingle("SHOW SEMANTIC MODELS");
    ASSERT_TRUE(show.has_value());
    ASSERT_TRUE(std::holds_alternative<ShowSemanticModelsStatement>(*show));
    EXPECT_EQ(handler->apply(std::get<ShowSemanticModelsStatement>(*show))->models.size(), 2);

    const auto drop = binder->parseAndBindSingle("DROP SEMANTIC MODEL WHERE NAME = 'M'");
    ASSERT_TRUE(drop.has_value());
    ASSERT_TRUE(std::holds_alternative<DropSemanticModelStatement>(*drop));
    const auto dropResult = handler->apply(std::get<DropSemanticModelStatement>(*drop));
    ASSERT_TRUE(dropResult.has_value());
    EXPECT_EQ(dropResult->name, "M");

    const auto remaining = handler->apply(std::get<ShowSemanticModelsStatement>(*show))->models;
    ASSERT_EQ(remaining.size(), 1);
    EXPECT_EQ(remaining.front().name, "SENTIMENT_CLF");

    /// Dropping again is an error, not a silent no-op.
    const auto dropAgain = handler->apply(std::get<DropSemanticModelStatement>(*drop));
    ASSERT_FALSE(dropAgain.has_value());
    EXPECT_EQ(dropAgain.error().code(), ErrorCode::UnknownSemanticModelName);
}

TEST_F(SemanticMapStatementTest, SemanticModelsAndPlainModelsDoNotShareANamespace)
{
    /// SHOW MODELS / DROP MODEL keep binding to the ONNX model statements.
    const auto showModels = binder->parseAndBindSingle("SHOW MODELS");
    ASSERT_TRUE(showModels.has_value());
    EXPECT_TRUE(std::holds_alternative<ShowModelsStatement>(*showModels));

    const auto dropModel = binder->parseAndBindSingle("DROP MODEL WHERE NAME = 'X'");
    ASSERT_TRUE(dropModel.has_value());
    EXPECT_TRUE(std::holds_alternative<DropModelStatement>(*dropModel));
}

TEST_F(SemanticMapStatementTest, CreateRejectsDuplicateName)
{
    ASSERT_TRUE(create(SentimentModel).has_value());
    const auto duplicate = create(SentimentModel);
    ASSERT_FALSE(duplicate.has_value());
    EXPECT_EQ(duplicate.error().code(), ErrorCode::SemanticModelAlreadyExists);
}

TEST_F(SemanticMapStatementTest, CreateRejectsInvalidDefinitions)
{
    struct Case
    {
        std::string description;
        std::string sql;
    };

    const std::vector<Case> cases{
        {"missing PROMPT",
         "CREATE SEMANTIC MODEL m INPUT (t VARSIZED) OUTPUT (o VARSIZED) SET ('http://x' AS LLM.ENDPOINT, 'm' AS LLM.MODEL_NAME)"},
        {"missing ENDPOINT",
         "CREATE SEMANTIC MODEL m INPUT (t VARSIZED) OUTPUT (o VARSIZED) SET ('p' AS LLM.PROMPT, 'm' AS LLM.MODEL_NAME)"},
        {"missing MODEL_NAME",
         "CREATE SEMANTIC MODEL m INPUT (t VARSIZED) OUTPUT (o VARSIZED) SET ('p' AS LLM.PROMPT, 'http://x' AS LLM.ENDPOINT)"},
        {"no SET clause at all", "CREATE SEMANTIC MODEL m INPUT (t VARSIZED) OUTPUT (o VARSIZED)"},
        {"unknown BACKEND", withOptions("o VARSIZED", ", 'onnx' AS LLM.BACKEND")},
        {"BATCH_SIZE of zero", withOptions("o VARSIZED", ", 0 AS LLM.BATCH_SIZE")},
        {"non-numeric BATCH_SIZE", withOptions("o VARSIZED", ", 'ten' AS LLM.BATCH_SIZE")},
        {"MAX_CONCURRENCY of zero", withOptions("o VARSIZED", ", 0 AS LLM.MAX_CONCURRENCY")},
        {"unknown PAYLOAD_FORMAT", withOptions("o VARSIZED", ", 'XML' AS LLM.PAYLOAD_FORMAT")},
        {"non-VARSIZED output", withOptions("o FLOAT32", "")},
        {"two outputs (fusion is not supported yet)", withOptions("o VARSIZED, p VARSIZED", "")},
    };

    for (const auto& [description, sql] : cases)
    {
        const auto result = create(sql);
        EXPECT_FALSE(result.has_value()) << "expected rejection: " << description;
        if (!result.has_value())
        {
            EXPECT_EQ(result.error().code(), ErrorCode::InvalidSemanticModel) << description << ": " << result.error().what();
        }
    }
    EXPECT_TRUE(semanticModelCatalog->getModelNames().empty());
}

TEST_F(SemanticMapStatementTest, BinderRejectsMalformedOptionKeys)
{
    /// Unqualified key: bindConfigOptions requires PREFIX.NAME.
    EXPECT_FALSE(binder->parseAndBindSingle("CREATE SEMANTIC MODEL m INPUT (t VARSIZED) OUTPUT (o VARSIZED) SET ('p' AS PROMPT)")
                     .has_value());
    /// Wrong namespace.
    EXPECT_FALSE(binder->parseAndBindSingle("CREATE SEMANTIC MODEL m INPUT (t VARSIZED) OUTPUT (o VARSIZED) SET ('p' AS SOURCE.PROMPT)")
                     .has_value());
    /// MODEL is a reserved keyword, which is why the option is called MODEL_NAME.
    EXPECT_FALSE(binder->parseAndBindSingle("CREATE SEMANTIC MODEL m INPUT (t VARSIZED) OUTPUT (o VARSIZED) SET ('x' AS LLM.MODEL)")
                     .has_value());
}

TEST_F(SemanticMapStatementTest, SemMapQueryBuildsUnresolvedPlaceholder)
{
    const auto plan = bindQuery("SELECT * FROM SEM_MAP(sentiment_clf, reviews) INTO result");

    const auto placeholders = getOperatorByType<SemanticMapNameLogicalOperator>(plan);
    ASSERT_EQ(placeholders.size(), 1);
    EXPECT_EQ(placeholders.front()->getModelName(), "SENTIMENT_CLF");
    EXPECT_EQ(placeholders.front()->getChildren().size(), 1);

    EXPECT_EQ(
        explain(plan, ExplainVerbosity::Short),
        "SINK(RESULT)\n"
        "  PROJECTION(fields: [*])\n"
        "    SEM_MAP_NAME(model: SENTIMENT_CLF)\n"
        "      SOURCE(REVIEWS)\n");
}

TEST_F(SemanticMapStatementTest, SemMapOverSubqueryKeepsSubqueryBelowPlaceholder)
{
    const auto plan
        = bindQuery("SELECT * FROM SEM_MAP(sentiment_clf, (SELECT reviewText FROM reviews WHERE reviewId > UINT64(2000000))) INTO result");

    EXPECT_EQ(
        explain(plan, ExplainVerbosity::Short),
        "SINK(RESULT)\n"
        "  PROJECTION(fields: [*])\n"
        "    SEM_MAP_NAME(model: SENTIMENT_CLF)\n"
        "      PROJECTION(fields: [REVIEWTEXT])\n"
        "        SELECTION(REVIEWID > 2000000)\n"
        "          SOURCE(REVIEWS)\n");
}

TEST_F(SemanticMapStatementTest, NestedSemMapStacksPlaceholders)
{
    const auto plan = bindQuery("SELECT * FROM SEM_MAP(summarizer, SEM_MAP(sentiment_clf, reviews)) INTO result");

    const auto placeholders = getOperatorByType<SemanticMapNameLogicalOperator>(plan);
    ASSERT_EQ(placeholders.size(), 2);

    EXPECT_EQ(
        explain(plan, ExplainVerbosity::Short),
        "SINK(RESULT)\n"
        "  PROJECTION(fields: [*])\n"
        "    SEM_MAP_NAME(model: SUMMARIZER)\n"
        "      SEM_MAP_NAME(model: SENTIMENT_CLF)\n"
        "        SOURCE(REVIEWS)\n");
}

TEST_F(SemanticMapStatementTest, SemMapDoesNotCaptureModelNameAsSource)
{
    /// Without the isSemanticMap flag the FROM-clause handling would register the model name as
    /// the query's source. The only source in the plan must be REVIEWS.
    const auto plan = bindQuery("SELECT sentiment FROM SEM_MAP(sentiment_clf, reviews) WHERE sentiment = 'POSITIVE' INTO result");
    const auto rendered = explain(plan, ExplainVerbosity::Short);
    EXPECT_NE(rendered.find("SOURCE(REVIEWS)"), std::string::npos) << rendered;
    EXPECT_EQ(rendered.find("SOURCE(SENTIMENT_CLF)"), std::string::npos) << rendered;
}

}
