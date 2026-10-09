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

#include <cstdint>
#include <cstdlib>
#include <string>
#include <utility>
#include <vector>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <LookbackConnections.hpp>
#include <LookbackContext.hpp>
#include <LookbackProvider.hpp>
#include <LookbackTemplate.hpp>

namespace NES
{
/// The fixture connections deliberately hand back optionals the tests then dereference after an
/// ASSERT_TRUE; the checker cannot see the assertion as a guard.
/// NOLINTBEGIN(bugprone-unchecked-optional-access)

class LookbackFunctionTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("LookbackFunctionTest.log", LogLevel::LOG_DEBUG); }
};

namespace
{
/// Reads the whole rendering back out of a context, mimicking what the traced code does: run()
/// reports the length, then copyOut() fills a buffer of exactly that size.
std::string runAndRender(Lookback::LookbackContext& context, const std::vector<std::string>& parameters)
{
    context.begin();
    for (const auto& parameter : parameters)
    {
        context.bind(
            reinterpret_cast<const int8_t*>(parameter.data()), parameter.size()); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    }
    const auto size = context.run();
    std::string rendered(size, '\0');
    context.copyOut(reinterpret_cast<int8_t*>(rendered.data()), size); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    return rendered;
}

Lookback::LookbackConnection fixtureConnection(Lookback::ResultRows rows)
{
    Lookback::LookbackConnection connection;
    connection.name = "fixture";
    connection.staticRows = std::move(rows);
    return connection;
}
}

/// --- Row template ------------------------------------------------------------------------

TEST_F(LookbackFunctionTest, templateSplitsLiteralsAroundPlaceholders)
{
    const auto rowTemplate = Lookback::RowTemplate::parse("{}={} @{}; ");
    EXPECT_EQ(rowTemplate.placeholderCount(), 3U);

    std::string rendered;
    rowTemplate.render({"HB", "6.2", "10:15"}, rendered);
    EXPECT_EQ(rendered, "HB=6.2 @10:15; ");
}

TEST_F(LookbackFunctionTest, templateWithoutPlaceholdersIsAConstantString)
{
    const auto rowTemplate = Lookback::RowTemplate::parse("a prior value exists");
    EXPECT_EQ(rowTemplate.placeholderCount(), 0U);

    std::string rendered;
    rowTemplate.render({}, rendered);
    EXPECT_EQ(rendered, "a prior value exists");
}

TEST_F(LookbackFunctionTest, templateRendersRowsByAppending)
{
    const auto rowTemplate = Lookback::RowTemplate::parse("[{}]");
    std::string rendered;
    rowTemplate.render({"a"}, rendered);
    rowTemplate.render({"b"}, rendered);
    EXPECT_EQ(rendered, "[a][b]");
}

TEST_F(LookbackFunctionTest, templateRejectsUnmatchedBrace)
{
    ASSERT_EXCEPTION_ERRORCODE(Lookback::RowTemplate::parse("value {"), ErrorCode::InvalidLiteral);
    ASSERT_EXCEPTION_ERRORCODE(Lookback::RowTemplate::parse("value }"), ErrorCode::InvalidLiteral);
}

/// A named placeholder is the trap worth failing loudly on: substitution is positional, so
/// silently ignoring the name would put the wrong column into a clinical alert.
TEST_F(LookbackFunctionTest, templateRejectsNamedPlaceholder)
{
    ASSERT_EXCEPTION_ERRORCODE(Lookback::RowTemplate::parse("{Wert}"), ErrorCode::InvalidLiteral);
    ASSERT_EXCEPTION_ERRORCODE(Lookback::RowTemplate::parse("{0}"), ErrorCode::InvalidLiteral);
}

TEST_F(LookbackFunctionTest, templateRejectsColumnCountMismatch)
{
    const auto rowTemplate = Lookback::RowTemplate::parse("{}-{}");
    std::string rendered;
    ASSERT_EXCEPTION_ERRORCODE(rowTemplate.render({"only-one"}, rendered), ErrorCode::LookbackFailure);
    ASSERT_EXCEPTION_ERRORCODE(rowTemplate.render({"a", "b", "c"}, rendered), ErrorCode::LookbackFailure);
}

/// --- Parameter markers -------------------------------------------------------------------

TEST_F(LookbackFunctionTest, countsParameterMarkers)
{
    EXPECT_EQ(Lookback::countParameterMarkers("SELECT 1"), 0U);
    EXPECT_EQ(Lookback::countParameterMarkers("SELECT a FROM t WHERE id = ? AND ts > ?"), 2U);
}

TEST_F(LookbackFunctionTest, ignoresParameterMarkersInsideStringLiterals)
{
    EXPECT_EQ(Lookback::countParameterMarkers("SELECT 'what?' FROM t WHERE id = ?"), 1U);
    /// '' is the SQL escape for an embedded quote, so the literal stays open across it.
    EXPECT_EQ(Lookback::countParameterMarkers("SELECT 'it''s ? here' FROM t"), 0U);
}

/// --- Connection configuration ------------------------------------------------------------

TEST_F(LookbackFunctionTest, parsesFixtureConnection)
{
    const auto parsed = Lookback::LookbackConnectionRegistry::parse(R"(
connections:
  fixture:
    max_rows: 2
    rows:
      - ["HB", "6.2"]
      - ["HB", "6.8"]
)");
    ASSERT_TRUE(parsed.contains("fixture"));
    const auto& connection = parsed.at("fixture");
    EXPECT_FALSE(connection.odbc.has_value());
    ASSERT_TRUE(connection.staticRows.has_value());
    EXPECT_EQ(connection.staticRows->size(), 2U);
    EXPECT_EQ(connection.limits.maxRows, 2U);
}

TEST_F(LookbackFunctionTest, parsesDatabaseConnection)
{
    const auto parsed = Lookback::LookbackConnectionRegistry::parse(R"(
connections:
  mlife:
    db_host: 10.0.0.5
    db_port: "1433"
    database: mlife
    username: nes_reader
    password: secret
    driver: ODBC Driver 18 for SQL Server
    trust_server_certificate: true
    strict: true
)");
    const auto& connection = parsed.at("mlife");
    ASSERT_TRUE(connection.odbc.has_value());
    EXPECT_EQ(connection.odbc->server, "10.0.0.5");
    EXPECT_EQ(connection.odbc->port, "1433");
    EXPECT_TRUE(connection.odbc->trustServerCertificate);
    EXPECT_TRUE(connection.limits.strict);
    /// Defaults survive when the key is absent.
    EXPECT_EQ(connection.limits.queryTimeoutMs, 2000U);
}

TEST_F(LookbackFunctionTest, rejectsUnknownKeyAndMissingRequiredKey)
{
    ASSERT_EXCEPTION_ERRORCODE(
        Lookback::LookbackConnectionRegistry::parse("connections:\n  a:\n    db_hostt: x\n"), ErrorCode::CannotLoadConfig);
    ASSERT_EXCEPTION_ERRORCODE(
        Lookback::LookbackConnectionRegistry::parse("connections:\n  a:\n    db_host: x\n"), ErrorCode::CannotLoadConfig);
    ASSERT_EXCEPTION_ERRORCODE(Lookback::LookbackConnectionRegistry::parse("no_connections_key: 1\n"), ErrorCode::CannotLoadConfig);
}

TEST_F(LookbackFunctionTest, rejectsConnectionThatIsBothFixtureAndDatabase)
{
    ASSERT_EXCEPTION_ERRORCODE(
        Lookback::LookbackConnectionRegistry::parse("connections:\n  a:\n    db_host: x\n    rows: [[\"1\"]]\n"),
        ErrorCode::CannotLoadConfig);
}

TEST_F(LookbackFunctionTest, expandsEnvironmentReferences)
{
    /// POSIX-only, so no std:: header declares them; the mt-unsafe warning is about a concurrent
    /// getenv, and gtest runs a test body on one thread.
    /// NOLINTNEXTLINE(concurrency-mt-unsafe,misc-include-cleaner)
    setenv("NES_LOOKBACK_TEST_SECRET", "hunter2", 1);
    EXPECT_EQ(Lookback::LookbackConnectionRegistry::expandEnvironment("pre-${NES_LOOKBACK_TEST_SECRET}-post"), "pre-hunter2-post");
    EXPECT_EQ(Lookback::LookbackConnectionRegistry::expandEnvironment("plain"), "plain");
    /// NOLINTNEXTLINE(concurrency-mt-unsafe,misc-include-cleaner)
    unsetenv("NES_LOOKBACK_TEST_SECRET");

    /// An unset variable is an error rather than an empty expansion, so a missing password fails
    /// loudly instead of attempting an anonymous connect.
    ASSERT_EXCEPTION_ERRORCODE(
        Lookback::LookbackConnectionRegistry::expandEnvironment("${NES_LOOKBACK_TEST_SECRET}"), ErrorCode::CannotLoadConfig);
    ASSERT_EXCEPTION_ERRORCODE(Lookback::LookbackConnectionRegistry::expandEnvironment("${unterminated"), ErrorCode::CannotLoadConfig);
}

TEST_F(LookbackFunctionTest, unknownConnectionNameIsReported)
{
    auto& registry = Lookback::LookbackConnectionRegistry::instance();
    registry.loadFromYaml("connections:\n  known:\n    rows: [[\"1\"]]\n");
    EXPECT_TRUE(registry.contains("known"));
    EXPECT_FALSE(registry.contains("absent"));
    ASSERT_EXCEPTION_ERRORCODE((void)registry.get("absent"), ErrorCode::UnknownLookbackConnection);
}

/// --- End to end through the context ------------------------------------------------------

TEST_F(LookbackFunctionTest, rendersEveryRowOfTheResult)
{
    Lookback::LookbackContext context{
        fixtureConnection({{"HB", "6.2"}, {"HB", "6.8"}}), "SELECT bez, wert FROM v WHERE id = ?", Lookback::RowTemplate::parse("{}={}; ")};

    EXPECT_EQ(runAndRender(context, {"1001"}), "HB=6.2; HB=6.8; ");
}

/// The requirement that motivates the whole function: a lookback that finds nothing must produce
/// the empty string, not a NULL and not a placeholder.
TEST_F(LookbackFunctionTest, emptyResultRendersAsEmptyString)
{
    Lookback::LookbackContext context{fixtureConnection({}), "SELECT 1 WHERE 1 = ?", Lookback::RowTemplate::parse("{}")};
    EXPECT_EQ(runAndRender(context, {"1001"}), "");
}

TEST_F(LookbackFunctionTest, stopsAtMaxRows)
{
    auto connection = fixtureConnection({{"a"}, {"b"}, {"c"}});
    connection.limits.maxRows = 2;
    Lookback::LookbackContext context{std::move(connection), "SELECT x FROM v", Lookback::RowTemplate::parse("{}")};

    EXPECT_EQ(runAndRender(context, {}), "ab");
}

TEST_F(LookbackFunctionTest, truncatesAtMaxResultBytes)
{
    constexpr uint32_t resultCap = 6;
    auto connection = fixtureConnection({{"aaaa"}, {"bbbb"}, {"cccc"}});
    connection.limits.maxResultBytes = resultCap;
    Lookback::LookbackContext context{std::move(connection), "SELECT x FROM v", Lookback::RowTemplate::parse("{}")};

    const auto rendered = runAndRender(context, {});
    EXPECT_EQ(rendered.size(), resultCap);
    EXPECT_EQ(rendered, "aaaabb");
}

/// German alert prose is the norm here, so a truncation must not sever a multi-byte codepoint.
TEST_F(LookbackFunctionTest, truncationRespectsUtf8Boundaries)
{
    auto connection = fixtureConnection({{"Hämoglobin"}});
    /// "Hä" is 3 bytes; a cut at 2 would land inside the 'ä'.
    connection.limits.maxResultBytes = 2;
    Lookback::LookbackContext context{std::move(connection), "SELECT x FROM v", Lookback::RowTemplate::parse("{}")};

    EXPECT_EQ(runAndRender(context, {}), "H");
}

/// A lookback failure must not lose the alert: the rendering comes back empty and the pipeline
/// keeps going.
TEST_F(LookbackFunctionTest, templateMismatchDoesNotLoseTheAlert)
{
    Lookback::LookbackContext context{fixtureConnection({{"one", "two"}}), "SELECT a, b FROM v", Lookback::RowTemplate::parse("{}")};
    EXPECT_EQ(runAndRender(context, {}), "");
}

TEST_F(LookbackFunctionTest, strictConnectionPropagatesTheFailure)
{
    auto connection = fixtureConnection({{"one", "two"}});
    connection.limits.strict = true;
    Lookback::LookbackContext context{std::move(connection), "SELECT a, b FROM v", Lookback::RowTemplate::parse("{}")};

    context.begin();
    ASSERT_EXCEPTION_ERRORCODE((void)context.run(), ErrorCode::LookbackFailure);
}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
