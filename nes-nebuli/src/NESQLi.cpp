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

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstring>
#include <expected>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <ostream>
#include <ranges>
#include <sstream>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/Util.hpp>
#include <QueryManager/GRPCClient.hpp>
#include <QueryManager/QueryManager.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/from_current.hpp>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <magic_enum/magic_enum.hpp>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <ErrorHandling.hpp>
#include <JsonOutputFormatter.hpp> /// NOLINT(misc-include-cleaner)
#include <LegacyOptimizer.hpp>
#include <StatementHandler.hpp>
#include <StatementOutputAssembler.hpp>
#include <TextOutputFormatter.hpp> /// NOLINT(misc-include-cleaner)

#ifdef EMBED_ENGINE
    #include <Configurations/Util.hpp>
    #include <QueryManager/EmbeddedQueryManager.hpp>
#endif
namespace
{
enum class ErrorBehaviour : uint8_t
{
    FAIL_FAST,
    RECOVER,
    CONTINUE_AND_FAIL
};
constexpr std::streamsize MAX_STATEMENT_SIZE = static_cast<std::streamsize>(1024) * 100;
}

int main(int argc, char** argv)
{
    CPPTRACE_TRY
    {
        NES::Logger::setupLogging("client.log", NES::LogLevel::LOG_ERROR);
        using argparse::ArgumentParser;
        ArgumentParser program("nesql");
        program.add_argument("-l", "--log-level").default_value("LOG_ERROR").help("Set the log level");
        program.add_argument("-s", "--server").help("grpc uri e.g., 127.0.0.1:8080");
        program.add_argument("-i", "--input")
            .default_value("/dev/stdin")
            .help("From where to read SQL statements. Use - for stdin which is the default");
        program.add_argument("-o", "--output").default_value("-").help("To where to write the output. Use - for stdout");
        program.add_argument("-e", "--error-behaviour")
            .default_value("FAIL_FAST")
            .choices("FAIL_FAST", "RECOVER", "CONTINUE_AND_FAIL")
            .help(
                "Fail and return non-zero exit code on first error, ignore error and continue, or continue and return non-zero exit code");
        program.add_argument("-f").default_value("TEXT").choices("TEXT", "JSON").help("Output format");
        program.add_argument("--", "--worker-config")
            .default_value(std::vector<std::string>{})
            .help(
                "Path to single node node worker configuration passes through followed options to single node worker.\n"
                "Cannot be set together with -s.");
        program.add_argument("-b", "--block-until-finished").flag().default_value(false).help("Block until all queries are finished");

        program.parse_args(argc, argv);
        auto logLevel = magic_enum::enum_cast<NES::LogLevel>(program.get<std::string>("-l"));
        if (not logLevel.has_value())
        {
            NES_ERROR("Invalid log level: {}", program.get<std::string>("-l"));
            return 1;
        }
        NES::Logger::getInstance()->changeLogLevel(logLevel.value());

        if (program.is_used("-s") && program.is_used("--"))
        {
            NES_ERROR("Cannot specify server address and start embedded worker as well");
            return 1;
        }

        const auto defaultOutputFormatOpt = magic_enum::enum_cast<NES::StatementOutputFormat>(program.get<std::string>("-f"));
        if (not defaultOutputFormatOpt.has_value())
        {
            NES_ERROR("Invalid output format: {}", program.get<std::string>("-f"));
            return 1;
        }
        const auto defaultOutputFormat = defaultOutputFormatOpt.value();

        const auto input = program.get("-i");
        const auto errorBehaviourOpt = magic_enum::enum_cast<ErrorBehaviour>(program.get<std::string>("-e"));
        if (not errorBehaviourOpt.has_value())
        {
            NES_FATAL_ERROR(
                "Error behaviour must be set to FAIL_FAST, RECOVER or CONTINUE_AND_FAIL, but was set to {}",
                program.get<std::string>("-e"));
            return 1;
        }
        const auto errorBehaviour = errorBehaviourOpt.value();

        auto sourceCatalog = std::make_shared<NES::SourceCatalog>();
        auto sinkCatalog = std::make_shared<NES::SinkCatalog>();
        auto optimizer = std::make_shared<NES::CLI::LegacyOptimizer>(sourceCatalog, sinkCatalog);
        auto binder = NES::StatementBinder{
            sourceCatalog,
            sinkCatalog,
            [](auto&& pH1) { return NES::AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(pH1)>(pH1)); }};

        std::shared_ptr<NES::QueryManager> queryManager{};
        if (program.is_used("-s"))
        {
            queryManager
                = std::make_shared<NES::GRPCClient>(CreateChannel(program.get<std::string>("-s"), grpc::InsecureChannelCredentials()));
        }
        else
        {
#ifdef EMBED_ENGINE
            auto confVec = program.get<std::vector<std::string>>("--");
            if ((confVec.size() + 1) > INT_MAX)
            {
                NES_FATAL_ERROR("Too many worker configuration options passed through, maximum is {}", INT_MAX);
                return 1;
            }
            const int singleNodeArgC = static_cast<int>(confVec.size() + 1);
            std::vector<const char*> singleNodeArgV;
            singleNodeArgV.reserve(singleNodeArgC + 1);
            singleNodeArgV.push_back("systest"); /// dummy option as arg expects first arg to be the program name
            for (auto& arg : confVec)
            {
                singleNodeArgV.push_back(arg.c_str());
            }
            auto singleNodeWorkerConfig = NES::Configurations::loadConfiguration<NES::Configuration::SingleNodeWorkerConfiguration>(
                                              singleNodeArgC, singleNodeArgV.data())
                                              .value_or(NES::Configuration::SingleNodeWorkerConfiguration{});

            queryManager = std::make_shared<NES::EmbeddedQueryManager>(singleNodeWorkerConfig);
#else
            NES_FATAL_ERROR(
                "No server address given. Please use the -s option to specify the server address or use nebuli-embedded to start a single "
                "node worker.")
            return 1;
#endif
        }

        NES::SourceStatementHandler sourceStatementHandler{sourceCatalog};
        NES::SinkStatementHandler sinkStatementHandler{sinkCatalog};
        NES::QueryStatementHandler queryStatementHandler{queryManager, optimizer};

        bool eofReached = false;
        unsigned int exitCode = 0;
        std::stringstream currentInput{};
        auto handleError = [&exitCode, &errorBehaviour, &currentInput](const auto& error)
        {
            if (errorBehaviour == ErrorBehaviour::FAIL_FAST)
            {
                throw error;
            }
            if (errorBehaviour == ErrorBehaviour::CONTINUE_AND_FAIL)
            {
                exitCode = 1;
            }
            NES_ERROR("Error encountered: {}", error.what());
            currentInput.str(std::string{});
        };
        std::vector<NES::QueryId> startedQueries{};
        while (not eofReached)
        {
            std::istream* istreamPtr = &std::cin;
            std::ifstream fileIstream;
            if (input != "/dev/stdin")
            {
                fileIstream = std::ifstream(input);
                if (not fileIstream)
                {
                    throw NES::QueryDescriptionNotReadable(std::strerror(errno)); /// NOLINT(concurrency-mt-unsafe)
                }
                istreamPtr = &fileIstream;
            }

            std::vector<std::expected<NES::Statement, NES::Exception>> toHandle{};
            std::shared_ptr<NES::AntlrSQLQueryParser::ManagedAntlrParser> managedParser{};
            /// If in daemon mode, read input line by line, and try to parse it after every line read.
            /// Requires user input to not start a new statement in the same line as ending a statement with a semicolon
            std::string currentLine{};
            currentLine.resize(MAX_STATEMENT_SIZE);
            if (not istreamPtr->getline(currentLine.data(), MAX_STATEMENT_SIZE))
            {
                if (istreamPtr->eof())
                {
                    eofReached = true;
                }
                else if (istreamPtr->fail())
                {
                    NES_FATAL_ERROR("Error encountered while reading statement, was the statement too big (>= 102400 characters)?");
                    return 1;
                }
                if (istreamPtr->bad())
                {
                    NES_FATAL_ERROR("I/O error while reading from input");
                    return 1;
                }
            }
            /// trim nulls
            auto endOfLine = std::ranges::find(currentLine, 0);
            currentLine.resize(endOfLine - currentLine.begin());
            currentLine += " "; /// Add space so that you don't have to type it out at the end of a line
            currentInput << currentLine;
            std::string currentInputCopy = currentInput.str();
            /// trim whitespace
            auto firstCharacterIter = std::ranges::find_if(
                std::views::reverse(currentInputCopy), [](const auto character) { return not std::isspace(character); });
            currentInputCopy.resize(firstCharacterIter.base() - currentInputCopy.begin());
            if (currentInputCopy.back() == ';')
            {
                managedParser = NES::AntlrSQLQueryParser::ManagedAntlrParser::create(currentInput.str());
                auto parseResult = managedParser->parseSingle();
                currentInput.str(std::string{});
                if (not parseResult.has_value())
                {
                    handleError(parseResult.error());
                    continue;
                }
                toHandle = std::vector{binder.bind(parseResult.value().get())};
            }

            for (auto& bindingResult : toHandle)
            {
                if (not bindingResult.has_value())
                {
                    handleError(bindingResult.error());
                    continue;
                }
                /// NOLINTNEXTLINE(fuchsia-trailing-return)
                auto visitor = [&](const auto& stmt) -> std::expected<NES::StatementResult, NES::Exception>
                {
                    if constexpr (requires { sourceStatementHandler.apply(stmt); })
                    {
                        return sourceStatementHandler.apply(stmt);
                    }
                    else if constexpr (requires { sinkStatementHandler.apply(stmt); })
                    {
                        return sinkStatementHandler.apply(stmt);
                    }
                    else if constexpr (requires { queryStatementHandler.apply(stmt); })
                    {
                        return queryStatementHandler.apply(stmt);
                    }
                    else
                    {
                        static_assert(false, "All statement types need to have a handler");
                        std::unreachable();
                    }
                };
                auto result = std::visit(visitor, bindingResult.value());
                if (not result.has_value())
                {
                    handleError(result.error());
                    continue;
                }
                if (std::holds_alternative<NES::QueryStatementResult>(result.value()))
                {
                    startedQueries.push_back(std::get<NES::QueryStatementResult>(result.value()).id);
                }
                switch (const auto format = getOutputFormat(bindingResult.value()).value_or(defaultOutputFormat))
                {
                    case NES::StatementOutputFormat::TEXT:
                        std::cout << std::visit(
                            [](const auto& statementResult)
                            {
                                return toText(
                                    NES::StatementOutputAssembler<std::remove_cvref_t<decltype(statementResult)>>{}.convert(
                                        statementResult));
                            },
                            result.value());
                        break;
                    case NES::StatementOutputFormat::JSON:
                        std::cout << std::visit(
                            [](const auto& statementResult)
                            {
                                nlohmann::json output
                                    = NES::StatementOutputAssembler<std::remove_cvref_t<decltype(statementResult)>>{}.convert(
                                        statementResult);
                                return output;
                            },
                            result.value())
                                  << "\n";
                }
                std::flush(std::cout);
            }
        }
        if (program.is_used("--block-until-finished"))
        {
            for (const auto& queryId : startedQueries)
            {
                auto status = queryManager->status(queryId);
                while (status.has_value()
                       && !(status.value().currentStatus == NES::QueryStatus::Stopped
                           || status.value().currentStatus == NES::QueryStatus::Failed))
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                    status = queryManager->status(queryId);
                }
            }
        }
        return exitCode;
    }
    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentErrorCode();
    }
    return 0;
}
