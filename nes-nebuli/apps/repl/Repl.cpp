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

#include <Repl.hpp>

#include <algorithm>
#include <array>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
#include <unistd.h>

#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Statements/JsonOutputFormatter.hpp> /// NOLINT(misc-include-cleaner)
#include <Statements/StatementHandler.hpp>
#include <Statements/StatementOutputAssembler.hpp>
#include <Statements/TextOutputFormatter.hpp> /// NOLINT(misc-include-cleaner)
#include <Util/Logger/Logger.hpp>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <ErrorHandling.hpp>
#include <replxx.hxx>

namespace NES
{

struct Repl::Impl
{
    SourceStatementHandler sourceStatementHandler;
    SinkStatementHandler sinkStatementHandler;
    TopologyStatementHandler topologyStatementHandler;
    std::shared_ptr<QueryStatementHandler> queryStatementHandler;
    StatementBinder binder;
    std::stop_token stopToken;

    std::unique_ptr<replxx::Replxx> rx;
    std::vector<std::string> history;
    bool interactiveMode = true;
    ErrorBehaviour errorBehaviour;
    StatementOutputFormat defaultOutputFormat;
    unsigned int exitCode = 0;

    /// Commands
    static constexpr const char* HELP_CMD = "help";
    static constexpr const char* QUIT_CMD = "quit";
    static constexpr const char* EXIT_CMD = "exit";
    static constexpr const char* CLEAR_CMD = "clear";

    /// NOLINTBEGIN(readability-convert-member-functions-to-static)

    Impl(
        SourceStatementHandler sourceStatementHandler,
        SinkStatementHandler sinkStatementHandler,
        TopologyStatementHandler topologyStatementHandler,
        std::shared_ptr<QueryStatementHandler> queryStatementHandler,
        StatementBinder binder,
        const ErrorBehaviour errorBehaviour,
        const StatementOutputFormat defaultOutputFormat,
        const bool interactiveMode,
        std::stop_token stopToken)
        : sourceStatementHandler(std::move(sourceStatementHandler))
        , sinkStatementHandler(std::move(sinkStatementHandler))
        , topologyStatementHandler(std::move(topologyStatementHandler))
        , queryStatementHandler(std::move(queryStatementHandler))
        , binder(std::move(binder))
        , stopToken(std::move(stopToken))
        , interactiveMode(interactiveMode)
        , errorBehaviour(errorBehaviour)
        , defaultOutputFormat(defaultOutputFormat)
    {
        if (interactiveMode)
        {
            setupReplxx();
        }
        else
        {
            NES_INFO("Non-interactive mode detected (not a TTY). Using basic input mode.\n");
        }
    }

    void setupReplxx()
    {
        rx = std::make_unique<replxx::Replxx>();

        rx->set_word_break_characters(" \t\n\r");

        /// Set up hints
        rx->set_hint_callback(
            [](const std::string& input, int&, replxx::Replxx::Color& color) -> std::vector<std::string>
            {
                if (input.empty())
                {
                    return {};
                }

                const std::vector<std::string> commands = {"help", "quit", "exit", "clear"};
                for (const auto& cmd : commands)
                {
                    if (input.starts_with(cmd))
                    {
                        color = replxx::Replxx::Color::BLUE;
                        return {" (command)"};
                    }
                }

                return {};
            });

        rx->history_load(".nebuli_history");
    }

    void printWelcome()
    {
        const bool useColour = isatty(STDOUT_FILENO) != 0;
        auto color = [&](const char* esc) { return useColour ? esc : ""; };
        const char* bold = color("\033[1m");
        const char* accent = color("\033[34m");
        const char* reset = color("\033[0m");

        constexpr std::string_view title = "NebulaStream Interactive Query Shell";
        constexpr std::size_t width = 60;
        const std::size_t pad = (width - title.size()) / 2;

        std::cout << '\n' << accent << std::string(width, '=') << '\n';
        std::cout << std::string(pad, ' ') << bold << title << reset << '\n';
        std::cout << accent << std::string(width, '=') << reset << '\n';

        struct Cmd
        {
            const char* name;
            const char* desc;
        };

        constexpr std::array<Cmd, 4> cmds{
            {{.name = "help", .desc = "Show this help message"},
             {.name = "clear", .desc = "Clear the screen"},
             {.name = "quit", .desc = "Exit the shell"},
             {.name = "exit", .desc = "Alias for quit"}}};

        std::cout << bold << "Commands" << reset << ":\n";
        for (auto [name, desc] : cmds)
        {
            std::cout << "  • " << bold << name << reset << std::string(8 - std::strlen(name), ' ') << "─ " << desc << '\n';
        }
        std::cout << '\n'
                  << "Enter SQL to execute it; multi‑line statements are supported and\n"
                  << "run automatically once the final line ends with a semicolon.\n\n";
    }

    void printHelp()
    {
        const bool useColour = isatty(STDOUT_FILENO) != 0;
        auto color = [&](const char* esc) { return useColour ? esc : ""; };

        const char* bold = color("\033[1m");
        const char* reset = color("\033[0m");
        const char* accent = color("\033[34m");

        struct Cmd
        {
            const char* name;
            const char* desc;
        };

        constexpr std::array<Cmd, 4> cmds{
            {{.name = "help", .desc = "Show this help message"},
             {.name = "clear", .desc = "Clear the screen"},
             {.name = "quit", .desc = "Exit the shell"},
             {.name = "exit", .desc = "Alias for quit"}}};

        std::size_t padWidth = 0;
        for (const auto& cmd : cmds)
        {
            padWidth = std::max(padWidth, std::strlen(cmd.name));
        }
        padWidth += 2;

        std::cout << '\n' << bold << "Commands" << reset << ":\n";
        for (const auto& cmd : cmds)
        {
            std::cout << "  " << bold << cmd.name << reset << std::string(padWidth - std::strlen(cmd.name), ' ') << "─ " << cmd.desc
                      << '\n';
        }

        std::cout << '\n'
                  << "Enter SQL to execute it; multi‑line statements are supported and\n"
                  << "run automatically once the final line ends with a semicolon.\n\n"
                  << "Docs: " << accent << "https://docs.nebula.stream/" << reset << "\n\n";
    }

    /// This method should handle "regular" errors, such as from parsing user input or being unable to execute statements.
    /// The try-catch in the main-loop should only catch unexpected errors.
    void handleError(auto error)
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
        std::cout << fmt::format("Error encountered: {}", error.what());
    }

    void clearScreen() const
    {
        constexpr const char* ansiClear = "\033[2J\033[H";
        if (interactiveMode)
        {
            rx->clear_screen();
        }
        else
        {
            std::cout << ansiClear << std::flush;
        }
    }

    [[nodiscard]] std::string getPrompt() const { return "NES 🌌 > "; }

    [[nodiscard]] bool isCommand(const std::string& input)
    {
        std::istringstream iss(input);
        std::string cmd;
        iss >> cmd;

        return cmd == HELP_CMD || cmd == QUIT_CMD || cmd == EXIT_CMD || cmd == CLEAR_CMD;
    }

    bool handleCommand(const std::string& input)
    {
        std::istringstream iss(input);
        std::string cmd;
        iss >> cmd;

        if (cmd == HELP_CMD)
        {
            printHelp();
            return false;
        }

        if (cmd == QUIT_CMD || cmd == EXIT_CMD)
        {
            if (interactiveMode)
            {
                std::cout << "Goodbye!\n";
            }
            return true;
        }

        if (cmd == CLEAR_CMD)
        {
            clearScreen();
            return false;
        }
        return false;
    }

    [[nodiscard]] std::string readMultiLineQuery(const std::string& firstLine) const
    {
        PRECONDITION(!firstLine.empty(), "first line may not be empty.");

        std::string query;
        std::string line;
        ssize_t parenCount = 0;
        bool inString = false;
        char stringChar = 0;

        while (true)
        {
            if (query.empty())
            {
                line = firstLine;
            }
            else if (!interactiveMode)
            {
                /// Use std::getline for non-interactive mode
                std::getline(std::cin, line);
                if (std::cin.eof())
                {
                    break;
                }
            }
            else
            {
                /// Use Replxx for interactive mode
                line = rx->input(getPrompt());
            }

            if (line.empty())
            {
                continue;
            }

            if (interactiveMode && !query.empty())
            {
                rx->history_add(line);
            }

            for (const char charInLine : line)
            {
                if (inString)
                {
                    if (charInLine == stringChar)
                    {
                        inString = false;
                        stringChar = 0;
                    }
                }
                else
                {
                    if (charInLine == '\'' || charInLine == '"')
                    {
                        inString = true;
                        stringChar = charInLine;
                    }
                    else if (charInLine == '(')
                    {
                        parenCount++;
                    }
                    else if (charInLine == ')')
                    {
                        parenCount--;
                    }
                }
            }

            query += line + "\n";

            if (parenCount > 0 || inString)
            {
                continue;
            }

            if (parenCount < 0)
            {
                throw QueryInvalid("too many closing parantesis");
            }

            /// Check if the line ends with a semicolon
            if (!line.empty() && line.back() == ';')
            {
                break;
            }
        }
        return query;
    }

    bool executeQuery(const std::string& query)
    {
        auto managedParser = NES::AntlrSQLQueryParser::ManagedAntlrParser::create(query);
        auto parseResult = managedParser->parseMultiple();
        if (not parseResult.has_value())
        {
            handleError(std::move(parseResult.error()));
            return false;
        }
        auto toHandle = parseResult.value() | std::views::transform([this](const auto& stmt) { return binder.bind(stmt.get()); })
            | std::ranges::to<std::vector>();

        for (auto& bindingResult : toHandle)
        {
            if (not bindingResult.has_value())
            {
                handleError(std::move(bindingResult.error()));
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
                else if constexpr (requires { topologyStatementHandler.apply(stmt); })
                {
                    return topologyStatementHandler.apply(stmt);
                }
                else if constexpr (requires { queryStatementHandler->apply(stmt); })
                {
                    return queryStatementHandler->apply(stmt);
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
                handleError(std::move(result.error()));
                continue;
            }
            switch (getOutputFormat(bindingResult.value()).value_or(defaultOutputFormat))
            {
                case NES::StatementOutputFormat::TEXT:
                    std::cout << std::visit(
                        [](const auto& statementResult)
                        {
                            return toText(
                                StatementOutputAssembler<std::remove_cvref_t<decltype(statementResult)>>{}.convert(statementResult));
                        },
                        result.value());
                    break;
                case NES::StatementOutputFormat::JSON:
                    std::cout << std::visit(
                        [](const auto& statementResult)
                        {
                            nlohmann::json output
                                = NES::StatementOutputAssembler<std::remove_cvref_t<decltype(statementResult)>>{}.convert(statementResult);
                            return output;
                        },
                        result.value())
                              << "\n";
            }
            std::flush(std::cout);
        }
        return true;
    }

    void run()
    {
        if (interactiveMode)
        {
            printWelcome();
        }

        while (!stopToken.stop_requested())
        {
            try
            {
                std::string input;

                if (!interactiveMode)
                {
                    /// Use std::getline for non-interactive mode to avoid terminal issues
                    if (!std::getline(std::cin, input))
                    {
                        if (std::cin.eof())
                        {
                            break;
                        }

                        continue;
                    }
                }
                else
                {
                    /// Use Replxx for interactive mode
                    const auto result = rx->input(getPrompt());
                    if (!result)
                    {
                        /// EoF reached
                        return;
                    }

                    input = result;
                }

                if (input.empty())
                {
                    continue;
                }

                /// Add to history (only in interactive mode)
                if (interactiveMode)
                {
                    rx->history_add(input);
                }

                /// Check if it's a command
                if (isCommand(input))
                {
                    if (handleCommand(input))
                    {
                        break;
                    }
                    continue;
                }

                /// Check if it's a single-line SQL query
                auto trim = [](const std::string& str) -> std::string
                {
                    const size_t start = str.find_first_not_of(" \t\n\r");
                    if (start == std::string::npos)
                    {
                        return "";
                    }
                    const size_t end = str.find_last_not_of(" \t\n\r");
                    return str.substr(start, end - start + 1);
                };
                auto isCompleteStatement = [&](const std::string& stmt) -> bool
                {
                    std::string trimmed = trim(stmt);
                    return !trimmed.empty() && trimmed.back() == ';';
                };
                if (isCompleteStatement(input))
                {
                    executeQuery(input);
                }
                else
                {
                    const std::string fullQuery = readMultiLineQuery(input);
                    executeQuery(fullQuery);
                }
            }
            catch (const Exception& e)
            {
                if (errorBehaviour == ErrorBehaviour::FAIL_FAST)
                {
                    throw;
                }
                std::cout << "Error: " << e.what() << "\n";
            }
        }

        if (interactiveMode)
        {
            rx->history_save(".nebuli_history");
        }
    }
};

Repl::Repl(
    SourceStatementHandler sourceStatementHandler,
    SinkStatementHandler sinkStatementHandler,
    TopologyStatementHandler topologyStatementHandler,
    std::shared_ptr<QueryStatementHandler> queryStatementHandler,
    StatementBinder binder,
    ErrorBehaviour errorBehaviour,
    StatementOutputFormat defaultOutputFormat,
    bool interactiveMode,
    std::stop_token stopToken)
    : impl(std::make_unique<Impl>(
          std::move(sourceStatementHandler),
          std::move(sinkStatementHandler),
          std::move(topologyStatementHandler),
          std::move(queryStatementHandler),
          std::move(binder),
          errorBehaviour,
          defaultOutputFormat,
          interactiveMode,
          std::move(stopToken)))
{
}

void Repl::run()
{
    impl->run();
}

Repl::~Repl() = default;

/// NOLINTEND(readability-convert-member-functions-to-static)

}
