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
#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <unistd.h>

#include <Util/Logger/Logger.hpp>
#include <coordinator/lib.h>
#include <fmt/format.h>
#include <rust/cxx.h>
#include <ErrorHandling.hpp>
#include <replxx.hxx>

namespace NES
{

struct Repl::Impl
{
    EmbeddedCoordinator& coordinator;
    std::stop_token stopToken;

    std::unique_ptr<replxx::Replxx> rx;
    bool interactiveMode = true;
    ErrorBehaviour errorBehaviour;
    bool jsonOutput;
    unsigned int exitCode = 0;

    /// Commands
    static constexpr const char* HELP_CMD = "help";
    static constexpr const char* QUIT_CMD = "quit";
    static constexpr const char* EXIT_CMD = "exit";
    static constexpr const char* CLEAR_CMD = "clear";

    /// NOLINTBEGIN(readability-convert-member-functions-to-static)

    Impl(
        EmbeddedCoordinator& coordinator,
        const ErrorBehaviour errorBehaviour,
        const bool jsonOutput,
        const bool interactiveMode,
        std::stop_token stopToken)
        : coordinator(coordinator)
        , stopToken(std::move(stopToken))
        , interactiveMode(interactiveMode)
        , errorBehaviour(errorBehaviour)
        , jsonOutput(jsonOutput)
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
        constexpr std::size_t pad = (width - title.size()) / 2;

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
        for (const auto& [name, desc] : cmds)
        {
            padWidth = std::max(padWidth, std::strlen(name));
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
    void handleError(const auto& error)
    {
        NES_ERROR("Error encountered: {}", error.what());
        std::cout << fmt::format("Error encountered: {}\n", error.what());
        if (errorBehaviour == ErrorBehaviour::CONTINUE_AND_FAIL)
        {
            exitCode = 1;
        }
        if (errorBehaviour == ErrorBehaviour::FAIL_FAST)
        {
            throw error;
        }
    }

    void clearScreen() const
    {
        if (interactiveMode)
        {
            rx->clear_screen();
        }
        else
        {
            constexpr auto ansiClear = "\033[2J\033[H";
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
                throw QueryInvalid("too many closing parenthesis");
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
        try
        {
            const auto rendered = coordinator.submit_sql(rust::Str{query.data(), query.size()}, jsonOutput);
            std::cout << std::string_view{rendered.data(), rendered.size()} << '\n';
            std::flush(std::cout);
        }
        catch (const rust::Error& error)
        {
            handleError(error);
            return false;
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
                    const auto* const result = rx->input(getPrompt());
                    if (result == nullptr)
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
                    const std::string trimmed = trim(stmt);
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
    EmbeddedCoordinator& coordinator, ErrorBehaviour errorBehaviour, bool jsonOutput, bool interactiveMode, std::stop_token stopToken)
    : impl(std::make_unique<Impl>(coordinator, errorBehaviour, jsonOutput, interactiveMode, std::move(stopToken)))
{
}

void Repl::run() const
{
    impl->run();
}

Repl::~Repl() = default;

/// NOLINTEND(readability-convert-member-functions-to-static)

}
