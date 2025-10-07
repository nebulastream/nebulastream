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

#pragma once

#include <cstdint>
#include <memory>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Statements/StatementHandler.hpp>

namespace NES
{

enum class ErrorBehaviour : uint8_t
{
    FAIL_FAST,
    RECOVER,
    CONTINUE_AND_FAIL
};

class Repl
{
    struct Impl;
    std::unique_ptr<Impl> impl;

public:
    explicit Repl(
        SourceStatementHandler sourceStatementHandler,
        SinkStatementHandler sinkStatementHandler,
        std::shared_ptr<QueryStatementHandler> queryStatementHandler,
        StatementBinder binder,
        ErrorBehaviour errorBehaviour,
        StatementOutputFormat defaultOutputFormat,
        bool interactiveMode);
    void run();
    ~Repl();
};

}
