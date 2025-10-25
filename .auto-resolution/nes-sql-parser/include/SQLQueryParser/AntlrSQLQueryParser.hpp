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

#include <ErrorHandling.hpp>


#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <ANTLRInputStream.h>
#include <AntlrSQLLexer.h>
#include <AntlrSQLParser.h>
#include <CommonTokenStream.h>
#include <ParserRuleContext.h>
#include <Plans/LogicalPlan.hpp>

namespace NES::AntlrSQLQueryParser
{

LogicalPlan bindLogicalQueryPlan(AntlrSQLParser::QueryContext* queryAst);
LogicalPlan createLogicalQueryPlanFromSQLString(std::string_view queryString);

/// @brief Safe, heap allocated wrapper around an ANTLR chain instance. ASTs lifetime is owned by the chain that created them.
class ManagedAntlrParser : public std::enable_shared_from_this<ManagedAntlrParser>
{
public:
    template <typename T>
    requires std::is_base_of_v<antlr4::ParserRuleContext, T>
    class ManagedContext
    {
        friend ManagedAntlrParser;
        T* pointer;
        std::shared_ptr<ManagedAntlrParser> owningParser;

        ManagedContext(T* pointer, const std::shared_ptr<ManagedAntlrParser>& owningParser) : pointer(pointer), owningParser(owningParser)
        {
        }

    public:
        T* get() const noexcept { return pointer; }
    };

    struct Private
    {
        explicit Private() = default;
    };

    explicit ManagedAntlrParser(Private, std::string_view input);
    static std::shared_ptr<ManagedAntlrParser> create(std::string_view input);
    std::expected<ManagedContext<AntlrSQLParser::StatementContext>, Exception> parseSingle();
    std::expected<std::vector<ManagedContext<AntlrSQLParser::StatementContext>>, Exception> parseMultiple();

private:
    antlr4::ANTLRInputStream inputStream;
    AntlrSQLLexer lexer;
    antlr4::CommonTokenStream tokens;
    AntlrSQLParser parser;
    std::string originalQuery;
    std::shared_ptr<antlr4::ANTLRErrorListener> errorListener;
};

}
