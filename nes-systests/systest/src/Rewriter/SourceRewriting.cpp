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

#include <Rewriter/SourceRewriting.hpp>

#include <algorithm>
#include <filesystem>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <AntlrSQLParser.h>
#include <TokenStreamRewriter.h>
#include <fmt/format.h>

#include <Model/ParsedTestFile.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Rewriter/ClassifiedStatement.hpp>
#include <Rewriter/Constants.hpp>
#include <Rewriter/NamePrefixer.hpp>
#include <Rewriter/RewriteContext.hpp>
#include <Rewriter/SqlParse.hpp>
#include <Util/Overloaded.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

SourceRewriter::SourceRewriter(const RewriteContext& context, const PrefixedNames& names) : context{context}, names{names}
{
}

RewrittenSource SourceRewriter::rewrite(SqlParse& parse, PhysicalSourceDeclaration declaration)
{
    /// The rewriter makes the ATTACH path absolute, under the test data directory.
    /// A path the test wrote would stay relative, or set the option twice next to an ATTACH.
    auto* declared = declaredOptions(declaration.definition);
    if (declaresOption(declared, Sql::Source, Sql::FilePath))
    {
        throw TestException(
            "A physical source must not choose its data file, because a source reads the data that its ATTACH names: {}",
            declaration.definition->getText());
    }

    const auto sourceNumber = ordinal++;

    /// The file that the source opens: the attached file, or the CSV that the rewriter plans for the inline rows.
    std::optional<std::filesystem::path> dataFile;
    std::optional<InlineData> inlineData;
    /// A source that produces its own data (e.g., Generator) takes neither.
    if (declaration.attached.has_value())
    {
        std::visit(
            Overloaded{
                [&](InlineRows&& inlined)
                {
                    auto file = context.sourceDataFile(sourceNumber);
                    inlineData = InlineData{.path = file, .rows = std::move(inlined).rows};
                    dataFile = std::move(file);
                },
                [&](const AttachedFile& attachedFile) { dataFile = context.testDataDir / attachedFile.path; }},
            std::move(*declaration.attached));
    }

    const auto setClause = renderSetClauseFor(parse, declared, dataFile);
    antlr4::TokenStreamRewriter rewriter{&parse.tokenStream()};
    /// The rewriter prefixes first, because the SET clause replaces what the test declared, and a later edit wins over an earlier one.
    prefixNames(parse, rewriter, names);
    insertSetClause(rewriter, declaration.definition, setClause);
    return RewrittenSource{
        .statement = SetupStatement{.sql = rewriter.getText(), .inlineData = std::move(inlineData)}, .inputFile = std::move(dataFile)};
}

/// The host pins the source to one worker, because an omitted host resolves to the worker that responds,
/// and on a topology no worker responds.
/// The CSV default exists because creating the source's descriptor rejects a source without an input format.
/// The options that the test wrote come last, unchanged.
std::string SourceRewriter::renderSetClauseFor(
    SqlParse& parse, AntlrSQLParser::NamedConfigExpressionSeqContext* declared, const std::optional<std::filesystem::path>& dataFile) const
{
    std::vector<std::string> options;
    if (dataFile.has_value())
    {
        options.push_back(Sql::option(Sql::Source, Sql::FilePath, dataFile->string()));
    }
    if (not declaresOption(declared, Sql::Source, Sql::Host))
    {
        options.push_back(Sql::option(Sql::Source, Sql::Host, context.sourceHost.view()));
    }
    if (not declaresOption(declared, Sql::InputFormatter, Sql::Type))
    {
        options.push_back(Sql::option(Sql::InputFormatter, Sql::Type, Sql::Csv));
    }
    if (const auto declaredText = parse.getTextOf(declared); not declaredText.empty())
    {
        options.push_back(declaredText);
    }
    return Sql::setClause(Sql::optionList(options));
}

void makeAnonymousSourcePathsAbsolute(
    const SqlParse& parse, antlr4::TokenStreamRewriter& rewriter, const std::filesystem::path& testDataDir)
{
    for (const auto* source : findAll<AntlrSQLParser::AnonymousSourceContext>(parse.tree()))
    {
        for (auto* option : source->parameters->namedConfigExpression())
        {
            if (auto* value = getStringValueOf(option); value != nullptr and isOption(option, Sql::Source, Sql::FilePath))
            {
                if (const std::filesystem::path declared{unquote(value->getText())}; declared.is_relative())
                {
                    rewriter.replace(value->getStart(), value->getStop(), Sql::stringLiteral((testDataDir / declared).string()));
                }
            }
        }
    }
}

void completeAnonymousSources(const SqlParse& parse, antlr4::TokenStreamRewriter& rewriter, const Host& host)
{
    for (const auto* source : findAll<AntlrSQLParser::AnonymousSourceContext>(parse.tree()))
    {
        const auto declared = source->parameters->namedConfigExpression();
        std::vector<std::string> missing;
        if (not std::ranges::any_of(declared, [](auto* option) { return isOption(option, Sql::Source, Sql::Host); }))
        {
            missing.push_back(Sql::option(Sql::Source, Sql::Host, host.view()));
        }
        if (not std::ranges::any_of(declared, [](auto* option) { return isOption(option, Sql::InputFormatter, Sql::Type); }))
        {
            missing.push_back(Sql::option(Sql::InputFormatter, Sql::Type, Sql::Csv));
        }
        if (not missing.empty())
        {
            rewriter.insertAfter(source->parameters->getStop(), fmt::format(", {}", Sql::optionList(missing)));
        }
    }
}

}
