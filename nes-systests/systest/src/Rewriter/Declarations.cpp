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

#include <Rewriter/Declarations.hpp>

#include <string>
#include <utility>
#include <variant>

#include <Identifiers/Identifier.hpp>
#include <Rewriter/ClassifiedStatement.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Rewriter/SinkRewriting.hpp>
#include <Util/Overloaded.hpp>

namespace NES
{
namespace
{

void declareNames(ClassifiedCreate& create, NameRegistry& registry, SinkByName& sinkByName, const bool submitsDeclaredSinks)
{
    std::visit(
        Overloaded{
            [&](const LogicalSourceDeclaration& declaration)
            {
                /// Qualifying the logical source name keeps it apart from the names of other test files.
                registry.declare(declaration.definition->sourceName->getText());
            },
            /// A physical source declares no name of its own.
            /// It references a logical source that another statement declares.
            [](const PhysicalSourceDeclaration&) {},
            [&](const ModelDeclaration& declaration)
            {
                /// A query that infers with a model refers to it, so a model qualifies like a source.
                registry.declare(declaration.definition->modelName->getText());
            },
            [&](const SinkDeclaration& declaration)
            {
                /// A test file that contains an EXPLAIN submits its sink declarations, so a plan that refers to a declared sink
                /// prints the name that the test gave it.
                /// An inlined sink prints as its type instead.
                /// A declared sink name qualifies like any other name.
                if (submitsDeclaredSinks)
                {
                    registry.declare(declaration.definition->sinkName->getText());
                }
                /// The rewriter inlines a sink into the query that writes to it, so this holds its type and schema and emits nothing.
                sinkByName.emplace(
                    Identifier::parse(declaration.definition->sinkName->getText()),
                    SinkDefinition{
                        .type = declaration.definition->type->getText(),
                        .schema = create.parse->textOf(declaration.definition->schemaDefinition())});
            }},
        create.declaration);
}

}

Declarations declareAll(ClassifiedTest& classified, const std::string& testFileKey)
{
    /// The query of an EXPLAIN can refer to a declared sink, and the plan prints that sink's name only when the sink exists in
    /// the catalog, so a file that contains an EXPLAIN registers its sink names and submits their declarations.
    /// An EXPLAIN whose query writes its sink inline needs no declaration, and an EXPLAIN runs nothing, so the
    /// declared sinks are never written.
    NameRegistry registry{testFileKey};
    SinkByName sinkByName;
    for (auto& statement : classified.statements)
    {
        if (auto* create = std::get_if<ClassifiedCreate>(&statement))
        {
            declareNames(*create, registry, sinkByName, classified.containsExplain);
        }
    }
    return Declarations{.names = std::move(registry).seal(), .sinkByName = std::move(sinkByName)};
}

}
