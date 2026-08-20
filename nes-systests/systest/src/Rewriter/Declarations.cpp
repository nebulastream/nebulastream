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

#include <utility>
#include <variant>

#include <Identifiers/Identifier.hpp>
#include <Rewriter/ClassifiedStatement.hpp>
#include <Rewriter/NamePrefixer.hpp>
#include <Rewriter/SinkRewriting.hpp>
#include <Util/Overloaded.hpp>

namespace NES
{
namespace
{

void declareNames(const ClassifiedCreate& create, NameRegistry& registry, SinkByName& sinkByName, const bool submitsDeclaredSinks)
{
    std::visit(
        Overloaded{
            [&](const LogicalSourceDeclaration& declaration) { registry.declare(declaration.definition->sourceName->getText()); },
            /// A physical source declares no name of its own.
            /// It references a logical source that another statement declares.
            [](const PhysicalSourceDeclaration&) {},
            [&](const ModelDeclaration& declaration)
            {
                /// A query that infers with a model refers to it, so a model is prefixed like a source.
                registry.declare(declaration.definition->modelName->getText());
            },
            [&](const SinkDeclaration& declaration)
            {
                /// A sink name is catalog-visible only when its declaration is submitted.
                if (submitsDeclaredSinks)
                {
                    registry.declare(declaration.definition->sinkName->getText());
                }
                /// Inlining needs the type and the schema either way.
                sinkByName.emplace(
                    Identifier::parse(declaration.definition->sinkName->getText()),
                    SinkDefinition{
                        .type = declaration.definition->type->getText(),
                        .schema = create.parse->textOf(declaration.definition->schemaDefinition())});
            }},
        create.declaration);
}

}

Declarations declareAll(const ClassifiedTestFile& classified, const TestFileKey& testFileKey)
{
    NameRegistry registry{testFileKey};
    SinkByName sinkByName;
    for (auto& create : classified.setup)
    {
        declareNames(create, registry, sinkByName, classified.containsExplain);
    }
    return Declarations{.names = std::move(registry).seal(), .sinkByName = std::move(sinkByName)};
}

}
