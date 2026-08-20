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

#include <Rewriter/SqlRewriter.hpp>

#include <utility>
#include <variant>
#include <Model/RewrittenTest.hpp>
#include <Parser/SystestParser.hpp>
#include <Parser/TestFileParser.hpp>
#include <Rewriter/Declarations.hpp>
#include <Rewriter/Emitter.hpp>
#include <Rewriter/PreparedStatement.hpp>

namespace NES
{

SqlRewriter::SqlRewriter(RewriteTarget target) : target{std::move(target)}
{
}

RewrittenTest SqlRewriter::rewrite(const TestFile& testFile) const
{
    auto prepared = prepare(testFile);
    auto declarations = declareAll(prepared, target.testFileKey);
    return Emitter{target, std::move(declarations)}.emit(prepared);
}

}
