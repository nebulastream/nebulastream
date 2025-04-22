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

#include <utility>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/QuerySignatureContext.hpp>
#include <ErrorHandling.hpp>
#include <magic_enum.hpp>

namespace NES::Optimizer
{
std::unique_ptr<AbstractRewriteRule> RewriteRuleGeneratedRegistrar::RegisterLowerToPhysicalSelectionRewriteRule()
{
    return std::make_unique<LowerToPhysicalSelection>();
}
}
