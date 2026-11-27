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

#include <CoordinatorStatement.hpp>

#include <string>
#include <utility>
#include <variant>

#include <Util/Reflection.hpp>
#include <rfl/Generic.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

Reflected Reflector<CoordinatorStatement>::operator()(const CoordinatorStatement& statement, const ReflectionContext& context) const
{
    return std::visit(
        [&context]<typename Planned>(const Planned& planned)
        {
            const auto fields = context.reflect(planned)->to_object();
            INVARIANT(static_cast<bool>(fields), "a planned statement must reflect to a JSON object");
            auto object = *fields;
            object.insert(std::string{"tag"}, rfl::Generic{std::string{Planned::TAG}});
            return Reflected{rfl::Generic{std::move(object)}};
        },
        statement);
}

}
