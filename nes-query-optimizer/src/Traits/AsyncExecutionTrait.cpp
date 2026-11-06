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

#include <Traits/AsyncExecutionTrait.hpp>

#include <cstddef>
#include <string>
#include <string_view>
#include <typeinfo>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

const std::type_info& AsyncExecutionTrait::getType() const /// NOLINT(readability-convert-member-functions-to-static)
{
    return typeid(AsyncExecutionTrait);
}

size_t AsyncExecutionTrait::hash() const /// NOLINT(readability-convert-member-functions-to-static)
{
    return 0;
}

std::string AsyncExecutionTrait::explain(ExplainVerbosity) const /// NOLINT(readability-convert-member-functions-to-static)
{
    return "AsyncExecutionTrait";
}

std::string_view AsyncExecutionTrait::getName() const /// NOLINT(readability-convert-member-functions-to-static)
{
    return NAME;
}

Reflected Reflector<AsyncExecutionTrait>::operator()(const AsyncExecutionTrait&, const ReflectionContext&) const
{
    return Reflected{};
}

AsyncExecutionTrait Unreflector<AsyncExecutionTrait>::operator()(const Reflected&, const ReflectionContext&) const
{
    return AsyncExecutionTrait{};
}

}
