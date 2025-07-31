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

#include <Serialization/WindowAggregationLogicalFunctionReflection.hpp>

#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <ErrorHandling.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>

namespace NES
{
Reflected Reflector<TypedWindowAggregationLogicalFunction<>>::operator()(const TypedWindowAggregationLogicalFunction<>& function) const
{
    return reflect(
        detail::ReflectedWindowAggregationLogicalFunction{.functionType = std::string{function.getName()}, .functionConfig = function->reflect()});
}

TypedWindowAggregationLogicalFunction<> Unreflector<TypedWindowAggregationLogicalFunction<>>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto [name, data] = context.unreflect<detail::ReflectedWindowAggregationLogicalFunction>(rfl);

    auto aggregationFunction = AggregationLogicalFunctionRegistry::instance().unreflect(name, data, context);
    if (!aggregationFunction.has_value())
    {
        throw CannotDeserialize("Failed to unreflect window aggregation logical function of type {}", name);
    }
    return aggregationFunction.value();
}
}
