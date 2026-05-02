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

#include <PointPhysicalFunctions.hpp>

#include <map>
#include <string>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <std/cmath.h>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val.hpp>

namespace NES
{

namespace
{
/// Component-wise binary op over Point's component map. `nautilus::static_iterable`
/// wraps the host-side std::map so the iteration is unrolled at trace time;
/// a plain range-for would produce "Invalid trace. constant loop".
template <typename Op>
Value componentWise(const Value& left, const Value& right, Op&& op)
{
    std::map<std::string, VarVal> result;
    for (const auto& [suffix, leftComponent] : nautilus::static_iterable(left.components()))
    {
        result.emplace(suffix, op(leftComponent, right.component(suffix)).castToType(DataType::Type::FLOAT64));
    }
    return Value{std::move(result)};
}
}

/// ===== PointConstructPhysicalFunction =====

PointConstructPhysicalFunction::PointConstructPhysicalFunction(PhysicalFunction x, PhysicalFunction y, PhysicalFunction z)
    : x(std::move(x)), y(std::move(y)), z(std::move(z))
{
}

Value PointConstructPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::map<std::string, VarVal> components;
    components.emplace("_X", x.execute(record, arena));
    components.emplace("_Y", y.execute(record, arena));
    components.emplace("_Z", z.execute(record, arena));
    return Value{std::move(components)};
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterPointPhysicalFunction(PhysicalFunctionRegistryArguments args)
{
    PRECONDITION(args.childFunctions.size() == 3, "Point construct requires exactly three children");
    return PointConstructPhysicalFunction{args.childFunctions[0], args.childFunctions[1], args.childFunctions[2]};
}

/// ===== PointAddPhysicalFunction =====

PointAddPhysicalFunction::PointAddPhysicalFunction(PhysicalFunction left, PhysicalFunction right)
    : left(std::move(left)), right(std::move(right))
{
}

Value PointAddPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    return componentWise(left.execute(record, arena), right.execute(record, arena), [](const VarVal& l, const VarVal& r) { return l + r; });
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterPointAddPhysicalFunction(PhysicalFunctionRegistryArguments args)
{
    PRECONDITION(args.childFunctions.size() == 2, "PointAdd requires exactly two children");
    return PointAddPhysicalFunction{args.childFunctions[0], args.childFunctions[1]};
}

/// ===== PointSubPhysicalFunction =====

PointSubPhysicalFunction::PointSubPhysicalFunction(PhysicalFunction left, PhysicalFunction right)
    : left(std::move(left)), right(std::move(right))
{
}

Value PointSubPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    return componentWise(left.execute(record, arena), right.execute(record, arena), [](const VarVal& l, const VarVal& r) { return l - r; });
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterPointSubPhysicalFunction(PhysicalFunctionRegistryArguments args)
{
    PRECONDITION(args.childFunctions.size() == 2, "PointSub requires exactly two children");
    return PointSubPhysicalFunction{args.childFunctions[0], args.childFunctions[1]};
}

/// ===== PointScalePhysicalFunction =====

PointScalePhysicalFunction::PointScalePhysicalFunction(PhysicalFunction point, PhysicalFunction scalar)
    : point(std::move(point)), scalar(std::move(scalar))
{
}

Value PointScalePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto pointValue = point.execute(record, arena);
    const auto scalarValue = scalar.execute(record, arena);
    const auto& factor = scalarValue.asScalar();
    std::map<std::string, VarVal> result;
    for (const auto& [suffix, component] : nautilus::static_iterable(pointValue.components()))
    {
        result.emplace(suffix, (component * factor).castToType(DataType::Type::FLOAT64));
    }
    return Value{std::move(result)};
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterPointScalePhysicalFunction(PhysicalFunctionRegistryArguments args)
{
    PRECONDITION(args.childFunctions.size() == 2, "PointScale requires exactly two children");
    return PointScalePhysicalFunction{args.childFunctions[0], args.childFunctions[1]};
}

/// ===== PointDistancePhysicalFunction =====

PointDistancePhysicalFunction::PointDistancePhysicalFunction(PhysicalFunction left, PhysicalFunction right)
    : left(std::move(left)), right(std::move(right))
{
}

Value PointDistancePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto leftValue = left.execute(record, arena);
    const auto rightValue = right.execute(record, arena);
    VarVal sumOfSquares{nautilus::val<double>(0.0)};
    for (const auto& [suffix, leftComponent] : nautilus::static_iterable(leftValue.components()))
    {
        const auto delta = (leftComponent - rightValue.component(suffix)).castToType(DataType::Type::FLOAT64);
        sumOfSquares = (sumOfSquares + (delta * delta)).castToType(DataType::Type::FLOAT64);
    }
    return Value::scalar(VarVal{nautilus::sqrt(sumOfSquares.getRawValueAs<nautilus::val<double>>())});
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterPointDistancePhysicalFunction(PhysicalFunctionRegistryArguments args)
{
    PRECONDITION(args.childFunctions.size() == 2, "PointDistance requires exactly two children");
    return PointDistancePhysicalFunction{args.childFunctions[0], args.childFunctions[1]};
}

}
