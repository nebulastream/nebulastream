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

#include <LookbackPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/ConstantValueVariableSizePhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <nautilus/static.hpp>
/// nautilus assembles val<> from several val_*.hpp; val.hpp is the umbrella users are meant to
/// include, so include-cleaner's preference for the individual defining headers is declined here.
#include <nautilus/val.hpp> /// NOLINT(misc-include-cleaner)
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

#include <LookbackConnections.hpp>
#include <LookbackContext.hpp>
#include <LookbackTemplate.hpp>

namespace NES
{

namespace
{

/// The physical registry hands over only the lowered child functions, so a constant argument's
/// bytes are reachable exactly one way — the accessor on the constant function itself. FMT does the
/// same with its format string.
std::string constantArgument(const PhysicalFunction& function, size_t index, const char* what)
{
    auto constant = function.tryGetAs<ConstantValueVariableSizePhysicalFunction>();
    if (!constant)
    {
        throw CannotInferSchema("LOOKBACK argument {} ({}) must be a constant VARSIZED literal", index + 1, what);
    }
    const auto& bytes = constant->get().getData();
    return std::string{reinterpret_cast<const char*>(bytes.data()), bytes.size()}; /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
}

}

LookbackPhysicalFunction::LookbackPhysicalFunction(
    std::shared_ptr<Lookback::LookbackContext> contextIn, std::vector<PhysicalFunction> parametersIn)
    : context(std::move(contextIn)), parameters(std::move(parametersIn))
{
}

VarVal LookbackPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    /// NOLINTNEXTLINE(misc-include-cleaner)
    const auto contextValue = nautilus::val<Lookback::LookbackContext*>(context.get());

    /// Stage this tuple's parameter values one at a time: a proxy call has a fixed C signature, so
    /// a variadic parameter list has to be pushed rather than passed. static_iterable unrolls at
    /// trace time, so the emitted code is a straight-line sequence of binds.
    nautilus::invoke(Lookback::lookbackBeginProxy, contextValue);
    for (const auto& parameter : nautilus::static_iterable(parameters))
    {
        const auto value = parameter.execute(record, arena).getRawValueAs<VariableSizedData>();
        nautilus::invoke(Lookback::lookbackBindProxy, contextValue, value.getContent(), value.getSize());
    }

    /// Two calls around the allocation — run reports the exact rendered length, copy fills the
    /// buffer — so the arena is never asked to reserve a worst-case maximum for a string that is
    /// usually short and frequently empty.
    /// NOLINTNEXTLINE(misc-include-cleaner)
    const nautilus::val<uint32_t> size = nautilus::invoke(Lookback::lookbackRunProxy, contextValue);
    auto result = arena.allocateVariableSizedData(nautilus::val<uint64_t>{size});
    nautilus::invoke(Lookback::lookbackCopyProxy, contextValue, result.getContent(), size);
    return VariableSizedData{result.getContent(), nautilus::val<uint64_t>{size}};
}

PhysicalFunctionRegistryReturnType LookbackPhysicalFunction::createLOOKBACK(PhysicalFunctionRegistryArguments arguments)
{
    constexpr size_t configurationArguments = 3;
    PRECONDITION(
        arguments.childFunctions.size() >= configurationArguments,
        "LOOKBACK requires at least {} arguments, but got {}",
        configurationArguments,
        arguments.childFunctions.size());

    const auto connectionName = constantArgument(arguments.childFunctions[0], 0, "connection name");
    const auto sql = constantArgument(arguments.childFunctions[1], 1, "lookback SQL");
    const auto rowTemplateText = constantArgument(arguments.childFunctions[2], 2, "row template");

    auto connection = Lookback::LookbackConnectionRegistry::instance().get(connectionName);
    auto rowTemplate = Lookback::RowTemplate::parse(rowTemplateText);
    auto context = std::make_shared<Lookback::LookbackContext>(std::move(connection), sql, std::move(rowTemplate));

    std::vector<PhysicalFunction> parameters{
        std::make_move_iterator(arguments.childFunctions.begin() + configurationArguments),
        std::make_move_iterator(arguments.childFunctions.end())};

    return LookbackPhysicalFunction(std::move(context), std::move(parameters));
}

}
