#include <Functions/TrimPhysicalFunction.hpp>
#include <gtest/gtest.h>
#include "Functions/ConstantValueVariableSizePhysicalFunction.hpp"

#include <Engine.hpp>

namespace NES
{

/// The string view is valid as long as the underlying memory is valid. For this test the lifetime is tied to the Arena
std::string_view fromVarSized(int8_t* ptrToVarSized)
{
    auto data = ptrToVarSized + 4;
    auto size = *reinterpret_cast<uint32_t*>(ptrToVarSized);
    return std::string_view(reinterpret_cast<char*>(data), size);
}

/// black magic to convert the variable size data to a string_view
ConstantValueVariableSizePhysicalFunction input(std::string_view sv)
{
    return ConstantValueVariableSizePhysicalFunction(reinterpret_cast<const int8_t*>(sv.data()), sv.size());
}

TEST(PhysicalFunction, BasicTest)
{
    /// UUT. Use a ConstantValue function as an input for the trim function
    TrimPhysicalFunction physicalFunction(input(" hello  World \t \n"));

    auto bm = BufferManager::create();
    Arena arena{bm};

    nautilus::engine::Options options;
    /// Switch between compiled and interpreted
    options.setOption("engine.Compilation", true);
    options.setOption("dump.all", true);
    options.setOption("dump.console", false);
    options.setOption("dump.file", true);

    nautilus::engine::NautilusEngine engine(options);

    auto function = engine.registerFunction(std::function(
        [&]()
        {
            ArenaRef arenaRef(&arena);
            const Record r{};
            return physicalFunction.execute(r, arenaRef).cast<VariableSizedData>().getReference();
        }));

    auto result = fromVarSized(function());
    EXPECT_EQ(result, std::string_view("hello  World"));
}
}
