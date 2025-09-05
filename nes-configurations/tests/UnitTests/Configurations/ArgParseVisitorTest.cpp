#include <Configurations/ArgParseVisitor.hpp>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/Enums/EnumOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <argparse/argparse.hpp>
#include <gtest/gtest.h>

namespace NES
{
enum class MyTestEnum : uint8_t
{
    THIS,
    THAT
};

struct InnerConfiguration final : BaseConfiguration
{
    BoolOption flag{"flaggy", "false", "This is a flag"};
    EnumOption<MyTestEnum> thisOrThis{"this", MyTestEnum::THAT, "This or That"};

    InnerConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { }

protected:
    std::vector<BaseOption*> getOptions() override { return {&flag, &thisOrThis}; }
};

struct Configuration final : BaseConfiguration
{
    ScalarOption<std::string> a{"A", "False", "This is Option A"};
    ScalarOption<size_t> b{"B", "42", "This is Option B"};
    BoolOption flag{"flaggy", "false", "This is a flag"};
    InnerConfiguration innerConfig{"inner", "innerconfiguration"};

protected:
    std::vector<BaseOption*> getOptions() override { return {&a, &b, &flag, &innerConfig}; }
};

TEST(ATest, ATest2)
{
    argparse::ArgumentParser parser("Test Arguments");
    NES::ArgParseVisitor visitor{parser};

    Configuration config;
    config.accept(visitor);

    try
    {
        parser.parse_args({"wo", "--A=yeee", "--B", "32", "--inner.flaggy", "--inner.this", "THISs"});
        NES::ArgParseParserVisitor vis{parser};
        Configuration config2;
        config2.accept(vis);
    }
    catch (std::exception& e)
    {
        std::cerr << e.what() << std::endl;
        std::cerr << parser;
    }
}
}