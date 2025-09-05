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
#pragma once
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>
#include <Configurations/ReadingVisitor.hpp>
#include <Configurations/WritingVisitor.hpp>
#include <argparse/argparse.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

namespace NES
{
class ArgParseVisitor final : public ReadingVisitor
{
public:
    explicit ArgParseVisitor(argparse::ArgumentParser& parser) : parser(parser) { }

    void push(const BaseOption& option) override
    {
        if (!option.getName().empty())
        {
            path.push_back(option.getName());
        }
    }

    void pop(const BaseOption& option) override
    {
        if (!option.getName().empty())
        {
            path.pop_back();
        }
    }

    void push(const ISequenceOption& option) override
    {
        isSequence = true;
        option.defaultValue()->accept(*this);
    }

    void pop(const ISequenceOption&) override { isSequence = false; }

protected:
    void visitLeaf(const BaseOption& option) override
    {
        path.push_back(option.getName());
        description = option.getDescription();
    }

    void visitEnum(std::span<std::string_view> allPossibleValues, const size_t& index) override
    {
        auto& argument = parser.add_argument(fmt::format("--{}", fmt::join(path, ".")));
        for (const auto& possibleValue : allPossibleValues)
        {
            argument.add_choice(std::string(possibleValue));
        }
        argument.help(fmt::format("{}. [{}]", description, fmt::join(allPossibleValues, "|")));
        if (isSequence)
        {
            argument.nargs(argparse::nargs_pattern::any);
        }
        else
        {
            argument.default_value(std::string(allPossibleValues[index])).nargs(1);
        }
        path.pop_back();
    }

    void visitUnsignedInteger(const size_t& value) override
    {
        auto& argument = parser.add_argument(fmt::format("--{}", fmt::join(path, "."))).help(description).template scan<'u', size_t>();

        if (isSequence)
        {
            argument.nargs(argparse::nargs_pattern::any);
        }
        else
        {
            argument.nargs(1).default_value(value);
        }

        path.pop_back();
    }

    void visitSignedInteger(const ssize_t& value) override
    {
        auto& argument = parser.add_argument(fmt::format("--{}", fmt::join(path, "."))).help(description).template scan<'i', ssize_t>();
        if (isSequence)
        {
            argument.nargs(argparse::nargs_pattern::any);
        }
        else
        {
            argument.nargs(1).default_value(value);
        }
        path.pop_back();
    }

    void visitDouble(const double& value) override
    {
        auto& argument = parser.add_argument(fmt::format("--{}", fmt::join(path, "."))).help(description).template scan<'f', double>();
        if (isSequence)
        {
            argument.nargs(argparse::nargs_pattern::any);
        }
        else
        {
            argument.nargs(1).default_value(value);
        }
        path.pop_back();
    }

    void visitBool(const bool&) override
    {
        auto& argument = parser.add_argument(fmt::format("--{}", fmt::join(path, "."))).help(description);
        if (isSequence)
        {
            argument.nargs(argparse::nargs_pattern::any);
        }
        else
        {
            argument.flag();
        }
        path.pop_back();
    }

    void visitString(const std::string& value) override
    {
        auto& argument = parser.add_argument(fmt::format("--{}", fmt::join(path, "."))).help(description);
        if (isSequence)
        {
            argument.nargs(argparse::nargs_pattern::any);
        }
        else
        {
            argument.nargs(1).default_value(value);
        }
        path.pop_back();
    }

private:
    bool isSequence = false;
    std::string description;
    std::vector<std::string> path;
    argparse::ArgumentParser& parser;
};

struct VectorSizeVisitor final : ReadingVisitor
{
    VectorSizeVisitor(const argparse::ArgumentParser& parser, const std::string& name, size_t& result)
        : parser(parser), name(name), result(result)
    {
    }

    void push(const BaseOption&) override { throw std::runtime_error("Vector size not implemented"); }

    void pop(const BaseOption&) override { throw std::runtime_error("Vector size not implemented"); }

    void push(const ISequenceOption&) override { throw std::runtime_error("Vector size not implemented"); }

    void pop(const ISequenceOption&) override { throw std::runtime_error("Vector size not implemented"); }

protected:
    void visitLeaf(const BaseOption&) override{};

    void visitEnum(std::span<std::string_view>, const size_t&) override { result = parser.get<std::vector<std::string>>(name).size(); }

    void visitUnsignedInteger(const size_t&) override { result = parser.get<std::vector<size_t>>(name).size(); }

    void visitSignedInteger(const ssize_t&) override { result = parser.get<std::vector<ssize_t>>(name).size(); }

    void visitDouble(const double&) override { result = parser.get<std::vector<double>>(name).size(); }

    void visitBool(const bool&) override { result = parser.get<std::vector<bool>>(name).size(); }

    void visitString(const std::string&) override { result = parser.get<std::vector<std::string>>(name).size(); }

public:
    const argparse::ArgumentParser& parser;
    const std::string& name;
    size_t& result;
};

class ArgParseParserVisitor final : public WritingVisitor
{
public:
    explicit ArgParseParserVisitor(const argparse::ArgumentParser& parser) : parser(parser) { }

    void push(BaseOption& option) override
    {
        if (!option.getName().empty())
        {
            path.push_back(option.getName());
        }
    }

    void pop(BaseOption& option) override
    {
        if (!option.getName().empty())
        {
            path.pop_back();
        }
    }

    size_t push(ISequenceOption& option) override
    {
        inSequence = true;
        index = 0;
        path.push_back(option.getName());
        const auto arg = fmt::format("--{}", fmt::join(path, "."));

        if (!parser.is_used(arg))
        {
            return 0;
        }

        size_t size = 0;
        VectorSizeVisitor visitor{parser, arg, size};
        option.defaultValue()->accept(visitor);
        return size;
    }

    void pop(ISequenceOption&) override
    {
        inSequence = false;
        path.pop_back();
    }

protected:
    template <typename T>
    T readValue()
    {
        auto argumentName = fmt::format("--{}", fmt::join(path, "."));
        if (inSequence)
        {
            return parser.get<std::vector<T>>(argumentName).at(index++);
        }
        path.pop_back();
        return parser.get<T>(argumentName);
    }

    void visitLeaf(BaseOption& option) override
    {
        if (!inSequence)
        {
            path.push_back(option.getName());
        }
    }

    void visitEnum(std::span<std::string_view> allPossibleValues, size_t& index) override
    {
        auto value = readValue<std::string>();
        auto it = std::ranges::find(allPossibleValues, value);
        INVARIANT(it != allPossibleValues.end(), "argparse should have rejected invalid values");
        index = std::distance(allPossibleValues.begin(), it);
    }

    void visitUnsignedInteger(size_t& value) override { value = readValue<size_t>(); }

    void visitSignedInteger(ssize_t& value) override { value = readValue<ssize_t>(); }

    void visitDouble(double& value) override { value = readValue<double>(); }

    void visitBool(bool& value) override { value = readValue<bool>(); }

    void visitString(std::string& value) override { value = readValue<std::string>(); }

private:
    bool inSequence;
    size_t index;
    std::vector<std::string> path;
    const argparse::ArgumentParser& parser;
};
}
