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

    void push(const ISequenceOption&) override { throw std::logic_error("Not implemented"); }

    void pop(const ISequenceOption&) override { throw std::logic_error("Not implemented"); }

protected:
    void visitLeaf(const BaseOption& option) override
    {
        path.push_back(option.getName());
        description = option.getDescription();
    }

    void visitEnum(std::span<std::string_view> allPossibleValues, const size_t& index) override
    {
        auto& argument = parser.add_argument(fmt::format("--{}", fmt::join(path, ".")))
                             .default_value(std::string(allPossibleValues[index]))
                             .help(description);
        for (const auto& possibleValue : allPossibleValues)
        {
            argument.add_choice(std::string(possibleValue));
        }
        path.pop_back();
    }

    void visitUnsignedInteger(const size_t& value) override
    {
        parser.add_argument(fmt::format("--{}", fmt::join(path, ".")))
            .nargs(1)
            .help(description)
            .default_value(fmt::format("{}", value))
            .template scan<'u', size_t>();
        path.pop_back();
    }

    void visitSignedInteger(const ssize_t& value) override
    {
        parser.add_argument(fmt::format("--{}", fmt::join(path, ".")))
            .nargs(1)
            .help(description)
            .default_value(fmt::format("{}", value))
            .template scan<'i', ssize_t>();
        path.pop_back();
    }

    void visitDouble(const double& value) override
    {
        parser.add_argument(fmt::format("--{}", fmt::join(path, ".")))
            .help(description)
            .nargs(1)
            .default_value(fmt::format("{}", value))
            .template scan<'f', double>();
        path.pop_back();
    }

    void visitBool(const bool&) override
    {
        parser.add_argument(fmt::format("--{}", fmt::join(path, "."))).help(description).flag();
        path.pop_back();
    }

    void visitString(const std::string& value) override
    {
        parser.add_argument(fmt::format("--{}", fmt::join(path, "."))).nargs(1).default_value(value).help(description);
        path.pop_back();
    }

private:
    std::string description;
    std::vector<std::string> path;
    argparse::ArgumentParser& parser;
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

    size_t push(ISequenceOption&) override { throw std::logic_error("Not implemented"); }

    void pop(ISequenceOption&) override { throw std::logic_error("Not implemented"); }

protected:
    void visitLeaf(BaseOption& option) override { path.push_back(option.getName()); }

    void visitEnum(std::span<std::string_view> allPossibleValues, size_t& index) override
    {
        auto value = parser.get<std::string>(fmt::format("--{}", fmt::join(path, ".")));
        auto it = std::ranges::find(allPossibleValues, value);
        INVARIANT(it != allPossibleValues.end(), "argparse should have rejected invalid values");
        index = std::distance(allPossibleValues.begin(), it);
        path.pop_back();
    }

    void visitUnsignedInteger(size_t& value) override
    {
        value = parser.get<size_t>(fmt::format("--{}", fmt::join(path, ".")));
        path.pop_back();
    }

    void visitSignedInteger(ssize_t& value) override
    {
        value = parser.get<ssize_t>(fmt::format("--{}", fmt::join(path, ".")));
        path.pop_back();
    }

    void visitDouble(double& value) override
    {
        value = parser.get<double>(fmt::format("--{}", fmt::join(path, ".")));
        path.pop_back();
    }

    void visitBool(bool& value) override
    {
        value = parser.get<bool>(fmt::format("--{}", fmt::join(path, ".")));
        path.pop_back();
    }

    void visitString(std::string& value) override
    {
        value = parser.get<std::string>(fmt::format("--{}", fmt::join(path, ".")));
        path.pop_back();
    }

private:
    std::vector<std::string> path;
    const argparse::ArgumentParser& parser;
};
}
