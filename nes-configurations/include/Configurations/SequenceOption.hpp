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

#include "ISequenceOption.hpp"


#include <cstddef>
#include <vector>
#include <Configurations/BaseOption.hpp>
#include <Configurations/TypedBaseOption.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::Configurations
{
/// This class implements sequential options of a type that has to be a subtype of the BaseOption.
template <DerivedBaseOption T>
class SequenceOption : public ISequenceOption
{
public:
    SequenceOption(const std::string& name, const std::string& description);

    void clear() override;

    T& operator[](size_t index) override;
    const T& operator[](size_t index) const;

    [[nodiscard]] size_t size() const override;
    [[nodiscard]] std::vector<T> getValues() const;
    [[nodiscard]] bool empty() const;

    template <class X>
    void add(X value)
    {
        auto option = T();
        option.setValue(value);
        options.push_back(option);
    };

    void add(T option) { options.push_back(option); };

    std::string toString() override;

    void accept(ReadingVisitor&) override;
    void accept(WritingVisitor&) override;

protected:
    void parseFromYAMLNode(YAML::Node node) override;
    void parseFromString(std::string identifier, std::unordered_map<std::string, std::string>& inputParams) override;

private:
    std::vector<T> options;
};

template <DerivedBaseOption T>
SequenceOption<T>::SequenceOption(const std::string& name, const std::string& description) : ISequenceOption(name, description)
{
}

template <DerivedBaseOption T>
void SequenceOption<T>::clear()
{
    options.clear();
}

template <DerivedBaseOption T>
void SequenceOption<T>::parseFromYAMLNode(YAML::Node)
{
}

template <DerivedBaseOption T>
void SequenceOption<T>::parseFromString(std::string, std::unordered_map<std::string, std::string>&)
{
}

template <DerivedBaseOption T>
void SequenceOption<T>::accept(ReadingVisitor&)
{
}

template <DerivedBaseOption T>
void SequenceOption<T>::accept(WritingVisitor&)
{
}

template <DerivedBaseOption T>
T& SequenceOption<T>::operator[](size_t index)
{
    return options[index];
}

template <DerivedBaseOption T>
const T& SequenceOption<T>::operator[](size_t index) const
{
    return options[index];
}

template <DerivedBaseOption T>
size_t SequenceOption<T>::size() const
{
    return options.size();
}

template <DerivedBaseOption T>
std::vector<T> SequenceOption<T>::getValues() const
{
    return options;
}

template <DerivedBaseOption T>
bool SequenceOption<T>::empty() const
{
    return options.empty();
}

template <DerivedBaseOption T>
std::string SequenceOption<T>::toString()
{
    std::stringstream os;
    os << "Name: " << name << "\n";
    os << "Description: " << description << "\n";
    return os.str();
}
}
