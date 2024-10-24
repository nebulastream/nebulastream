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

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <stack>
#include <string>
#include <string_view>
#include <typeindex>
#include <unordered_map>
#include <utility>
#include <vector>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/util/json_util.h>
#include <sys/types.h>
#include <magic_enum.hpp>


/// TODO #429: This is currently not implemented in Nebulastream!
/// I copied the configuration structure we have and simplified it a bit.
/// The prototype only supports templated TypedConfigurationOptions (including enums) and CompoundConfigurationOptions.
///
/// The issue is that our visitor does not have direct access to the value of the option. This means serialization cannot read from the
/// configuration object and deserialization can not write into the object. To provide access to the underlying value of each configuration
/// option we have to provide concrete visit functions for them. Unfortunately, we can not rely sole on templates because templated methods
/// can not be virtual. The OptionVisitor can expose a templated `visit` function which then dispatches onto the virtual type specific visit
/// functions (e.g. `visitFloat`). An implementation of the OptionVisitor needs to implement all typed visit functions to gain access to the
/// underlying value and can now free read or write into the configuration option.
/// We coalesce types into broader categories, so we do not have to implement visitUint64, visitUint32..., but can just used visitUnsigned.
///
/// We may have to adapt the OptionVisitor to support SequenceOptions.


/// Mimic Implementation of NebulaStream for the sake of prototyping.
struct OptionVisitor;

class BaseOption
{
public:
    BaseOption() = default;

    /// Constructor to create a new option.
    BaseOption(std::string name, std::string description) : name(std::move(name)), description(std::move(description)) { }
    virtual ~BaseOption() = default;
    BaseOption(const BaseOption& other) = default;
    BaseOption(BaseOption&& other) noexcept = default;
    BaseOption& operator=(const BaseOption& other) = default;
    BaseOption& operator=(BaseOption&& other) noexcept = default;

    /// Clears the option and sets a default value if available.
    virtual void clear() = 0;

    virtual void accept(OptionVisitor&) = 0;

    [[nodiscard]] const std::string& getName() const { return name; }
    [[nodiscard]] const std::string& getDescription() const { return description; }

protected:
    friend class BaseConfiguration;

    std::string name;
    std::string description;
};


class CompoundConfiguration : public BaseOption
{
public:
    CompoundConfiguration(const std::string& name, const std::string& description) : BaseOption(name, description) { }
    CompoundConfiguration() = default;

    /// clears all options and set the default values
    void clear() override
    {
        for (auto* opt : getOptions())
        {
            opt->clear();
        }
    }

    void accept(OptionVisitor& visitor) override;

protected:
    virtual std::vector<BaseOption*> getOptions() = 0;
};

template <typename T>
class TypedConfigurationOption : public BaseOption
{
public:
    TypedConfigurationOption() = default;
    template <typename U = T>
    TypedConfigurationOption(const std::string& name, const std::string& description, U&& defaultValue)
        : BaseOption(name, description), defaultValue(std::forward<U>(defaultValue)), value(this->defaultValue)
    {
    }

    void clear() override { value = defaultValue; }
    void accept(OptionVisitor&) override;
    [[nodiscard]] const T& getValue() const { return value; }
    template <typename U = T>
    void setValue(U&& newValue)
    {
        value = std::forward<U>(newValue);
    }

private:
    T defaultValue;
    T value;
};

template <typename T>
requires std::is_enum_v<T>
class EnumOption : public TypedConfigurationOption<T>
{
public:
    EnumOption() = default;
    EnumOption(const std::string& name, const std::string& description, T defaultValue)
        : TypedConfigurationOption<T>(name, description, defaultValue)
    {
    }
};

/// New OptionVisitor Interface:
/// templated `visit` function that dispatches to concrete typed visits
struct OptionVisitor
{
    virtual ~OptionVisitor() = default;
    template <typename T>
    void visit(TypedConfigurationOption<T>& option);

    /// When starting a compound type. All subsequent visits are part of the CompoundType.
    virtual void push(BaseOption& o) = 0;
    /// When finishing a compound type
    virtual void pop(BaseOption& o) = 0;

protected:
    /// Called for every leaf element. This is called before calling the typed visit functions
    virtual void visitLeaf(BaseOption& o) = 0;

    /// Typed visit functions:

    ///we lose the concrete enum type, but sometimes we want to present the human-readable enum value so the visitEnum
    ///provides both: string value and underlying value.
    virtual void visitEnum(std::string_view enumName, size_t& underlying) = 0;
    virtual void visitUnsignedInteger(size_t&) = 0;
    virtual void visitSignedInteger(ssize_t&) = 0;
    virtual void visitFloat(double&) = 0;
    virtual void visitBool(bool&) = 0;
    virtual void visitString(std::string&) = 0;
};

template <typename T>
void OptionVisitor::visit(TypedConfigurationOption<T>& option)
{
    visitLeaf(option);
    /// bool first because it also counts as a unsigned integral
    if constexpr (std::same_as<bool, T>)
    {
        bool v = option.getValue();
        visitBool(v);
        option.setValue(static_cast<T>(v));
    }
    else if constexpr (std::signed_integral<T>)
    {
        ssize_t v = option.getValue();
        visitSignedInteger(v);
        option.setValue(static_cast<T>(v));
    }
    else if constexpr (std::unsigned_integral<T>)
    {
        size_t v = option.getValue();
        visitUnsignedInteger(v);
        option.setValue(static_cast<T>(v));
    }
    else if constexpr (std::floating_point<T>)
    {
        double v = option.getValue();
        visitFloat(v);
        option.setValue(static_cast<T>(v));
    }
    else if constexpr (std::convertible_to<std::string, T> && std::convertible_to<T, std::string>)
    {
        std::string v = option.getValue();
        visitString(v);
        option.setValue(static_cast<T>(v));
    }
    else if constexpr (std::is_enum_v<T>)
    {
        static_assert(std::unsigned_integral<magic_enum::underlying_type_t<T>>, "This is only implemented for unsigned enums");
        size_t v = static_cast<magic_enum::underlying_type_t<T>>(option.getValue());
        visitEnum(magic_enum::enum_name(option.getValue()), v);
        option.setValue(magic_enum::enum_cast<T>(v).value());
    }
    else
    {
        static_assert(false, "Not handled");
    }
}

template <typename T>
void TypedConfigurationOption<T>::accept(OptionVisitor& o)
{
    o.visit(*this);
}

void CompoundConfiguration::accept(OptionVisitor& visitor)
{
    visitor.push(*this);
    for (auto* option : getOptions())
    {
        option->accept(visitor);
    }
    visitor.pop(*this);
}

/// Example Visitor Implementations:

/// Prints all values of a configuration
struct PrintingVisitor : OptionVisitor
{
    explicit PrintingVisitor(std::ostream& os) : os(os) { }
    void push(BaseOption& o) override
    {
        os << std::string(indent * 4, ' ') << o.getName() << ": " << o.getDescription() << '\n';
        indent++;
    }
    void pop(BaseOption&) override { indent--; }

protected:
    void visitLeaf(BaseOption& o) override { os << std::string(indent * 4, ' ') << o.getName(); }
    void visitEnum(std::string_view enumName, size_t&) override { os << ": " << enumName << '\n'; }
    void visitUnsignedInteger(size_t& v) override { os << ": " << v << " (Unsigned Integer)" << '\n'; }
    void visitSignedInteger(ssize_t& v) override { os << ": " << v << " (Signed Integer)" << '\n'; }
    void visitFloat(double& v) override { os << ": " << v << " (Float)" << '\n'; }
    void visitBool(bool& v) override { os << ": " << (v ? "True" : "False") << " (Bool)" << '\n'; }
    void visitString(std::string& v) override { os << ": " << v << " (String)" << '\n'; }

private:
    std::ostream& os;
    size_t indent = 0;
};

/// Inserts a new Protobuf Message type into the FileDescriptorProto. The Message type can be loaded and instantiated into
/// a new protobuf message using a DynamicMessageFactory
struct ProtobufMessageTypeBuilderOptionVisitor : OptionVisitor
{
    explicit ProtobufMessageTypeBuilderOptionVisitor(google::protobuf::FileDescriptorProto& proto, std::string configName)
        : protoFile(proto)
    {
        current.push(protoFile.add_message_type());
        current.top()->set_name(configName);
        fieldNumber.push(1);
    }

    void push(BaseOption& o) override
    {
        auto* field = current.top()->add_field();
        auto* type = current.top()->add_nested_type();

        type->set_name(o.getName() + "_type_name");

        field->set_name(o.getName());
        field->set_type(google::protobuf::FieldDescriptorProto::TYPE_MESSAGE);
        field->set_type_name(o.getName() + "_type_name");
        field->set_number(fieldNumber.top()++);

        currentField.push(field);
        current.push(type);
        fieldNumber.push(1);
    }

    void pop(BaseOption&) override
    {
        currentField.pop();
        current.pop();
        fieldNumber.pop();
    }

protected:
    void visitLeaf(BaseOption& o) override
    {
        auto* child = current.top();
        currentField.push(child->add_field());
        currentField.top()->set_name(o.getName());
        currentField.top()->set_number(fieldNumber.top()++);
    }

    void visitEnum(std::string_view, size_t&) override
    {
        currentField.top()->set_type(google::protobuf::FieldDescriptorProto_Type_TYPE_UINT64);
    }
    void visitUnsignedInteger(size_t&) override { currentField.top()->set_type(google::protobuf::FieldDescriptorProto_Type_TYPE_UINT64); }
    void visitSignedInteger(ssize_t&) override { currentField.top()->set_type(google::protobuf::FieldDescriptorProto_Type_TYPE_INT64); }
    void visitFloat(double&) override { currentField.top()->set_type(google::protobuf::FieldDescriptorProto_Type_TYPE_DOUBLE); }
    void visitBool(bool&) override { currentField.top()->set_type(google::protobuf::FieldDescriptorProto_Type_TYPE_BOOL); }
    void visitString(std::string&) override { currentField.top()->set_type(google::protobuf::FieldDescriptorProto_Type_TYPE_STRING); }


private:
    std::stack<google::protobuf::DescriptorProto*> current;
    std::stack<google::protobuf::FieldDescriptorProto*> currentField;
    std::stack<size_t> fieldNumber;
    std::unique_ptr<google::protobuf::DescriptorPool> pool = std::make_unique<google::protobuf::DescriptorPool>();
    std::unique_ptr<google::protobuf::DynamicMessageFactory> factory
        = std::make_unique<google::protobuf::DynamicMessageFactory>(pool.get());
    google::protobuf::FileDescriptorProto& protoFile;
};

/// Uses protobuf reflection util to dynamically fill a protobuf message with the contents of a configuration object.
struct ProtobufSerializeOptionVisitor final : OptionVisitor
{
    explicit ProtobufSerializeOptionVisitor(google::protobuf::Message* message)
    {
        const google::protobuf::Descriptor* descriptor = message->GetDescriptor();
        reflections.push(message->GetReflection());
        messages.push(message);
        descriptors.push(descriptor);
    }

    void push(BaseOption& o) override
    {
        const auto* descriptor = descriptors.top()->FindFieldByName(o.getName());
        if (!descriptor)
        {
            std::cerr << "Field not found: " << o.getName() << std::endl;
            assert(descriptor && "Field not found");
        }
        auto* message = reflections.top()->MutableMessage(messages.top(), descriptor);
        const auto* reflection = message->GetReflection();

        reflections.push(reflection);
        messages.push(message);
        descriptors.push(message->GetDescriptor());
    }
    void pop(BaseOption&) override
    {
        reflections.pop();
        messages.pop();
        descriptors.pop();
    }

protected:
    void visitLeaf(BaseOption& o) override { fieldDescriptor = descriptors.top()->FindFieldByName(o.getName()); }
    void visitEnum(std::string_view, size_t& underlying) override
    {
        reflections.top()->SetUInt64(messages.top(), fieldDescriptor, underlying);
    }
    void visitUnsignedInteger(size_t& v) override { reflections.top()->SetUInt64(messages.top(), fieldDescriptor, v); }
    void visitSignedInteger(ssize_t& v) override { reflections.top()->SetInt64(messages.top(), fieldDescriptor, v); }
    void visitFloat(double& v) override { reflections.top()->SetDouble(messages.top(), fieldDescriptor, v); }
    void visitBool(bool& v) override { reflections.top()->SetBool(messages.top(), fieldDescriptor, v); }
    void visitString(std::string& s) override { reflections.top()->SetString(messages.top(), fieldDescriptor, s); }

private:
    const google::protobuf::FieldDescriptor* fieldDescriptor{};
    std::stack<google::protobuf::Message*> messages;
    std::stack<const google::protobuf::Reflection*> reflections;
    std::stack<const google::protobuf::Descriptor*> descriptors;
};

/// Uses protobuf reflection util to dynamically write the contents of a protobuf message into a configuration object.
struct ProtobufDeserializeVisitor final : OptionVisitor
{
    explicit ProtobufDeserializeVisitor(google::protobuf::Message* message)
    {
        const google::protobuf::Descriptor* descriptor = message->GetDescriptor();
        reflections.push(message->GetReflection());
        messages.push(message);
        descriptors.push(descriptor);
    }

    void push(BaseOption& o) override
    {
        const auto* descriptor = descriptors.top()->FindFieldByName(o.getName());
        auto* message = reflections.top()->MutableMessage(messages.top(), descriptor);
        const auto* reflection = message->GetReflection();

        reflections.push(reflection);
        messages.push(message);
        descriptors.push(message->GetDescriptor());
    }
    void pop(BaseOption&) override
    {
        reflections.pop();
        messages.pop();
        descriptors.pop();
    }

protected:
    void visitLeaf(BaseOption& o) override { fieldDescriptor = descriptors.top()->FindFieldByName(o.getName()); }
    void visitEnum(std::string_view, size_t& underlying) override
    {
        underlying = reflections.top()->GetUInt64(*messages.top(), fieldDescriptor);
    }
    void visitUnsignedInteger(size_t& v) override { v = reflections.top()->GetUInt64(*messages.top(), fieldDescriptor); }
    void visitSignedInteger(ssize_t& v) override { v = reflections.top()->GetInt64(*messages.top(), fieldDescriptor); }
    void visitFloat(double& v) override { v = reflections.top()->GetDouble(*messages.top(), fieldDescriptor); }
    void visitBool(bool& v) override { v = reflections.top()->GetBool(*messages.top(), fieldDescriptor); }
    void visitString(std::string& s) override { s = reflections.top()->GetString(*messages.top(), fieldDescriptor); }

private:
    const google::protobuf::FieldDescriptor* fieldDescriptor{};
    std::stack<google::protobuf::Message*> messages;
    std::stack<const google::protobuf::Reflection*> reflections;
    std::stack<const google::protobuf::Descriptor*> descriptors;
};


/// Test Configurations
enum Test
{
    TestMode,
    ReleaseMode,
    BenchmarkMode,
};

class MyInnerConfiguration final : public CompoundConfiguration
{
public:
    MyInnerConfiguration() = default;
    MyInnerConfiguration(const std::string& name, const std::string& description) : CompoundConfiguration(name, description) { }

    TypedConfigurationOption<std::string> message = {"Message", "This is a message", "Hi!"};
    TypedConfigurationOption<bool> enable = {"Enable", "Is this enabled?", true};
    TypedConfigurationOption<uint8_t> test = {"NumberOfBytes", "number of bytes", 32};

protected:
    std::vector<BaseOption*> getOptions() override { return {&message, &enable, &test}; }
};

class MyTopLevelConfiguration : public CompoundConfiguration
{
public:
    MyTopLevelConfiguration(const std::string& name, const std::string& description) : CompoundConfiguration(name, description) { }
    MyTopLevelConfiguration() = default;

    EnumOption<Test> testMode = {"TestMode", "Which mode?", TestMode};
    TypedConfigurationOption<int> numberOfTests = {"NumberOfTests", "How Many Tests", 41};
    MyInnerConfiguration inner{"Inner", "This is the inner config"};

protected:
    std::vector<BaseOption*> getOptions() override { return {&testMode, &numberOfTests, &inner}; }
};

/// We do not want rebuild every protobuf message type whenever we want to serialize a configuration.
/// This class caches protobuf message types and provides a convenient way to create a new protobuf message based on a
/// configuration class.
struct ProtobufConfigCache
{
    ProtobufConfigCache() { protoFile.set_name("configurations"); }

    template <typename ConfigurationClass>
    google::protobuf::Message* getEmptyMessage()
    {
        if (auto it = configNames.find(std::type_index(typeid(ConfigurationClass))); it != configNames.end())
        {
            auto fileDesc = pool.BuildFile(protoFile);
            const google::protobuf::Descriptor* messageDesc = fileDesc->FindMessageTypeByName(it->second);
            return factory.GetPrototype(messageDesc)->New();
        }

        ProtobufMessageTypeBuilderOptionVisitor option(protoFile, typeid(ConfigurationClass).name());
        ConfigurationClass configuration("root", "");
        configuration.accept(option);
        configNames[std::type_index(typeid(ConfigurationClass))] = typeid(ConfigurationClass).name();
        return getEmptyMessage<ConfigurationClass>();
    }

    google::protobuf::DescriptorPool pool;
    google::protobuf::DynamicMessageFactory factory;
    google::protobuf::FileDescriptorProto protoFile;
    std::unordered_map<std::type_index, std::string> configNames;
};

/// How to use it!
int main()
{
    ProtobufConfigCache cache;

    MyTopLevelConfiguration configuration("root", "");
    /// Modify configuration so it does not contain the default values
    configuration.testMode.setValue(ReleaseMode);
    configuration.numberOfTests.setValue(1);
    configuration.inner.enable.setValue(false);

    auto* message = cache.getEmptyMessage<MyTopLevelConfiguration>();
    ProtobufSerializeOptionVisitor serializer(message);
    configuration.accept(serializer);

    /// Second configuration with default values
    MyTopLevelConfiguration receiveConfiguration("root", "");
    PrintingVisitor printingVisitor(std::cout);

    /// Print default values
    receiveConfiguration.accept(printingVisitor);

    /// Deserialize message overwriting default configuration
    ProtobufDeserializeVisitor deserializer(message);
    receiveConfiguration.accept(deserializer);

    /// Print updated values
    receiveConfiguration.accept(printingVisitor);
}
