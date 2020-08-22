#ifndef NES_TESTS_UTIL_SCHEMASOURCEDESCRIPTOR_HPP_
#define NES_TESTS_UTIL_SCHEMASOURCEDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <API/Schema.hpp>
namespace NES{

class SchemaSourceDescriptor: public SourceDescriptor{
  public:
    static SourceDescriptorPtr create(SchemaPtr schema){
        return std::make_shared<SchemaSourceDescriptor>(schema);
    }
    explicit SchemaSourceDescriptor(SchemaPtr schema): SourceDescriptor(schema){

    }
    std::string toString() override {
        return "Schema Source Descriptor";
    }
    bool equal(SourceDescriptorPtr other) override {
        return other->getSchema()->equals(this->getSchema());
    }
    ~SchemaSourceDescriptor() override = default;
};


}

#endif//NES_TESTS_UTIL_SCHEMASOURCEDESCRIPTOR_HPP_
