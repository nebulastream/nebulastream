#ifndef NES_TESTS_UTIL_DUMMYSINK_HPP_
#define NES_TESTS_UTIL_DUMMYSINK_HPP_

#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
namespace NES {
class DummySink : public SinkDescriptor {
  public:
    static SinkDescriptorPtr create() {
        return std::make_shared<DummySink>();
    }
    DummySink() : SinkDescriptor(){};
    ~DummySink() override = default;
    std::string toString() override {
        return std::string();
    }
    bool equal(SinkDescriptorPtr) override {
        return false;
    }
};
}
#endif//NES_TESTS_UTIL_DUMMYSINK_HPP_
