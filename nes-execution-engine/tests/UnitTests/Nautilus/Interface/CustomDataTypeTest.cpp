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

#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/DataTypes/InvocationPlugin.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Experimental/NESIR/Types/IntegerStamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::ExecutionEngine::Experimental::Interpreter {

class CustomDataTypeTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("CustomDataTypeTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup CustomDataTypeTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup CustomDataTypeTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down CustomDataTypeTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down CustomDataTypeTest test class." << std::endl; }
};

class CustomType : public Any {
  public:
    static const inline auto type = TypeIdentifier::create<CustomType>();
    CustomType(Value<> x, Value<> y) : Any(&type), x(x), y(y){};

    std::shared_ptr<CustomType> add(const CustomType& other) const {
        return std::make_unique<CustomType>(x + other.x, y + other.y);
    }

    std::shared_ptr<Int64> power(const CustomType& other) const {
        return std::make_unique<Int64>(x * other.x - y);
    }

    Value<> x;
    Value<> y;
    std::shared_ptr<Any> copy() override { return std::make_unique<CustomType>(x, y); }
};

class CustomTypeInvocationPlugin : public InvocationPlugin {
  public:
    std::optional<Value<>> Add(const Value<>& left, const Value<>& right) const override {
        if (isa<CustomType>(left.value) && isa<CustomType>(right.value)) {
            auto& ct1 = left.getValue().staticCast<CustomType>();
            auto& ct2 = right.getValue().staticCast<CustomType>();
            return Value(ct1.add(ct2));
        } else {
            return std::nullopt;
        }
    }
};

[[maybe_unused]] static InvocationPluginRegistry::Add<CustomTypeInvocationPlugin> cPlugin;

TEST_F(CustomDataTypeTest, customCustomDataTypeTest) {
    Value<Int64> x = 32l;
    Value<Int64> y = 32l;

    auto c1 = Value<CustomType>(std::make_shared<CustomType>(x, y));
    auto c2 = Value<CustomType>(std::make_shared<CustomType>(x, y));

    auto c3 = c1 + c2;
    c1 = c2;
    NES_DEBUG(c3.value);
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter