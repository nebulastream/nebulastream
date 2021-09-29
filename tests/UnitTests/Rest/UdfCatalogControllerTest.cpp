/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <gtest/gtest.h>
#include <cpprest/http_client.h>
#include <string>
#include <REST/Controller/UdfCatalogController.hpp>
#include <Catalogs/UdfCatalog.hpp>
#include <Util/Logger.hpp>
#include <UdfCatalogService.pb.h>

using namespace std::string_literals;
using namespace NES::Catalogs;

namespace NES {

class UdfCatalogControllerTest : public testing::Test {
  protected:
    static void SetUpTestCase() {
        NES::setupLogging("UdfTest.log", NES::LOG_DEBUG);
        GOOGLE_PROTOBUF_VERIFY_VERSION;
    }

    UdfCatalogControllerTest()
        : udfCatalog(std::make_shared<UdfCatalog>()),
          udfCatalogController(UdfCatalogController(udfCatalog))
    {}

    static RegisterJavaUdfRequest createRegisterJavaUdfRequest(const std::string& udfName,
                                                               const std::string& udfClassName,
                                                               const std::string& methodName,
                                                               const JavaSerializedInstance& serializedInstance,
                                                               const JavaUdfByteCodeList& byteCodeList) {
        auto javaUdfRequest = RegisterJavaUdfRequest{};
        javaUdfRequest.set_udf_name(udfName);
        javaUdfRequest.set_udf_class_name(udfClassName);
        javaUdfRequest.set_udf_method_name(methodName);
        javaUdfRequest.set_serialized_instance(serializedInstance.data(), serializedInstance.size());
        for (const auto& [className, byteCode] : byteCodeList) {
            auto* javaClass = javaUdfRequest.add_classes();
            javaClass->set_class_name(className);
            javaClass->set_byte_code(std::string{byteCode.begin(), byteCode.end()});
        }
        return javaUdfRequest;
    }

    static void verifyResponseStatusCode(const web::http::http_request& request,
                                  const web::http::status_code expectedStatusCode) {
        request.get_response()
            .then([=](const pplx::task<http_response>& task) {
                auto response = task.get();
                ASSERT_EQ(response.status_code(), expectedStatusCode);
            })
            .wait();
    }

    static void verifySerializedInstance(const JavaSerializedInstance& actual,
                                         const std::string& expected) {
        auto converted = JavaSerializedInstance {expected.begin(), expected.end()};
        ASSERT_EQ(actual, converted);
    }

    static void verifyByteCodeList(const JavaUdfByteCodeList& actual,
                                   const google::protobuf::RepeatedPtrField<RegisterJavaUdfRequest_JavaUdfClassDefinition>& expected) {
        ASSERT_EQ(actual.size(), static_cast<decltype(actual.size())>(expected.size()));
        for (const auto& classDefinition : expected) {
            auto actualByteCode = actual.find(classDefinition.class_name());
            ASSERT_TRUE(actualByteCode != actual.end());
            auto converted = JavaByteCode(classDefinition.byte_code().begin(), classDefinition.byte_code().end());
            ASSERT_EQ(actualByteCode->second, converted);
        }
    }

    UdfCatalogPtr udfCatalog;
    UdfCatalogController udfCatalogController;
};

TEST_F(UdfCatalogControllerTest, HandlePostToRegisterJavaUdfDescriptor) {
    // given a REST message containing a Java UDF
    auto javaUdfRequest = createRegisterJavaUdfRequest("my_udf",
                                                       "some_package.my_udf",
                                                       "udf_method",
                                                       {1},
                                                       {{"some_package.my_udf", {1}}});
    auto request = web::http::http_request {web::http::methods::POST};
    request.set_body(javaUdfRequest.SerializeAsString());
    // when that message is passed to the controller
    udfCatalogController.handlePost({"udfCatalog", "registerJavaUdf"}, request);
    // then the HTTP response is OK
    verifyResponseStatusCode(request, status_codes::OK);
    // and the catalog contains a Java UDF descriptor representing the Java UDF
    auto javaUdfDescriptor = udfCatalog->getUdfDescriptor(javaUdfRequest.udf_name());
    ASSERT_NE(javaUdfDescriptor, nullptr);
    ASSERT_EQ(javaUdfDescriptor->getClassName(), javaUdfRequest.udf_class_name());
    ASSERT_EQ(javaUdfDescriptor->getMethodName(), javaUdfRequest.udf_method_name());
    verifySerializedInstance(javaUdfDescriptor->getSerializedInstance(), javaUdfRequest.serialized_instance());
    verifyByteCodeList(javaUdfDescriptor->getByteCodeList(), javaUdfRequest.classes());
}

} // namespace NES