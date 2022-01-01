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

#include "../../util/ProtobufMessageFactory.hpp"
#include <Catalogs/UdfCatalog.hpp>
#include <REST/Controller/UdfCatalogController.hpp>
#include <UdfCatalogService.pb.h>
#include <Util/Logger.hpp>
#include <cpprest/http_client.h>
#include <string>

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
        : udfCatalog(std::make_shared<UdfCatalog>()), udfCatalogController(UdfCatalogController(udfCatalog)) {}

    static RegisterJavaUdfRequest createRegisterJavaUdfRequest(const std::string& udfName,
                                                               const std::string& udfClassName,
                                                               const std::string& methodName,
                                                               const JavaSerializedInstance& serializedInstance,
                                                               const JavaUdfByteCodeList& byteCodeList) {
        auto javaUdfRequest = RegisterJavaUdfRequest{};
        javaUdfRequest.set_udf_name(udfName);
        auto* descriptorMessage = javaUdfRequest.mutable_java_udf_descriptor();
        descriptorMessage->set_udf_class_name(udfClassName);
        descriptorMessage->set_udf_method_name(methodName);
        descriptorMessage->set_serialized_instance(serializedInstance.data(), serializedInstance.size());
        for (const auto& [className, byteCode] : byteCodeList) {
            auto* javaClass = descriptorMessage->add_classes();
            javaClass->set_class_name(className);
            javaClass->set_byte_code(byteCode.data(), byteCode.size());
        }
        return javaUdfRequest;
    }

    static void verifyResponseStatusCode(const web::http::http_request& request,
                                         const web::http::status_code expectedStatusCode) {
        request.get_response()
            .then([=](const pplx::task<web::http::http_response>& task) {
                auto response = task.get();
                ASSERT_EQ(response.status_code(), expectedStatusCode);
            })
            .wait();
    }

    static void verifyResponseResult(const web::http::http_request& request, const web::json::value& expected) {
        request.get_response()
            .then([&expected](const pplx::task<web::http::http_response>& task) {
                auto response = task.get();
                response.extract_json()
                    .then([&expected](const pplx::task<web::json::value>& task) {
                        web::json::value json;
                        ASSERT_EQ(task.get(), expected);
                    })
                    .wait();
            })
            .wait();
    }

    static void verifySerializedInstance(const JavaSerializedInstance& actual, const std::string& expected) {
        auto converted = JavaSerializedInstance{expected.begin(), expected.end()};
        ASSERT_EQ(actual, converted);
    }

    static void
    verifyByteCodeList(const JavaUdfByteCodeList& actual,
                       const google::protobuf::RepeatedPtrField<JavaUdfDescriptorMessage_JavaUdfClassDefinition>& expected) {
        ASSERT_EQ(actual.size(), static_cast<decltype(actual.size())>(expected.size()));
        for (const auto& classDefinition : expected) {
            auto actualByteCode = actual.find(classDefinition.class_name());
            ASSERT_TRUE(actualByteCode != actual.end());
            auto converted = JavaByteCode(classDefinition.byte_code().begin(), classDefinition.byte_code().end());
            ASSERT_EQ(actualByteCode->second, converted);
        }
    }

    [[nodiscard]] static GetJavaUdfDescriptorResponse
    extractGetJavaUdfDescriptorResponse(const web::http::http_request& request) {
        GetJavaUdfDescriptorResponse response;
        request.get_response()
            .then([&response](const pplx::task<web::http::http_response>& task) {
                task.get()
                    .extract_string(true)
                    .then([&response](const pplx::task<std::string>& task) {
                        response.ParseFromString(task.get());
                    })
                    .wait();
            })
            .wait();
        return response;
    }

    UdfCatalogPtr udfCatalog;
    UdfCatalogController udfCatalogController;
};

TEST_F(UdfCatalogControllerTest, HandlePostToRegisterJavaUdfDescriptor) {
    // given a REST message containing a Java UDF
    auto javaUdfRequest = ProtobufMessageFactory::createRegisterJavaUdfRequest("my_udf",
                                                                               "some_package.my_udf",
                                                                               "udf_method",
                                                                               {1},
                                                                               {{"some_package.my_udf", {1}}});
    auto request = web::http::http_request{web::http::methods::POST};
    request.set_body(javaUdfRequest.SerializeAsString());
    // when that message is passed to the controller
    udfCatalogController.handlePost({UdfCatalogController::path_prefix, "registerJavaUdf"}, request);
    // then the HTTP response is OK
    verifyResponseStatusCode(request, web::http::status_codes::OK);
    // and the catalog contains a Java UDF descriptor representing the Java UDF
    auto javaUdfDescriptor = udfCatalog->getUdfDescriptor(javaUdfRequest.udf_name());
    ASSERT_NE(javaUdfDescriptor, nullptr);
    auto descriptorMessage = javaUdfRequest.java_udf_descriptor();
    ASSERT_EQ(javaUdfDescriptor->getClassName(), descriptorMessage.udf_class_name());
    ASSERT_EQ(javaUdfDescriptor->getMethodName(), descriptorMessage.udf_method_name());
    verifySerializedInstance(javaUdfDescriptor->getSerializedInstance(), descriptorMessage.serialized_instance());
    verifyByteCodeList(javaUdfDescriptor->getByteCodeList(), descriptorMessage.classes());
}

TEST_F(UdfCatalogControllerTest, HandlePostShouldVerifyUrlPathPrefix) {
    // given a REST message
    auto request = web::http::http_request{web::http::methods::POST};
    // when that message is passed to the controller with the wrong path prefix
    udfCatalogController.handlePost({"wrong-path-prefix"}, request);
    // then the HTTP response is InternalServerError
    verifyResponseStatusCode(request, web::http::status_codes::InternalError);
}

TEST_F(UdfCatalogControllerTest, HandlePostChecksForKnownPath) {
    // given a REST message
    auto request = web::http::http_request{web::http::methods::POST};
    // when that message is passed to the controller with an unknown path
    udfCatalogController.handlePost({UdfCatalogController::path_prefix, "unknown-path"}, request);
    // then the HTTP response is BadRequest
    verifyResponseStatusCode(request, web::http::status_codes::BadRequest);
}

TEST_F(UdfCatalogControllerTest, HandlePostHandlesException) {
    // given a REST message containing a wrongly formed Java UDF (bytecode list is empty)
    auto javaUdfRequest =
        ProtobufMessageFactory::createRegisterJavaUdfRequest("my_udf", "some_package.my_udf", "udf_method", {1}, {});
    auto request = web::http::http_request{web::http::methods::POST};
    request.set_body(javaUdfRequest.SerializeAsString());
    // when that message is passed to the controller (which should cause an exception)
    udfCatalogController.handlePost({UdfCatalogController::path_prefix, "registerJavaUdf"}, request);
    // then the response is BadRequest
    verifyResponseStatusCode(request, web::http::status_codes::BadRequest);
    // make sure the response does not contain the stack trace
    request.get_response()
        .then([=](const pplx::task<web::http::http_response>& task) {
            auto response = task.get();
            response.extract_string(true)
                .then([=](const pplx::task<std::string>& task) {
                    auto message = task.get();
                    ASSERT_TRUE(message.find("Stack trace") == std::string::npos);
                })
                .wait();
        })
        .wait();
}

TEST_F(UdfCatalogControllerTest, HandleDeleteToRemoveUdf) {
    // given the UDF catalog contains a Java UDF
    auto javaUdfDescriptor = JavaUdfDescriptor::create("some_package.my_udf"s,
                                                       "udf_method"s,
                                                       JavaSerializedInstance{1},
                                                       JavaUdfByteCodeList{{"some_package.my_udf"s, JavaByteCode{1}}});
    auto udfName = "my_udf"s;
    udfCatalog->registerJavaUdf(udfName, javaUdfDescriptor);
    // when a REST message is passed to the controller to remove the UDF
    auto request = web::http::http_request{web::http::methods::DEL};
    request.set_request_uri(UdfCatalogController::path_prefix + "/removeUdf?udfName="s + udfName);
    udfCatalogController.handleDelete({UdfCatalogController::path_prefix, "removeUdf"}, request);
    // then the response is OK
    verifyResponseStatusCode(request, web::http::status_codes::OK);
    // and the UDF does no longer exist in the catalog
    ASSERT_EQ(udfCatalog->getUdfDescriptor(udfName), nullptr);
    // and the response shows that the UDF was removed
    web::json::value json;
    json["removed"] = web::json::value(true);
    verifyResponseResult(request, json);
}

TEST_F(UdfCatalogControllerTest, HandleDeleteSignalsIfUdfDidNotExist) {
    // when a REST message is passed to the controller to remove a UDF that does not exist
    auto request = web::http::http_request{web::http::methods::DEL};
    request.set_request_uri(UdfCatalogController::path_prefix + "/removeUdf?udfName=unknown_udf"s);
    udfCatalogController.handleDelete({UdfCatalogController::path_prefix, "removeUdf"}, request);
    // then the response is OK
    verifyResponseStatusCode(request, web::http::status_codes::OK);
    // and the response shows that the UDF was not removed
    web::json::value json;
    json["removed"] = web::json::value(false);
    verifyResponseResult(request, json);
}

TEST_F(UdfCatalogControllerTest, HandleDeleteExpectsUdfParameter) {
    // when a REST message is passed to the controller that is missing the udfName parameter
    auto request = web::http::http_request{web::http::methods::DEL};
    request.set_request_uri(UdfCatalogController::path_prefix + "/removeUdf"s);
    udfCatalogController.handleDelete({UdfCatalogController::path_prefix, "removeUdf"}, request);
    // then the response is BadRequest
    verifyResponseStatusCode(request, web::http::status_codes::BadRequest);
}

TEST_F(UdfCatalogControllerTest, HandleDeleteTreatsSuperfluousParametersAreAnErrorCondition) {
    // when a REST message is passed to the controller that is contains parameters other than the udfName parameter
    auto request = web::http::http_request{web::http::methods::DEL};
    request.set_request_uri(UdfCatalogController::path_prefix + "/removeUdf?udfName=some_udf&unknownParameter=unknownValue"s);
    udfCatalogController.handleDelete({UdfCatalogController::path_prefix, "removeUdf"}, request);
    // then the response is BadRequest
    verifyResponseStatusCode(request, web::http::status_codes::BadRequest);
}

TEST_F(UdfCatalogControllerTest, HandleDeleteShouldVerifyUrlPathPrefix) {
    // given a REST message
    auto request = web::http::http_request{web::http::methods::DEL};
    // when that message is passed to the controller with the wrong path prefix
    udfCatalogController.handleDelete({"wrong-path-prefix"}, request);
    // then the HTTP response is InternalServerError
    verifyResponseStatusCode(request, web::http::status_codes::InternalError);
}

TEST_F(UdfCatalogControllerTest, HandleDeleteChecksForKnownPath) {
    // when a REST message is passed to the controller with an unknown path
    auto request = web::http::http_request{web::http::methods::DEL};
    request.set_request_uri(UdfCatalogController::path_prefix + "/unknown-path?udfName=unknown_udf"s);
    udfCatalogController.handleDelete({UdfCatalogController::path_prefix, "unknown-path"}, request);
    // then the HTTP response is BadRequest
    verifyResponseStatusCode(request, web::http::status_codes::BadRequest);
}

TEST_F(UdfCatalogControllerTest, HandleGetToRetrieveJavaUdfDescriptor) {
    // given the UDF catalog contains a Java UDF
    auto javaUdfDescriptor = JavaUdfDescriptor::create("some_package.my_udf"s,
                                                       "udf_method"s,
                                                       JavaSerializedInstance{1},
                                                       JavaUdfByteCodeList{{"some_package.my_udf"s, JavaByteCode{1}}});
    auto udfName = "my_udf"s;
    udfCatalog->registerJavaUdf(udfName, javaUdfDescriptor);
    // when a REST message is passed to the controller to get the UDF descriptor
    auto request = web::http::http_request{web::http::methods::GET};
    request.set_request_uri(UdfCatalogController::path_prefix + "/getUdfDescriptor?udfName="s + udfName);
    udfCatalogController.handleGet({UdfCatalogController::path_prefix, "getUdfDescriptor"}, request);
    // then the response is OK
    verifyResponseStatusCode(request, web::http::status_codes::OK);
    // and the response message indicates that the UDF was found
    auto response = extractGetJavaUdfDescriptorResponse(request);
    ASSERT_TRUE(response.found());
    // and the response contains the UDF descriptor
    const auto& descriptorMessage = response.java_udf_descriptor();
    ASSERT_EQ(javaUdfDescriptor->getClassName(), descriptorMessage.udf_class_name());
    ASSERT_EQ(javaUdfDescriptor->getMethodName(), descriptorMessage.udf_method_name());
    verifySerializedInstance(javaUdfDescriptor->getSerializedInstance(), descriptorMessage.serialized_instance());
    verifyByteCodeList(javaUdfDescriptor->getByteCodeList(), descriptorMessage.classes());
}

TEST_F(UdfCatalogControllerTest, HandleGetShouldReturnNotFoundIfUdfDoesNotExist) {
    // when a REST message is passed to the controller to get the UDF descriptor of an UDF that does not exist
    auto request = web::http::http_request{web::http::methods::GET};
    request.set_request_uri(UdfCatalogController::path_prefix + "/getUdfDescriptor?udfName=unknownUdf"s);
    udfCatalogController.handleGet({UdfCatalogController::path_prefix, "getUdfDescriptor"}, request);
    // then the response is OK
    verifyResponseStatusCode(request, web::http::status_codes::OK);
    // and the response message indicates that the UDF was not found
    auto response = extractGetJavaUdfDescriptorResponse(request);
    ASSERT_FALSE(response.found());
}

TEST_F(UdfCatalogControllerTest, HandleGetExpectsUdfParameter) {
    // when a REST message is passed to the controller that is missing the udfName parameter
    auto request = web::http::http_request{web::http::methods::GET};
    request.set_request_uri(UdfCatalogController::path_prefix + "/getUdfDescriptor"s);
    udfCatalogController.handleGet({UdfCatalogController::path_prefix, "getUdfDescriptor"}, request);
    // then the response is BadRequest
    verifyResponseStatusCode(request, web::http::status_codes::BadRequest);
}

TEST_F(UdfCatalogControllerTest, HandleGetTreatsSuperfluousParametersAreAnErrorCondition) {
    // when a REST message is passed to the controller that is contains parameters other than the udfName parameter
    auto request = web::http::http_request{web::http::methods::GET};
    request.set_request_uri(UdfCatalogController::path_prefix
                            + "/getUdfDescriptor?udfName=some_udf&unknownParameter=unknownValue"s);
    udfCatalogController.handleGet({UdfCatalogController::path_prefix, "getUdfDescriptor"}, request);
    // then the response is BadRequest
    verifyResponseStatusCode(request, web::http::status_codes::BadRequest);
}

TEST_F(UdfCatalogControllerTest, HandleGetShouldVerifyUrlPathPrefix) {
    // given a REST message
    auto request = web::http::http_request{web::http::methods::GET};
    // when that message is passed to the controller with the wrong path prefix
    udfCatalogController.handleGet({"wrong-path-prefix"}, request);
    // then the HTTP response is InternalServerError
    verifyResponseStatusCode(request, web::http::status_codes::InternalError);
}

TEST_F(UdfCatalogControllerTest, HandleGetChecksForKnownPath) {
    // when a REST message is passed to the controller with an unknown path
    auto request = web::http::http_request{web::http::methods::GET};
    request.set_request_uri(UdfCatalogController::path_prefix + "/unknown-path?udfName=unknown_udf"s);
    udfCatalogController.handleGet({UdfCatalogController::path_prefix, "unknown-path"}, request);
    // then the HTTP response is BadRequest
    verifyResponseStatusCode(request, web::http::status_codes::BadRequest);
}

TEST_F(UdfCatalogControllerTest, HandleGetToRetrieveListOfUdfs) {
    // given the UDF catalog contains a Java UDF
    auto javaUdfDescriptor = JavaUdfDescriptor::create("some_package.my_udf"s,
                                                       "udf_method"s,
                                                       JavaSerializedInstance{1},
                                                       JavaUdfByteCodeList{{"some_package.my_udf"s, JavaByteCode{1}}});
    auto udfName = "my_udf"s;
    udfCatalog->registerJavaUdf(udfName, javaUdfDescriptor);
    // when a REST message is passed to the controller to get a list of the UDFs
    auto request = web::http::http_request{web::http::methods::GET};
    udfCatalogController.handleGet({UdfCatalogController::path_prefix, "listUdfs"}, request);
    // then the response is OK
    verifyResponseStatusCode(request, web::http::status_codes::OK);
    // and the response message contains a list of UDFs
    web::json::value json;
    json["udfs"][0] = web::json::value(udfName);
    verifyResponseResult(request, json);
}

TEST_F(UdfCatalogControllerTest, HandleGetToRetrieveEmptyUdfList) {
    // when a REST message is passed to the controller to get a list of the UDFs
    auto request = web::http::http_request{web::http::methods::GET};
    udfCatalogController.handleGet({UdfCatalogController::path_prefix, "listUdfs"}, request);
    // then the response is OK
    verifyResponseStatusCode(request, web::http::status_codes::OK);
    // and the response message contains an empty list of UDFs
    web::json::value json;
    json["udfs"] = web::json::value::array();
    verifyResponseResult(request, json);
}

}// namespace NES