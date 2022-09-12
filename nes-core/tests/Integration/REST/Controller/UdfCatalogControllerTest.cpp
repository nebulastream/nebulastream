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

#include <Util/ProtobufMessageFactory.hpp>
#include <API/Query.hpp>
#include <NesBaseTest.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <REST/ServerTypes.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cpr/cpr.h>
#include <gtest/gtest.h>
#include <memory>
#include <nes-grpc/UdfCatalogService.pb.h>
#include <nlohmann/json.hpp>

namespace NES {
using namespace std::string_literals;
using namespace NES::Catalogs;
class UdfCatalogControllerTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ConnectivityControllerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TopologyControllerTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down ConnectivityControllerTest test class."); }

    static RegisterJavaUdfRequest createRegisterJavaUdfRequest(const std::string& udfName,
                                                               const std::string& udfClassName,
                                                               const std::string& methodName,
                                                               const UDF::JavaSerializedInstance& serializedInstance,
                                                               const UDF::JavaUdfByteCodeList& byteCodeList) {
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

    static void verifyResponseStatusCode(const cpr::Response& response,
                                         long expectedStatusCode) {
        ASSERT_EQ(response.status_code, expectedStatusCode);
    }

    static void verifyResponseResult(const cpr::Response& response, const nlohmann::json expected) {
        NES_DEBUG(response.text);
        auto responseJson =  nlohmann::json::parse(response.text);
        ASSERT_TRUE(responseJson == expected);

    }

    static void verifySerializedInstance(const UDF::JavaSerializedInstance& actual, const std::string& expected) {
        auto converted = UDF::JavaSerializedInstance{expected.begin(), expected.end()};
        ASSERT_EQ(actual, converted);
    }

    static void
    verifyByteCodeList(const UDF::JavaUdfByteCodeList& actual,
                       const google::protobuf::RepeatedPtrField<JavaUdfDescriptorMessage_JavaUdfClassDefinition>& expected) {
        ASSERT_EQ(actual.size(), static_cast<decltype(actual.size())>(expected.size()));
        for (const auto& classDefinition : expected) {
            auto actualByteCode = actual.find(classDefinition.class_name());
            ASSERT_TRUE(actualByteCode != actual.end());
            auto converted = UDF::JavaByteCode(classDefinition.byte_code().begin(), classDefinition.byte_code().end());
            ASSERT_EQ(actualByteCode->second, converted);
        }
    }

    [[nodiscard]] static GetJavaUdfDescriptorResponse
    extractGetJavaUdfDescriptorResponse(const cpr::Response& response) {
        GetJavaUdfDescriptorResponse udfResponse;
        udfResponse.ParseFromString(response.text);
        return udfResponse;
    }

};

TEST_F(UdfCatalogControllerTest, HandlePostToRegisterJavaUdfDescriptor) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryCatalogControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);

    if (!success) {
        FAIL() << "Rest server failed to start";
    }


    // given a REST message containing a Java UDF
    auto javaUdfRequest = ProtobufMessageFactory::createRegisterJavaUdfRequest("my_udf",
                                                                               "some_package.my_udf",
                                                                               "udf_method",
                                                                               {1},
                                                                               {{"some_package.my_udf", {1}}});
    auto response   = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udf-catalog/registerJavaUdf"},
                              cpr::Header{{"Content-Type", "text/plain"}}, cpr::Body{javaUdfRequest.SerializeAsString()},
                              cpr::ConnectTimeout{3000}, cpr::Timeout{3000});
    // then the HTTP response is OK
    verifyResponseStatusCode(response, 200);

    auto response2   = cpr::Get(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udf-catalog/getUdfDescriptor"},
                              cpr::Parameters{{"udfName", javaUdfRequest.udf_name()}},
                              cpr::ConnectTimeout{3000}, cpr::Timeout{3000});
    //check if udf entry exists
    ASSERT_EQ(response2.status_code, 200l);
    // and the catalog contains a Java UDF descriptor representing the Java UDF
    auto udfCatalog = coordinator->getUdfCatalog();
    auto javaUdfDescriptor = UDF::UdfDescriptor::as<UDF::JavaUdfDescriptor>(udfCatalog->getUdfDescriptor(javaUdfRequest.udf_name()));
    //JavaUdfDescriptorn
    ASSERT_NE(javaUdfDescriptor, nullptr);
    auto descriptorMessage = javaUdfRequest.java_udf_descriptor();
    ASSERT_EQ(javaUdfDescriptor->getClassName(), descriptorMessage.udf_class_name());
    ASSERT_EQ(javaUdfDescriptor->getMethodName(), descriptorMessage.udf_method_name());
    verifySerializedInstance(javaUdfDescriptor->getSerializedInstance(), descriptorMessage.serialized_instance());
    verifyByteCodeList(javaUdfDescriptor->getByteCodeList(), descriptorMessage.classes());
}

TEST_F(UdfCatalogControllerTest, HandlePostShouldVerifyUrlPathPrefix) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryCatalogControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);

    if (!success) {
        FAIL() << "Rest server failed to start";
    }

    auto response   = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udf-catalog/SUPER_SECRET_URL"},
                              cpr::Header{{"Content-Type", "text/plain"}}, cpr::Body{"Whats the object-oriented way to become wealthy? Inheritance."},
                              cpr::ConnectTimeout{3000}, cpr::Timeout{3000});
    // given a REST message
    verifyResponseStatusCode(response, 404l);
}

TEST_F(UdfCatalogControllerTest, HandlePostHandlesException) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryCatalogControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);

    if (!success) {
        FAIL() << "Rest server failed to start";
    }

    // given a REST message containing a wrongly formed Java UDF (bytecode list is empty)
    auto javaUdfRequest =
        ProtobufMessageFactory::createRegisterJavaUdfRequest("my_udf", "some_package.my_udf", "udf_method", {1}, {});
    auto response   = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udf-catalog/registerJavaUdf"},
                              cpr::Header{{"Content-Type", "text/plain"}}, cpr::Body{javaUdfRequest.SerializeAsString()},
                              cpr::ConnectTimeout{3000}, cpr::Timeout{3000});
    // then the response is BadRequest
    verifyResponseStatusCode(response, 400);
    // make sure the response does not contain the stack trace
    ASSERT_TRUE(response.text.find("Stack trace") == std::string::npos);
}

TEST_F(UdfCatalogControllerTest, HandleDeleteToRemoveUdf) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryCatalogControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);

    if (!success) {
        FAIL() << "Rest server failed to start";
    }


    // given a REST message containing a Java UDF
    auto javaUdfRequest = ProtobufMessageFactory::createRegisterJavaUdfRequest("my_udf",
                                                                               "some_package.my_udf",
                                                                               "udf_method",
                                                                               {1},
                                                                               {{"some_package.my_udf", {1}}});
    auto response   = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udf-catalog/registerJavaUdf"},
                              cpr::Header{{"Content-Type", "text/plain"}}, cpr::Body{javaUdfRequest.SerializeAsString()},
                              cpr::ConnectTimeout{3000}, cpr::Timeout{3000});
    // then the HTTP response is OK
    verifyResponseStatusCode(response, 200);
    // given the UDF catalog contains a Java UDF
    // when a REST message is passed to the controller to remove the UDF
    auto response2   = cpr::Delete(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udf-catalog/removeUdf"},
                              cpr::Parameters{{"udfName", "my_udf"}},
                              cpr::ConnectTimeout{3000}, cpr::Timeout{3000});
    // then the response is OK
    verifyResponseStatusCode(response2, 200);
    // and the UDF does no longer exist in the catalog
    auto udfCatalog = coordinator->getUdfCatalog();
    ASSERT_EQ(UDF::UdfDescriptor::as<UDF::JavaUdfDescriptor>(udfCatalog->getUdfDescriptor("my_udf")), nullptr);
    // and the response shows that the UDF was removed
    nlohmann::json json;
    json["removed"] = true;
    auto responseJson = nlohmann::json::parse(response2.text);
    verifyResponseResult(response2, json);
}

TEST_F(UdfCatalogControllerTest, HandleDeleteSignalsIfUdfDidNotExist) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryCatalogControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);

    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    // when a REST message is passed to the controller to remove a UDF that does not exist
    auto response   = cpr::Delete(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udf-catalog/removeUdf"},
                                 cpr::Parameters{{"udfName", "my_udf"}},
                                 cpr::ConnectTimeout{3000}, cpr::Timeout{3000});
    // then the response is OK
    verifyResponseStatusCode(response, 200);
    // and the response shows that the UDF was not removed
    nlohmann::json json;
    json["removed"] = false;
    verifyResponseResult(response, json);
}

TEST_F(UdfCatalogControllerTest, HandleDeleteExpectsUdfParameter) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryCatalogControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);

    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    // when a REST message is passed to the controller that is missing the udfName parameter
    auto response   = cpr::Delete(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udf-catalog/removeUdf"},
                                cpr::ConnectTimeout{3000}, cpr::Timeout{3000});
    // then the response is BadRequest
    verifyResponseStatusCode(response, 400);
}

TEST_F(UdfCatalogControllerTest, HandleDeleteTreatsSuperfluousParametersAreIgnored) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryCatalogControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);

    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    // when a REST message is passed to the controller that is contains parameters other than the udfName parameter
    auto response   = cpr::Delete(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udf-catalog/removeUdf"},
                                cpr::Parameters{{"udfName", "my_udf"}, {"meaning_of_life", "42"}},
                                cpr::ConnectTimeout{3000}, cpr::Timeout{3000});

    // then the response is BadRequest
    verifyResponseStatusCode(response, 200);
    //Oatpp ignores all query parameters that arent defined in the endpoint
}


TEST_F(UdfCatalogControllerTest, HandleGetShouldReturnNotFoundIfUdfDoesNotExist) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryCatalogControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);

    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    // when a REST message is passed to the controller to get the UDF descriptor of an UDF that does not exist
    auto response   = cpr::Get(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udf-catalog/getUdfDescriptor"},
                              cpr::Parameters{{"udfName", "greatest-udf-of-all-time"}},
                              cpr::ConnectTimeout{3000}, cpr::Timeout{3000});
    auto request = web::http::http_request{web::http::methods::GET};
    // then the response is OK
    verifyResponseStatusCode(response, 200);
    // and the response message indicates that the UDF was not found
    auto response1 = extractGetJavaUdfDescriptorResponse(response);
    ASSERT_FALSE(response1.found());
}

TEST_F(UdfCatalogControllerTest, HandleGetExpectsUdfParameter) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryCatalogControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);

    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    // when a REST message is passed to the controller that is missing the udfName parameter
    auto response   = cpr::Get(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udf-catalog/getUdfDescriptor"},
                             cpr::ConnectTimeout{3000}, cpr::Timeout{3000});
    auto request = web::http::http_request{web::http::methods::GET};
    // then the response is BadRequest
    verifyResponseStatusCode(response, 400);
}

TEST_F(UdfCatalogControllerTest, HandleGetToRetrieveListOfUdfs) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryCatalogControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);

    if (!success) {
        FAIL() << "Rest server failed to start";
    }


    // given a REST message containing a Java UDF
    auto javaUdfRequest = ProtobufMessageFactory::createRegisterJavaUdfRequest("my_udf",
                                                                               "some_package.my_udf",
                                                                               "udf_method",
                                                                               {1},
                                                                               {{"some_package.my_udf", {1}}});
    auto response   = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udf-catalog/registerJavaUdf"},
                              cpr::Header{{"Content-Type", "text/plain"}}, cpr::Body{javaUdfRequest.SerializeAsString()},
                              cpr::ConnectTimeout{3000}, cpr::Timeout{3000});
    // then the HTTP response is OK
    verifyResponseStatusCode(response, 200);
    // when a REST message is passed to the controller to get a list of the UDFs
    auto response2   = cpr::Get(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udf-catalog/listUdfs"},
                              cpr::ConnectTimeout{3000}, cpr::Timeout{3000});
    // then the response is OK
    verifyResponseStatusCode(response2, 200);
    // and the response message contains a list of UDFs
    nlohmann::json json;
    std::vector<std::string> udfs;
    udfs.emplace_back("my_udf");
    json["udfs"] = udfs;
    verifyResponseResult(response2, json);
}

TEST_F(UdfCatalogControllerTest, HandleGetToRetrieveEmptyUdfList) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryCatalogControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);

    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    // when a REST message is passed to the controller to get a list of the UDFs
    auto response   = cpr::Get(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udf-catalog/listUdfs"},
                              cpr::ConnectTimeout{3000}, cpr::Timeout{3000});
    // then the response is OK
    verifyResponseStatusCode(response, 200);
    // and the response message contains an empty list of UDFs
    nlohmann::json json;
    std::vector<std::string> udfs = {};
    json["udfs"] = udfs;
    verifyResponseResult(response, json);
}
}// namespace NES