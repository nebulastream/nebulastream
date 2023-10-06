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

#include <API/Query.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/UDF/JavaUDFDescriptor.hpp>
#include <Services/QueryParsingService.hpp>
#include <UdfCatalogService.pb.h>
#include <Util/JavaUDFDescriptorBuilder.hpp>
#include <Util/PythonUDFDescriptorBuilder.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ProtobufMessageFactory.hpp>
#include <Util/TestUtils.hpp>
#include <cpr/cpr.h>
#include <google/protobuf/text_format.h>
#include <gtest/gtest.h>
#include <memory>
#include <nlohmann/json.hpp>
#include <oatpp/web/protocol/http/Http.hpp>

namespace NES {
using namespace std::string_literals;
using namespace NES::Catalogs;
using namespace google::protobuf;
using namespace oatpp::web::protocol::http;

class UDFCatalogControllerTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ConnectivityControllerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ConnectivityControllerTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down ConnectivityControllerTest test class."); }

    static void verifyResponseResult(const cpr::Response& response, const nlohmann::json expected) {
        NES_DEBUG("{}", response.text);
        nlohmann::json responseJson;
        ASSERT_NO_THROW(responseJson = nlohmann::json::parse(response.text));
        ASSERT_TRUE(responseJson == expected);
    }

    static void verifySerializedInstance(const jni::JavaSerializedInstance& actual, const std::string& expected) {
        auto converted = jni::JavaSerializedInstance{expected.begin(), expected.end()};
        ASSERT_EQ(actual, converted);
    }

    static void
    verifyByteCodeList(const jni::JavaUDFByteCodeList& actual,
                       const google::protobuf::RepeatedPtrField<JavaUdfDescriptorMessage_JavaUdfClassDefinition>& expected) {
        ASSERT_EQ(actual.size(), static_cast<decltype(actual.size())>(expected.size()));
        for (const auto& classDefinition : expected) {
            auto actualByteCode = std::find_if(actual.cbegin(), actual.cend(), [&](const jni::JavaClassDefinition& c) {
                return c.first == classDefinition.class_name();
            });
            ASSERT_TRUE(actualByteCode != actual.end());
            auto converted = jni::JavaByteCode(classDefinition.byte_code().begin(), classDefinition.byte_code().end());
            ASSERT_EQ(actualByteCode->second, converted);
        }
    }

    [[nodiscard]] static GetUDFDescriptorResponse extractGetUDFDescriptorResponse(const cpr::Response& response) {
        GetUDFDescriptorResponse udfResponse;
        udfResponse.ParseFromString(response.text);
        return udfResponse;
    }

    void startCoordinator() {
        NES_INFO("UdfCatalogController: Start coordinator");
        coordinatorConfig = CoordinatorConfiguration::createDefault();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;

        coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
        ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
        NES_INFO("UDFCatalogControllerTest: Coordinator started successfully");
    }

    void stopCoordinator() {
        bool stopCrd = coordinator->stopCoordinator(true);
        ASSERT_TRUE(stopCrd);
    }

    NesCoordinatorPtr coordinator;
    CoordinatorConfigurationPtr coordinatorConfig;
};

//Test if retrieval of a UDF added directly to UDFCatalog over POST returns same UDF
TEST_F(UDFCatalogControllerTest, getUDFJavaDescriptorReturnsUDF) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    auto udfCatalog = coordinator->getUDFCatalog();

    // Check Java UDF Descriptor
    // create a Java Udf descriptor and specify an udf name
    std::string udfName = "my_udf_java";
    auto javaUdfDescriptor = Catalogs::UDF::JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor();

    //register udf with coordinator
    udfCatalog->registerUDF(udfName, javaUdfDescriptor);

    //send a GET request to REST API of coordinator for the previously defined java udf
    auto future = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/getUdfDescriptor"},
                                cpr::Parameters{{"udfName", udfName}});
    future.wait();
    auto response = future.get();
    //check if response code indicates a udf has been retrieved
    ASSERT_EQ(response.status_code, Status::CODE_200.code);
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Origin"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Methods"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Headers"));
    // extract protobuf message from string response
    GetUDFDescriptorResponse udfResponse = extractGetUDFDescriptorResponse(response);
    // from protobuf message, get the java udf descriptor
    auto udfDescriptor = udfResponse.udfdescriptor();
    JavaUdfDescriptorMessage descriptor;
    udfDescriptor.descriptormessage().UnpackTo(&descriptor);
    // and compare udf descriptor with the one registered to the coordinator earlier
    ASSERT_EQ(javaUdfDescriptor->getClassName(), descriptor.udf_class_name());
    ASSERT_EQ(javaUdfDescriptor->getMethodName(), descriptor.udf_method_name());
    verifySerializedInstance(javaUdfDescriptor->getSerializedInstance(), descriptor.serialized_instance());
    verifyByteCodeList(javaUdfDescriptor->getByteCodeList(), descriptor.classes());
    stopCoordinator();
}

//Test if retrieval of a UDF added directly to UDFCatalog over POST returns same UDF
TEST_F(UDFCatalogControllerTest, getUDFPythonDescriptorReturnsUDF) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    auto udfCatalog = coordinator->getUDFCatalog();

    // Check Java UDF Descriptor
    // create a Java Udf descriptor and specify an udf name
    std::string udfName = "my_udf_python";
    auto pythonUDFDescriptor = Catalogs::UDF::PythonUDFDescriptorBuilder::createDefaultPythonUDFDescriptor();

    //register udf with coordinator
    udfCatalog->registerUDF(udfName, pythonUDFDescriptor);

    //send a GET request to REST API of coordinator for the previously defined java udf
    auto future = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/getUdfDescriptor"},
                                cpr::Parameters{{"udfName", udfName}});
    future.wait();
    auto response = future.get();
    //check if response code indicates a udf has been retrieved
    ASSERT_EQ(response.status_code, Status::CODE_200.code);
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Origin"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Methods"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Headers"));
    // extract protobuf message from string response
    GetUDFDescriptorResponse udfResponse = extractGetUDFDescriptorResponse(response);
    // from protobuf message, get the java udf descriptor
    auto udfDescriptor = udfResponse.udfdescriptor();
    PythonUDFDescriptorMessage descriptor;
    udfDescriptor.descriptormessage().UnpackTo(&descriptor);
    // and compare udf descriptor with the one registered to the coordinator earlier
    ASSERT_EQ(pythonUDFDescriptor->getMethodName(), descriptor.udf_method_name());
    ASSERT_EQ(pythonUDFDescriptor->getFunctionString(), descriptor.function_string());
    stopCoordinator();
}

// Test behavior of GET for a UDF that doesn't exist
TEST_F(UDFCatalogControllerTest, testGetUdfDescriptorIfNoUdfExists) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    auto udfCatalog = coordinator->getUDFCatalog();

    std::string udfName = "my_udf";
    //send a GET request to REST API of coordinator for an udf that doesn't exist
    auto future = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/getUdfDescriptor"},
                                cpr::Parameters{{"udfName", udfName}});

    future.wait();
    auto response = future.get();
    //check that status code indicates specified udf doesn't exist
    ASSERT_EQ(response.status_code, Status::CODE_404.code);
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Origin"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Methods"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Headers"));
    // and compare contents of response
    // extract protobuf message from string response
    GetUDFDescriptorResponse udfResponse = extractGetUDFDescriptorResponse(response);
    ASSERT_TRUE(!udfResponse.found() && !udfResponse.has_udfdescriptor());
    stopCoordinator();
}

//Test if Oatpp framework correctly returns 404 when endpoint isn't defined
TEST_F(UDFCatalogControllerTest, testErrorIfUnknownEndpointIsUsed) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    //create a request to an endpoint that isn't defined
    auto future = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/SUPER_SECRET_URL"},
                                 cpr::Header{{"Content-Type", "text/plain"}},
                                 cpr::Body{"Whats the object-oriented way to become wealthy? Inheritance."});

    future.wait();
    auto response = future.get();
    // and see if the response code is 404
    ASSERT_EQ(response.status_code, Status::CODE_404.code);
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Origin"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Methods"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Headers"));
    stopCoordinator();
}

//Test if RegisterUdf endpoint handles exceptions without returning a stack trace
TEST_F(UDFCatalogControllerTest, testIfRegisterEndpointHandlesExceptionsWithoutReturningAStackTrace) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    // given a REST message containing a wrongly formed Java UDF (bytecode list is empty)
    auto javaUdfRequest = ProtobufMessageFactory::createDefaultRegisterJavaUdfRequest();
    JavaUdfDescriptorMessage descriptor;
    javaUdfRequest.mutable_udfdescriptor()->mutable_descriptormessage()->UnpackTo(&descriptor);
    descriptor.clear_classes();
    // have to do this otherwise the classes are not cleared in the java udf request
    javaUdfRequest.mutable_udfdescriptor()->mutable_descriptormessage()->PackFrom(descriptor);

    auto future = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/registerUdf"},
                                 cpr::Header{{"Content-Type", "text/plain"}},
                                 cpr::Body{javaUdfRequest.SerializeAsString()});
    future.wait();
    auto response = future.get();
    // then the response is BadRequest
    ASSERT_EQ(response.status_code, Status::CODE_400.code);
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Origin"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Methods"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Headers"));
    // make sure the response does not contain the stack trace
    ASSERT_TRUE(response.text.find("Stack trace") == std::string::npos);
    stopCoordinator();
}

//Test if registerJavaUdf endpoint correctly adds java udf
TEST_F(UDFCatalogControllerTest, testIfRegisterUdfEndpointCorrectlyAddsJavaUDF) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    auto udfCatalog = coordinator->getUDFCatalog();
    //check to see if no udfs registered
    ASSERT_TRUE(udfCatalog->listUDFs().empty());
    // create a javaUdfRequest
    auto javaUdfRequest = ProtobufMessageFactory::createDefaultRegisterJavaUdfRequest();

    // submit the javaUdfRequest to the registerJavaUdf endpoint
    auto future = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/registerUdf"},
                                 cpr::Header{{"Content-Type", "text/plain"}},
                                 cpr::Body{javaUdfRequest.SerializeAsString()});
    future.wait();
    auto response = future.get();
    // then the HTTP response is OK
    ASSERT_EQ(response.status_code, Status::CODE_200.code);
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Origin"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Methods"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Headers"));
    ASSERT_EQ(response.text, "Registered UDF");
    // check to see if a udf has been added to the udf catalog
    ASSERT_FALSE(udfCatalog->listUDFs().empty());
    // get udf catalog entry
    auto descriptorFromCoordinator = UDF::UDFDescriptor::as<UDF::JavaUDFDescriptor>(udfCatalog->getUDFDescriptor("my_udf"));
    //extract the udf descriptor from the udf post request
    JavaUdfDescriptorMessage descriptorFromPostRequest;
    javaUdfRequest.udfdescriptor().descriptormessage().UnpackTo(&descriptorFromPostRequest);
    //and compare its fields to the java udf catalog entry now found in the coordinator
    ASSERT_EQ(descriptorFromCoordinator->getClassName(), descriptorFromPostRequest.udf_class_name());
    ASSERT_EQ(descriptorFromCoordinator->getMethodName(), descriptorFromPostRequest.udf_method_name());
    verifySerializedInstance(descriptorFromCoordinator->getSerializedInstance(), descriptorFromPostRequest.serialized_instance());
    verifyByteCodeList(descriptorFromCoordinator->getByteCodeList(), descriptorFromPostRequest.classes());
    stopCoordinator();
}

//Test if registerUdf endpoint correctly adds Python udf
TEST_F(UDFCatalogControllerTest, testIfRegisterUdfEndpointCorrectlyAddsPythonUDF) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    auto udfCatalog = coordinator->getUDFCatalog();
    //check to see if no udfs registered
    ASSERT_TRUE(udfCatalog->listUDFs().empty());
    // create a pythonUDFRequest
    auto pythonUDFRequest = ProtobufMessageFactory::createDefaultRegisterPythonUdfRequest();

    // submit the pythonUDFRequest to the registerUdf endpoint
    auto future = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/registerUdf"},
                                 cpr::Header{{"Content-Type", "text/plain"}},
                                 cpr::Body{pythonUDFRequest.SerializeAsString()});
    future.wait();
    auto response = future.get();
    // then the HTTP response is OK
    ASSERT_EQ(response.status_code, Status::CODE_200.code);
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Origin"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Methods"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Headers"));
    ASSERT_EQ(response.text, "Registered UDF");
    // check to see if a udf has been added to the udf catalog
    ASSERT_FALSE(udfCatalog->listUDFs().empty());
    // get udf catalog entry
    auto descriptorFromCoordinator = UDF::UDFDescriptor::as<UDF::PythonUDFDescriptor>(udfCatalog->getUDFDescriptor("my_udf"));
    //extract the udf descriptor from the udf post request
    PythonUDFDescriptorMessage descriptorFromPostRequest;
    pythonUDFRequest.udfdescriptor().descriptormessage().UnpackTo(&descriptorFromPostRequest);
    //and compare its fields to the java udf catalog entry now found in the coordinator
    ASSERT_EQ(descriptorFromCoordinator->getFunctionString(), descriptorFromPostRequest.function_string());
    ASSERT_EQ(descriptorFromCoordinator->getMethodName(), descriptorFromPostRequest.udf_method_name());
    stopCoordinator();
}

//Test if deleting an Udf catalog entry works as expected
TEST_F(UDFCatalogControllerTest, testRemoveUdfEndpoint) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    auto udfCatalog = coordinator->getUDFCatalog();

    // create a Java Udf descriptor and specify an udf name
    std::string udfName = "my_udf";
    auto javaUdfDescriptor = Catalogs::UDF::JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor();

    //register udf with coordinator
    udfCatalog->registerUDF(udfName, javaUdfDescriptor);

    // given the UDF catalog contains a Java UDF
    // when a REST message is passed to the controller to remove the UDF
    auto future = cpr::DeleteAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/removeUdf"},
                                   cpr::Parameters{{"udfName", udfName}});
    future.wait();
    auto response = future.get();
    // then the response is OK
    ASSERT_EQ(response.status_code, Status::CODE_200.code);
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Origin"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Methods"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Headers"));
    // and the UDF no longer exists in the catalog
    ASSERT_EQ(UDF::UDFDescriptor::as<UDF::JavaUDFDescriptor>(udfCatalog->getUDFDescriptor("my_udf")), nullptr);
    // and the response shows that the UDF was removed
    nlohmann::json json;
    json["removed"] = true;
    verifyResponseResult(response, json);
    stopCoordinator();
}

//Test if removeUdf endpoint handles non-existent UDF correctly
TEST_F(UDFCatalogControllerTest, testRemoveUdfEndpointIfUdfDoesNotExist) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    // when a REST message is passed to the controller to remove a UDF that does not exist
    auto future = cpr::DeleteAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/removeUdf"},
                                   cpr::Parameters{{"udfName", "my_udf"}});
    future.wait();
    auto response = future.get();
    // then the response is NOT FOUND
    ASSERT_EQ(response.status_code, Status::CODE_200.code);
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Origin"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Methods"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Headers"));
    // and the response shows that the UDF was not removed
    nlohmann::json json;
    json["removed"] = false;
    verifyResponseResult(response, json);
    stopCoordinator();
}

//Test if removeUdf endpoint handles missing query parameter correctly
TEST_F(UDFCatalogControllerTest, testRemoveUdfEndpointHandlesMissingQueryParametersCorrectly) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    // when a REST message is passed to the controller that is missing the udfName parameter
    auto future = cpr::DeleteAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/removeUdf"});
    future.wait();
    auto response = future.get();
    // then the response is BadRequest
    ASSERT_EQ(response.status_code, Status::CODE_400.code);
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Origin"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Methods"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Headers"));
    stopCoordinator();
}

//Test if removeUdf endpoint handles extra query parameters correctly
TEST_F(UDFCatalogControllerTest, testIfRemoveUdfEndpointHandlesExtraQueryParametersCorrectly) {
    // Oatpp framework ignores all query parameters that aren't defined in the endpoint, effectively ignoring them.
    // Superfluous query parameters have no effect on the behavior of an endpoint
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    // when a REST message is passed to the controller that is contains parameters other than the udfName parameter
    auto future = cpr::DeleteAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/removeUdf"},
                                   cpr::Parameters{{"udfName", "my_udf"}, {"meaning_of_life", "42"}});
    future.wait();
    auto response = future.get();
    // then the response is NOT FOUND
    ASSERT_EQ(response.status_code, Status::CODE_200.code);
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Origin"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Methods"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Headers"));
    nlohmann::json json;
    json["removed"] = false;
    verifyResponseResult(response, json);
    stopCoordinator();
}

//Test if listUdfs endpoint handles missing query parameters correctly
TEST_F(UDFCatalogControllerTest, testIfListUdfsEndpointHandlesMissingQueryParameters) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    // when a REST message is passed to the controller that is missing the udfName parameter
    auto future = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/getUdfDescriptor"});
    future.wait();
    auto response = future.get();
    // then the response is BadRequest
    ASSERT_EQ(response.status_code, Status::CODE_400.code);
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Origin"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Methods"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Headers"));
    stopCoordinator();
}

//Test if listUdfs endpoint behaves as expected when all else is correct
TEST_F(UDFCatalogControllerTest, testIfListUdfsEndpointReturnsListAsExpected) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    auto udfCatalog = coordinator->getUDFCatalog();

    // create a Java Udf descriptor and specify an udf name
    std::string udfName = "my_udf";
    auto javaUdfDescriptor = Catalogs::UDF::JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor();
    //register udf with coordinator
    udfCatalog->registerUDF(udfName, javaUdfDescriptor);

    // when a REST message is passed to the controller to get a list of the UDFs
    auto future = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/listUdfs"});
    future.wait();
    auto response = future.get();
    // then the response is OK
    ASSERT_EQ(response.status_code, Status::CODE_200.code);
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Origin"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Methods"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Headers"));
    // and the response message contains a list of UDFs
    nlohmann::json json;
    std::vector<std::string> udfs = udfCatalog->listUDFs();
    json["udfs"] = udfs;
    verifyResponseResult(response, json);
    stopCoordinator();
}

//Test if listUdfs endpoint behaves correctly when no UDFs are registered
TEST_F(UDFCatalogControllerTest, testIfListUdfsReturnsEmptyUdfList) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    auto udfCatalog = coordinator->getUDFCatalog();
    // when a REST message is passed to the controller to get a list of the UDFs
    auto future = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/listUdfs"});
    // then the response is OK
    future.wait();
    auto response = future.get();
    ASSERT_EQ(response.status_code, Status::CODE_200.code);
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Origin"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Methods"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Headers"));
    // and the response message contains an empty list of UDFs
    nlohmann::json json;
    std::vector<std::string> udfs = udfCatalog->listUDFs();

    json["udfs"] = udfs;
    verifyResponseResult(response, json);
    stopCoordinator();
}
}// namespace NES