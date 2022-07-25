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

#ifndef NES_NES_CORE_INCLUDE_REST_APPCOMPONENT_HPP_
#define NES_NES_CORE_INCLUDE_REST_APPCOMPONENT_HPP_

#include <oatpp/parser/json/mapping/ObjectMapper.hpp>
#include <oatpp/web/server/HttpConnectionHandler.hpp>
#include <oatpp/network/tcp/server/ConnectionProvider.hpp>
#include <oatpp/core/macro/component.hpp>

namespace NES {
/**
 *  Class which creates and holds Application components and registers components in oatpp::base::Environment
 *  Order of components initialization is from top to bottom
 */
class AppComponent {
  public:
    AppComponent() = delete;

    AppComponent(std::string host, uint16_t port) : host(host), port(port) { NES_INFO("Creating AppComponent"); };

    /**
     *  Create ConnectionProvider component which listens on the port
     */
    OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::network::ServerConnectionProvider>, serverConnectionProvider)
    ([&] {
        return oatpp::network::tcp::server::ConnectionProvider::createShared({host, port, oatpp::network::Address::IP_4});
    }());
    /**
   *  Create Router component
   */
    OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::web::server::HttpRouter>, httpRouter)
    ([] {
        return oatpp::web::server::HttpRouter::createShared();
    }());

    /**
   *  Create ConnectionHandler component which uses Router component to route requests
   */
    OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::network::ConnectionHandler>, serverConnectionHandler)
    ([] {
        OATPP_COMPONENT(std::shared_ptr<oatpp::web::server::HttpRouter>, router);// get Router component
        return oatpp::web::server::HttpConnectionHandler::createShared(router);
    }());

    /**
   *  Create ObjectMapper component to serialize/deserialize DTOs in Contoller's API
   */
    OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::data::mapping::ObjectMapper>, apiObjectMapper)
    ([] {
        return oatpp::parser::json::mapping::ObjectMapper::createShared();
    }());

  private:
    std::string host;
    uint16_t port;
};
}//namespace NES
#endif//NES_NES_CORE_INCLUDE_REST_APPCOMPONENT_HPP_

//#endif /* AppComponent_hpp */