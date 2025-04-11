#include <iostream>

#include "oatpp/parser/json/mapping/ObjectMapper.hpp"

#include "oatpp/web/server/HttpConnectionHandler.hpp"

#include "oatpp/network/Server.hpp"
#include "oatpp/network/tcp/server/ConnectionProvider.hpp"

#include "controller/QueryController.hpp"
#include "controller/SourceCatalogController.hpp"

#include <SourceCatalogs/SourceCatalog.hpp>
#include <NebuLi.hpp>
#include <ErrorHandling.hpp>
#include <yaml-cpp/yaml.h>

void run(int port, std::shared_ptr<NES::Catalogs::Source::SourceCatalog> sourceCatalog) {

    // // Create a JSON ObjectMapper manually
    auto objectMapper = oatpp::parser::json::mapping::ObjectMapper::createShared();

    // Create and configure a Router
    auto router = oatpp::web::server::HttpRouter::createShared();
    auto queryController = std::make_shared<QueryController>(objectMapper);
    router->addController(queryController);
    auto sourceCatalogController = std::make_shared<SourceCatalogController>(objectMapper, sourceCatalog);
    router->addController(sourceCatalogController);

    /* Create HTTP connection handler with router */
    auto connectionHandler = oatpp::web::server::HttpConnectionHandler::createShared(router);

    /* Create TCP connection provider */
    auto connectionProvider =
        oatpp::network::tcp::server::ConnectionProvider::createShared({"0.0.0.0", static_cast<v_uint16>(port), oatpp::network::Address::IP_4});

    /* Create a server, which takes provided TCP connections and passes them to HTTP connection handler. */
    oatpp::network::Server server(connectionProvider, connectionHandler);

    /* Print info about server port */
    OATPP_LOGI("MyApp", "Server running on port %s", connectionProvider->getProperty("port").getData());

    server.run();
}


int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " PORT CONFIG_FILE\n";
        return 1;
    }
    int port = std::stoi(argv[1]);
    const std::filesystem::path& filePath = argv[2];

     // create and fill the source catalog based on the provided YAML file
     auto sourceCatalog = std::make_shared<NES::Catalogs::Source::SourceCatalog>();

     std::ifstream file(filePath);
     if (!file)
     {
        throw NES::QueryDescriptionNotReadable(std::strerror(errno));
     }
     auto config = NES::CLI::loadConfig(file);
     addSources(sourceCatalog, config);

     for (auto const& [key, val] : sourceCatalog->getAllLogicalSourceAsString())
     {
        std::cout << key        // string (key)
                  << ':'
                  << val        // string's value
                  << std::endl;
     }


    /* Init oatpp Environment */
    oatpp::base::Environment::init();

    /* Run App */
    run(port, sourceCatalog);

    /* Destroy oatpp Environment */
    oatpp::base::Environment::destroy();

    return 0;

}
