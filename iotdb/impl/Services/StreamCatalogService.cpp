#include "Services/StreamCatalogService.hpp"

namespace NES{

bool StreamCatalogService::addNewLogicalStream(std::string &streamName, std::string &streamSchema) {
    SchemaPtr schema = UtilityFunctions::createSchemaFromCode(streamSchema);
    return StreamCatalog::instance().addLogicalStream(streamName, schema);
}

bool StreamCatalogService::removeLogicalStream(std::string &streamName) {
    return StreamCatalog::instance().removeLogicalStream(streamName);
}

std::vector<string> StreamCatalogService::getAllLogicalStreamNames() {
    return StreamCatalog::instance().
}

}
