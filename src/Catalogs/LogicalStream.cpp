
#include <API/ParameterTypes.hpp>
#include <API/Schema.hpp>
#include <API/UserAPIExpression.hpp>
#include <Catalogs/LogicalStream.hpp>
#include <Util/Logger.hpp>

namespace NES {

LogicalStream::LogicalStream(std::string name, NES::SchemaPtr schemaPtr)
    : name(name) {
    schema = schemaPtr->copy();
}

SchemaPtr LogicalStream::getSchema() {
    return schema;
}

std::string LogicalStream::getName() {
    return name;
}

}// namespace NES
