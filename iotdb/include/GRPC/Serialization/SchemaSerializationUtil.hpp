#ifndef NES_INCLUDE_GRPC_SERIAIZATION_SCHEMASERIALIZATIONUTIL_HPP_
#define NES_INCLUDE_GRPC_SERIAIZATION_SCHEMASERIALIZATIONUTIL_HPP_

#include <memory>

namespace NES {

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class SerializableSchema;

class SchemaSerializationUtil {
  public:
    static SerializableSchema* serializeSchema(SchemaPtr dataType, SerializableSchema* serializedSchema);
    static SchemaPtr deserializeSchema(SerializableSchema* serializedSchema);
};
}// namespace NES

#endif//NES_INCLUDE_GRPC_SERIAIZATION_SCHEMASERIALIZATIONUTIL_HPP_
