#ifndef NES_INCLUDE_GRPC_SERIAIZATION_SCHEMASERIALIZATIONUTIL_HPP_
#define NES_INCLUDE_GRPC_SERIAIZATION_SCHEMASERIALIZATIONUTIL_HPP_

#include <memory>

namespace NES {

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class SerializableSchema;

/**
 * @brief The SchemaSerializationUtil offers functionality to serialize and de-serialize schemas to the
 * corresponding protobuffer object.
 */
class SchemaSerializationUtil {
  public:
    /**
     * @brief Serializes a schema and all its fields to a SerializableSchema object.
     * @param schema SchemaPtr.
     * @param serializedSchema The corresponding protobuff object, which is used to capture the state of the object.
     * @return the modified serializedSchema
     */
    static SerializableSchema* serializeSchema(SchemaPtr schema, SerializableSchema* serializedSchema);

    /**
    * @brief De-serializes the SerializableSchema and all its fields to a SchemaPtr
    * @param serializedSchema the serialized schema.
    * @return SchemaPtr
    */
    static SchemaPtr deserializeSchema(SerializableSchema* serializedSchema);
};
}// namespace NES

#endif//NES_INCLUDE_GRPC_SERIAIZATION_SCHEMASERIALIZATIONUTIL_HPP_
