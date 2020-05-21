

#include <API/Schema.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <SerializableOperator.pb.h>
#include <google/protobuf/util/json_util.h>
namespace NES {
class SerializeOperators {
    void serialize(QueryPlanPtr plan) {
        auto ope = SerializableOperator();
        auto rootOperator = plan->getRootOperator();
        serializeOperator(rootOperator, &ope);

        std::string json_string;
        google::protobuf::util::MessageToJsonString(ope, &json_string);
    }

    void serializeNode(NodePtr node, SerializableOperator* serializedOperator) {
        auto operatorNode = node->as<OperatorNode>();
        if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
            auto sourceDetails = SerializableOperator_SourceDetails();
            serializedOperator->mutable_details()->PackFrom(sourceDetails);
        } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
            auto sinkDetails = SerializableOperator_SinkDetails();
            serializedOperator->mutable_details()->PackFrom(sinkDetails);
        } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
            auto filterDetails = SerializableOperator_FilterDetails();
            serializedOperator->mutable_details()->PackFrom(filterDetails);
        } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
            auto mapDetails = SerializableOperator_MapDetails();
            serializedOperator->mutable_details()->PackFrom(mapDetails);
        }
        // serialize input schema
        serializeSchema(operatorNode->getInputSchema(), serializedOperator->mutable_inputschema());
        // serialize output schema
        serializeSchema(operatorNode->getOutputSchema(), serializedOperator->mutable_outputschema());
    }

    void serializeSchema(SchemaPtr schema, SerializableSchema* serializedSchema) {
        // serialize all field in schema
        for (const auto& field : schema->fields) {
            auto serializedField = serializedSchema->add_fields();
            serializedField->set_name(field->name);
            serializeDataType(field->getDataType(), serializedField->mutable_type());
        }
    }

    void serializeDataType(DataTypePtr dataType, SerializableDataType* serializedDataType) {
        if (dataType->isUndefined()) {
            serializedDataType->set_type(SerializableDataType_Type_UNDEFINED);
        } else if (dataType->isEqual(createDataType(BasicType::CHAR))) {
            serializedDataType->set_type(SerializableDataType_Type_UNDEFINED);
        } else if (dataType->isEqual(createDataType(BasicType::INT8))) {
            serializedDataType->set_type(SerializableDataType_Type_INT8);
        } else if (dataType->isEqual(createDataType(BasicType::INT16))) {
            serializedDataType->set_type(SerializableDataType_Type_INT16);
        } else if (dataType->isEqual(createDataType(BasicType::INT32))) {
            serializedDataType->set_type(SerializableDataType_Type_INT32);
        } else if (dataType->isEqual(createDataType(BasicType::INT64))) {
            serializedDataType->set_type(SerializableDataType_Type_INT64);
        } else if (dataType->isEqual(createDataType(BasicType::UINT8))) {
            serializedDataType->set_type(SerializableDataType_Type_UINT8);
        } else if (dataType->isEqual(createDataType(BasicType::UINT16))) {
            serializedDataType->set_type(SerializableDataType_Type_UINT16);
        } else if (dataType->isEqual(createDataType(BasicType::UINT32))) {
            serializedDataType->set_type(SerializableDataType_Type_UINT32);
        } else if (dataType->isEqual(createDataType(BasicType::UINT64))) {
            serializedDataType->set_type(SerializableDataType_Type_UINT64);
        } else if (dataType->isEqual(createDataType(BasicType::FLOAT32))) {
            serializedDataType->set_type(SerializableDataType_Type_FLOAT32);
        } else if (dataType->isEqual(createDataType(BasicType::FLOAT64))) {
            serializedDataType->set_type(SerializableDataType_Type_FLOAT64);
        } else {
            NES_THROW_RUNTIME_ERROR("Serialize: serialization is not possible.");
        }
    }

    void serializeOperator(NodePtr parent, SerializableOperator* serializedParent) {

        serializeNode(parent, serializedParent);

        for (auto child : parent->getChildren()) {
            auto serializedChild = serializedParent->add_children();
            serializeOperator(child, serializedChild);
        }
    }
};// namespace NES
}// namespace NES
