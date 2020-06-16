
#include <QueryCompiler/DataTypes/BasicGeneratableType.hpp>
namespace NES {

BasicGeneratableType::BasicGeneratableType(DataTypePtr type) : GeneratableDataType(), type(type){}

const CodeExpressionPtr BasicGeneratableType::getTypeDefinitionCode() const {}
const CodeExpressionPtr BasicGeneratableType::getCode() const {}
CodeExpressionPtr BasicGeneratableType::generateCode() {
    return GeneratableDataType::generateCode();
}


}