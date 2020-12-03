
#include <QueryCompiler/CCodeGenerator/Definitions/NamespaceDefinition.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/NamespaceDeclaration.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/Declaration.hpp>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
namespace NES{

NamespaceDefinition::NamespaceDefinition(std::string name):name(name) {}

NamespaceDefinitionPtr NamespaceDefinition::create(std::string name) {
    return std::make_shared<NamespaceDefinition>(name);
}

void NamespaceDefinition::addDeclaration(DeclarationPtr declaration) {
    this->declarations.emplace_back(declaration);
}

DeclarationPtr NamespaceDefinition::getDeclaration() {
    std::stringstream namespaceCode;
    namespaceCode << "namespace " << name;
    namespaceCode << "{";
    for(auto declaration:declarations){
        namespaceCode << declaration->getCode();
    }
    namespaceCode << "}";

    return NamespaceDeclaration::create(namespaceCode.str());
}


}