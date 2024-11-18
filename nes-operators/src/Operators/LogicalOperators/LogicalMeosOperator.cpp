// LogicalMeosOperator.cpp

#include "Operators/LogicalOperators/LogicalOperator.hpp"
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/LogicalMeosOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>

namespace NES {

namespace MeosOperator {

LogicalMeosOperator::LogicalMeosOperator(const ExpressionNodePtr left,
                                         const ExpressionNodePtr middle,
                                         const ExpressionNodePtr right,
                                         const std::string function,
                                         OperatorId id)
    : Operator(id),                                            // Initialize Operator with OperatorId
      LogicalOperator(id),                                     // Initialize LogicalOperator with OperatorId
      LogicalQuaternaryOperator(left, middle, right, function),// Initialize LogicalQuaternaryOperator with necessary arguments
      left(left), middle(middle), right(right), function(function) {
    NES_INFO("LogicalMeosOperator (ID: {}) constructed with function '{}'.", id, function);

    // Initialize schemas
    leftInputSchema = Schema::create();
    middleInputSchema = Schema::create();
    rightInputSchema = Schema::create();
    outputSchema = Schema::create();

    // Define input schemas
    leftInputSchema->addField("ais$timestamp", BasicType::UINT64);
    NES_INFO("Current fields in leftInputSchema: {}", leftInputSchema->toString());
    middleInputSchema->addField("ais$latitude", BasicType::FLOAT64);
    NES_INFO("Current fields in middleInputSchema: {}", middleInputSchema->toString());
    rightInputSchema->addField("ais$longitude", BasicType::FLOAT32);
    NES_INFO("Current fields in rightInputSchema: {}", rightInputSchema->toString());

    // Optionally, initialize outputSchema here if necessary
    // Alternatively, rely on inferSchema() to set it
}

// Destructor
LogicalMeosOperator::~LogicalMeosOperator() = default;

OperatorPtr LogicalMeosOperator::copy() {
    NES_INFO("LogicalMeosOperator::copy() called for Operator ID: {}", this->getId());

    // Create a new LogicalMeosOperator instance
    auto copy = LogicalOperatorFactory::createMeosOperator(left, middle, right, function, id);

    // Deep copy input schemas
    if (leftInputSchema) {
        copy->setLeftInputSchema(std::make_shared<Schema>(*leftInputSchema));
        NES_INFO("Copied leftInputSchema: {}", copy->getLeftInputSchema()->toString());
    } else {
        NES_WARNING("LogicalMeosOperator::copy(): leftInputSchema is null.");
    }

    if (middleInputSchema) {
        copy->setMiddleInputSchema(std::make_shared<Schema>(*middleInputSchema));
        NES_INFO("Copied middleInputSchema: {}", copy->getMiddleInputSchema()->toString());
    } else {
        NES_WARNING("LogicalMeosOperator::copy(): middleInputSchema is null.");
    }

    if (rightInputSchema) {
        copy->setRightInputSchema(std::make_shared<Schema>(*rightInputSchema));
        NES_INFO("Copied rightInputSchema: {}", copy->getRightInputSchema()->toString());
    } else {
        NES_WARNING("LogicalMeosOperator::copy(): rightInputSchema is null.");
    }

    // Deep copy outputSchema
    if (outputSchema) {
        copy->setOutputSchema(std::make_shared<Schema>(*outputSchema));
        NES_INFO("Copied outputSchema: {}", copy->getOutputSchema()->toString());
    } else {
        NES_WARNING("LogicalMeosOperator::copy(): outputSchema is null.");
    }

    // Copy signatures and state
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setZ3Signature(z3Signature);
    copy->setOperatorState(operatorState);

    // Copy properties
    for (const auto& [key, value] : properties) {
        copy->addProperty(key, value);
    }

    // Infer schema on the copied operator to ensure schemas are correctly set
    bool schemaInferred = copy->inferSchema();
    if (schemaInferred) {
        NES_INFO("LogicalMeosOperator::copy(): Schema inferred successfully for copied Operator ID: {}", copy->getId());
    } else {
        NES_ERROR("LogicalMeosOperator::copy(): Schema inference failed for copied Operator ID: {}", copy->getId());
    }

    return copy;
}

bool LogicalMeosOperator::inferSchema() {
    NES_INFO("LogicalMeosOperator::inferSchema() called for Operator ID: {}", this->getId());

    if (!leftInputSchema || !middleInputSchema || !rightInputSchema) {
        NES_ERROR("LogicalMeosOperator::inferSchema(): One or more input schemas are null.");
        return false;
    }

    NES_INFO("Final fields in leftInputSchema: {}", leftInputSchema->toString());
    NES_INFO("Final fields in middleInputSchema: {}", middleInputSchema->toString());
    NES_INFO("Final fields in rightInputSchema: {}", rightInputSchema->toString());

    // Clear existing outputSchema to avoid duplicate fields if inferSchema() is called multiple times
    outputSchema->clear();

    // Set the output schema based on input schemas and operator logic
    outputSchema->addField("ais$timestamp", BasicType::UINT64);
    outputSchema->addField("ais$latitude", BasicType::FLOAT64);
    outputSchema->addField("ais$longitude", BasicType::FLOAT32);

    NES_INFO("Output schema set to: {}", outputSchema->toString());

    return true;
}

std::vector<OperatorPtr> LogicalMeosOperator::getOperatorsBySchema(const SchemaPtr& schema) const {
    std::vector<OperatorPtr> operators;
    for (const auto& child : getChildren()) {
        auto childOperator = child->as<Operator>();
        if (!childOperator) {
            NES_WARNING("LogicalMeosOperator::getOperatorsBySchema(): Child operator is not a valid Operator.");
            continue;
        }

        if (childOperator->getOutputSchema() && childOperator->getOutputSchema()->equals(schema, false)) {
            operators.emplace_back(childOperator);
        }
    }
    NES_INFO("LogicalMeosOperator::getOperatorsBySchema(): Found {} operators matching the given schema.", operators.size());
    return operators;
}

std::vector<OperatorPtr> LogicalMeosOperator::getLeftOperators() const {
    NES_INFO("LogicalMeosOperator::getLeftOperators() called.");
    return getOperatorsBySchema(getLeftInputSchema());
}

std::vector<OperatorPtr> LogicalMeosOperator::getRightOperators() const {
    NES_INFO("LogicalMeosOperator::getRightOperators() called.");
    return getOperatorsBySchema(getRightInputSchema());
}

void LogicalMeosOperator::inferInputOrigins() {
    NES_INFO("LogicalMeosOperator::inferInputOrigins() called.");

    // Collect left input origins
    std::vector<OriginId> leftInputOriginIds;
    for (const auto& child : this->getLeftOperators()) {
        auto logicalChild = child->as<LogicalOperator>();
        if (!logicalChild) {
            NES_WARNING("LogicalMeosOperator::inferInputOrigins(): Child operator is not a LogicalOperator.");
            continue;
        }

        logicalChild->inferInputOrigins();
        auto childOriginIds = logicalChild->getOutputOriginIds();
        leftInputOriginIds.insert(leftInputOriginIds.end(), childOriginIds.begin(), childOriginIds.end());
    }
    this->leftInputOriginIds = leftInputOriginIds;
    NES_INFO("LogicalMeosOperator::inferInputOrigins(): Collected {} left input origins.", leftInputOriginIds.size());

    // Collect right input origins
    std::vector<OriginId> rightInputOriginIds;
    for (const auto& child : this->getRightOperators()) {
        auto logicalChild = child->as<LogicalOperator>();
        if (!logicalChild) {
            NES_WARNING("LogicalMeosOperator::inferInputOrigins(): Child operator is not a LogicalOperator.");
            continue;
        }

        logicalChild->inferInputOrigins();
        auto childOriginIds = logicalChild->getOutputOriginIds();
        rightInputOriginIds.insert(rightInputOriginIds.end(), childOriginIds.begin(), childOriginIds.end());
    }
    this->rightInputOriginIds = rightInputOriginIds;
    NES_INFO("LogicalMeosOperator::inferInputOrigins(): Collected {} right input origins.", rightInputOriginIds.size());
}

void LogicalMeosOperator::inferStringSignature() {
    std::string signature = function + "(";
    for (size_t i = 0; i < children.size(); ++i) {
        auto childOperator = children[i]->as<Operator>();
        if (childOperator && childOperator->getOutputSchema()) {
            signature += childOperator->getOutputSchema()->toString();
        } else {
            signature += "UnknownSchema";
        }

        if (i < children.size() - 1) {
            signature += ", ";
        }
    }
    signature += ")";

    // Store the signature
    this->stringSignature = signature;

    // Logging the inferred signature
    NES_INFO("LogicalMeosOperator::inferStringSignature(): Inferred String Signature: {}", signature);
}

ExpressionNodePtr LogicalMeosOperator::getLeft() const { return left; }

ExpressionNodePtr LogicalMeosOperator::getMiddle() const { return middle; }

ExpressionNodePtr LogicalMeosOperator::getRight() const { return right; }

std::string LogicalMeosOperator::getFunction() const { return function; }

}// namespace MeosOperator

}// namespace NES
