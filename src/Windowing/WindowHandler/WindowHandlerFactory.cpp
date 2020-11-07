/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Windowing/WindowHandler/JoinForwardRefs.hpp>
#include <Windowing/WindowHandler/WindowHandlerFactory.hpp>
#include <Windowing/WindowHandler/WindowHandlerFactoryDetails.hpp>

namespace NES::Windowing {

AbstractWindowHandlerPtr WindowHandlerFactory::createAggregationWindowHandler(LogicalWindowDefinitionPtr windowDefinition) {
    if (windowDefinition->isKeyed()) {
        auto logicalKeyType = windowDefinition->getOnKey()->getStamp();
        auto physicalKeyType = DefaultPhysicalTypeFactory().getPhysicalType(logicalKeyType);
        if (physicalKeyType->isBasicType()) {
            auto basicKeyType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalKeyType);
            switch (basicKeyType->getNativeType()) {
                case BasicPhysicalType::UINT_8: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<uint8_t>(windowDefinition);
                case BasicPhysicalType::UINT_16: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<uint16_t>(windowDefinition);
                case BasicPhysicalType::UINT_32: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<uint32_t>(windowDefinition);
                case BasicPhysicalType::UINT_64: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<uint64_t>(windowDefinition);
                case BasicPhysicalType::INT_8: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<int8_t>(windowDefinition);
                case BasicPhysicalType::INT_16: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<int16_t>(windowDefinition);
                case BasicPhysicalType::INT_32: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<int32_t>(windowDefinition);
                case BasicPhysicalType::INT_64: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<int64_t>(windowDefinition);
                case BasicPhysicalType::FLOAT: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<float>(windowDefinition);
                case BasicPhysicalType::DOUBLE: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<double>(windowDefinition);
                case BasicPhysicalType::CHAR: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<char>(windowDefinition);
                case BasicPhysicalType::BOOLEAN: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<bool>(windowDefinition);
            }
        } else {
            NES_THROW_RUNTIME_ERROR("WindowHandlerFactory: currently we dont support non basic key types");
        }
    }
    NES_THROW_RUNTIME_ERROR("WindowHandlerFactory: currently we dont support non keyed aggregations");
}

AbstractWindowHandlerPtr WindowHandlerFactory::createJoinWindowHandler(Join::LogicalJoinDefinitionPtr joinDefinition) {
    auto logicalKeyType = joinDefinition->getJoinKey()->getStamp();
    auto physicalKeyType = DefaultPhysicalTypeFactory().getPhysicalType(logicalKeyType);
    if (physicalKeyType->isBasicType()) {
        auto basicKeyType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalKeyType);
        switch (basicKeyType->getNativeType()) {
            case BasicPhysicalType::UINT_8: return WindowHandlerFactoryDetails::createJoinHandler<uint8_t>(joinDefinition);
            case BasicPhysicalType::UINT_16: return WindowHandlerFactoryDetails::createJoinHandler<uint16_t>(joinDefinition);
            case BasicPhysicalType::UINT_32: return WindowHandlerFactoryDetails::createJoinHandler<uint32_t>(joinDefinition);
            case BasicPhysicalType::UINT_64: return WindowHandlerFactoryDetails::createJoinHandler<uint64_t>(joinDefinition);
            case BasicPhysicalType::INT_8: return WindowHandlerFactoryDetails::createJoinHandler<int8_t>(joinDefinition);
            case BasicPhysicalType::INT_16: return WindowHandlerFactoryDetails::createJoinHandler<int16_t>(joinDefinition);
            case BasicPhysicalType::INT_32: return WindowHandlerFactoryDetails::createJoinHandler<int32_t>(joinDefinition);
            case BasicPhysicalType::INT_64: return WindowHandlerFactoryDetails::createJoinHandler<int64_t>(joinDefinition);
            case BasicPhysicalType::FLOAT: return WindowHandlerFactoryDetails::createJoinHandler<float>(joinDefinition);
            case BasicPhysicalType::DOUBLE: return WindowHandlerFactoryDetails::createJoinHandler<double>(joinDefinition);
            case BasicPhysicalType::CHAR: return WindowHandlerFactoryDetails::createJoinHandler<char>(joinDefinition);
            case BasicPhysicalType::BOOLEAN: return WindowHandlerFactoryDetails::createJoinHandler<bool>(joinDefinition);
        }
    }
    NES_THROW_RUNTIME_ERROR("WindowHandlerFactory: currently we dont support non keyed aggregations");
}

}// namespace NES::Windowing