/*
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

#ifndef NES_YAMLMODEL_H
#define NES_YAMLMODEL_H

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/Identifiers.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <yaml-cpp/yaml.h>

#define UNIKERNEL_MODEL_YAML_DECODE(field) rhs.field = node[#field].as<decltype(rhs.field)>()
#define UNIKERNEL_MODEL_YAML_ENCODE(field) node[#field] = rhs.field

struct SchemaField {
    std::string name;
    std::string type;
};

namespace YAML {
template<>
struct convert<SchemaField> {
    static Node encode(const SchemaField& rhs) {
        Node node;
        UNIKERNEL_MODEL_YAML_ENCODE(name);
        UNIKERNEL_MODEL_YAML_ENCODE(type);
        return node;
    };
    static Node decode(const Node& node, SchemaField& rhs) {
        UNIKERNEL_MODEL_YAML_DECODE(name);
        UNIKERNEL_MODEL_YAML_DECODE(type);
        return node;
    };
};
}// namespace YAML

struct SchemaConfiguration {
    std::vector<SchemaField> fields;
};

namespace YAML {
template<>
struct convert<SchemaConfiguration> {
    static Node encode(const SchemaConfiguration& rhs) {
        Node node;
        UNIKERNEL_MODEL_YAML_ENCODE(fields);
        return node;
    };

    static Node decode(const Node& node, SchemaConfiguration& rhs) {
        UNIKERNEL_MODEL_YAML_DECODE(fields);
        return node;
    };
};
}// namespace YAML

static NES::BasicType toBasicType(NES::DataTypePtr dt) {
    if (dt->isNumeric()) {
        if (auto integer = NES::DataType::as<NES::Integer>(dt); integer) {
            if (integer->lowerBound == 0) {
                switch (integer->getBits()) {
                    case 8: return NES::BasicType::UINT8;
                    case 16: return NES::BasicType::UINT16;
                    case 32: return NES::BasicType::UINT32;
                    case 64: return NES::BasicType::UINT64;
                }
            } else {
                switch (integer->getBits()) {
                    case 8: return NES::BasicType::INT8;
                    case 16: return NES::BasicType::INT16;
                    case 32: return NES::BasicType::INT32;
                    case 64: return NES::BasicType::INT64;
                }
            }
        }
        if (auto floatingPoint = NES::DataType::as<NES::Float>(dt); floatingPoint) {
            switch (floatingPoint->getBits()) {
                case 32: return NES::BasicType::FLOAT32;
                case 64: return NES::BasicType::FLOAT64;
            }
        }
    }
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

struct EndpointConfiguration {
    size_t subQueryID;
    SchemaConfiguration schema;
    std::string ip;
    uint32_t port;
    size_t nodeId;
    std::optional<size_t> operatorId;

    void setSchema(const NES::SchemaPtr& newSchema) {
        this->schema.fields.clear();
        for (const auto& field : newSchema->fields) {
            this->schema.fields.emplace_back(field->getName(),
                                             std::string(magic_enum::enum_name(toBasicType(field->getDataType()))));
        }
    }
};

namespace YAML {
template<>
struct convert<EndpointConfiguration> {
    static Node encode(const EndpointConfiguration& rhs) {
        Node node;
        UNIKERNEL_MODEL_YAML_ENCODE(ip);
        UNIKERNEL_MODEL_YAML_ENCODE(subQueryID);
        UNIKERNEL_MODEL_YAML_ENCODE(port);
        if (rhs.operatorId.has_value()) {
            node["operatorId"] = rhs.operatorId.value();
        }

        UNIKERNEL_MODEL_YAML_ENCODE(nodeId);
        UNIKERNEL_MODEL_YAML_ENCODE(schema);

        return node;
    };

    static Node decode(const Node& node, EndpointConfiguration& rhs) {
        UNIKERNEL_MODEL_YAML_DECODE(ip);
        UNIKERNEL_MODEL_YAML_DECODE(subQueryID);
        UNIKERNEL_MODEL_YAML_DECODE(port);
        if (!node["operatorId"].IsNull()) {
            rhs.operatorId.emplace(node["operatorId"].as<NES::OperatorId>());
        }

        UNIKERNEL_MODEL_YAML_DECODE(nodeId);
        UNIKERNEL_MODEL_YAML_DECODE(schema);
        return node;
    };
};
}// namespace YAML
struct WorkerLinkConfiguration {
    std::string ip;
    uint32_t port;
    NES::NodeId nodeId;
    NES::PartitionId partitionId;
    NES::SubpartitionId subpartitionId;
    NES::OperatorId operatorId;
};

namespace YAML {
template<>
struct convert<WorkerLinkConfiguration> {
    static Node encode(const WorkerLinkConfiguration& rhs) {
        Node node;
        UNIKERNEL_MODEL_YAML_ENCODE(ip);
        UNIKERNEL_MODEL_YAML_ENCODE(port);
        UNIKERNEL_MODEL_YAML_ENCODE(partitionId);
        UNIKERNEL_MODEL_YAML_ENCODE(nodeId);
        UNIKERNEL_MODEL_YAML_ENCODE(subpartitionId);
        UNIKERNEL_MODEL_YAML_ENCODE(operatorId);
        return node;
    };

    static Node decode(const Node& node, WorkerLinkConfiguration& rhs) {
        UNIKERNEL_MODEL_YAML_DECODE(ip);
        UNIKERNEL_MODEL_YAML_DECODE(port);
        UNIKERNEL_MODEL_YAML_DECODE(partitionId);
        UNIKERNEL_MODEL_YAML_DECODE(nodeId);
        UNIKERNEL_MODEL_YAML_DECODE(subpartitionId);
        UNIKERNEL_MODEL_YAML_DECODE(operatorId);
        return node;
    };
};
}// namespace YAML

struct WorkerStageConfiguration {
    size_t stageId;
    size_t numberOfOperatorHandlers;
};

namespace YAML {
template<>
struct convert<WorkerStageConfiguration> {
    static Node encode(const WorkerStageConfiguration& rhs) {
        Node node;
        UNIKERNEL_MODEL_YAML_ENCODE(stageId);
        UNIKERNEL_MODEL_YAML_ENCODE(numberOfOperatorHandlers);
        return node;
    };

    static Node decode(const Node& node, WorkerStageConfiguration& rhs) {
        UNIKERNEL_MODEL_YAML_DECODE(stageId);
        UNIKERNEL_MODEL_YAML_DECODE(numberOfOperatorHandlers);
        return node;
    };
};
}// namespace YAML

struct WorkerSubQueryConfiguration {
    std::vector<WorkerStageConfiguration> stages;
    NES::QuerySubPlanId subQueryId;
    size_t outputSchemaSizeInBytes;
    WorkerLinkConfiguration downstream;
    std::vector<WorkerLinkConfiguration> upstreams;
};

namespace YAML {
template<>
struct convert<WorkerSubQueryConfiguration> {
    static Node encode(const WorkerSubQueryConfiguration& rhs) {
        Node node;
        UNIKERNEL_MODEL_YAML_ENCODE(stages);
        UNIKERNEL_MODEL_YAML_ENCODE(subQueryId);
        UNIKERNEL_MODEL_YAML_ENCODE(outputSchemaSizeInBytes);
        UNIKERNEL_MODEL_YAML_ENCODE(downstream);
        UNIKERNEL_MODEL_YAML_ENCODE(upstreams);
        return node;
    };

    static Node decode(const Node& node, WorkerSubQueryConfiguration& rhs) {
        UNIKERNEL_MODEL_YAML_DECODE(stages);
        UNIKERNEL_MODEL_YAML_DECODE(subQueryId);
        UNIKERNEL_MODEL_YAML_DECODE(outputSchemaSizeInBytes);
        UNIKERNEL_MODEL_YAML_DECODE(downstream);
        UNIKERNEL_MODEL_YAML_DECODE(upstreams);
        return node;
    };
};
}// namespace YAML

struct WorkerConfiguration {
    std::string ip;
    uint32_t port;
    NES::NodeId nodeId;
    std::vector<WorkerSubQueryConfiguration> subQueries;
};

namespace YAML {
template<>
struct convert<WorkerConfiguration> {
    static Node encode(const WorkerConfiguration& rhs) {
        Node node;
        UNIKERNEL_MODEL_YAML_ENCODE(ip);
        UNIKERNEL_MODEL_YAML_ENCODE(port);
        UNIKERNEL_MODEL_YAML_ENCODE(nodeId);
        UNIKERNEL_MODEL_YAML_ENCODE(subQueries);
        return node;
    };
    static Node decode(const Node& node, WorkerConfiguration& rhs) {
        UNIKERNEL_MODEL_YAML_DECODE(ip);
        UNIKERNEL_MODEL_YAML_DECODE(port);
        UNIKERNEL_MODEL_YAML_DECODE(nodeId);
        UNIKERNEL_MODEL_YAML_DECODE(subQueries);
        return node;
    }
};
}// namespace YAML

struct QueryConfiguration {
    size_t queryID;
    size_t workerID;
};

namespace YAML {
template<>
struct convert<QueryConfiguration> {
    static Node decode(const Node& node, QueryConfiguration& rhs) {
        UNIKERNEL_MODEL_YAML_DECODE(queryID);
        UNIKERNEL_MODEL_YAML_DECODE(workerID);
        return node;
    };
    static Node encode(const QueryConfiguration& rhs) {
        YAML::Node node;
        UNIKERNEL_MODEL_YAML_ENCODE(queryID);
        UNIKERNEL_MODEL_YAML_ENCODE(workerID);
        return node;
    };
};
}// namespace YAML

struct Configuration {
    std::vector<EndpointConfiguration> sources;
    EndpointConfiguration sink;
    std::vector<WorkerConfiguration> workers;
    QueryConfiguration query;
};

namespace YAML {
template<>
struct convert<Configuration> {
    static Node decode(const Node& node, Configuration& rhs) {
        UNIKERNEL_MODEL_YAML_DECODE(sources);
        UNIKERNEL_MODEL_YAML_DECODE(sink);
        UNIKERNEL_MODEL_YAML_DECODE(query);
        return node;
    };
    static Node encode(const Configuration& rhs) {
        YAML::Node node;
        UNIKERNEL_MODEL_YAML_ENCODE(sources);
        UNIKERNEL_MODEL_YAML_ENCODE(sink);
        UNIKERNEL_MODEL_YAML_ENCODE(query);
        return node;
    };
};
}// namespace YAML
#endif//NES_YAMLMODEL_H
