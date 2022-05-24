//
// Created by eleicha on 20.05.22.
//

#ifndef NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_TENSORSOURCEDESCRIPTOR_HPP
#define NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_TENSORSOURCEDESCRIPTOR_HPP

#include <Catalogs/Source/PhysicalSourceTypes/TensorSourceType.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <chrono>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical Tensor source
 */
class TensorSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema, TensorSourceTypePtr csvSourceType);

    /**
     * @brief get source config ptr with all configurations for csv source
     */
    TensorSourceTypePtr getSourceConfig() const;

    [[nodiscard]] bool equal(SourceDescriptorPtr const& other) override;
    std::string toString() override;
    SourceDescriptorPtr copy() override;

  private:
    explicit TensorSourceDescriptor(SchemaPtr schema, TensorSourceTypePtr sourceConfig);

    TensorSourceTypePtr tensorSourceType;
};

using TensorSourceDescriptorPtr = std::shared_ptr<TensorSourceDescriptor>;

}// namespace NES

#endif// NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_TENSORSOURCEDESCRIPTOR_HPP
