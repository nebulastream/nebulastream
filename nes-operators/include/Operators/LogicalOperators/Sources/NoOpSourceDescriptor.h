//
// Created by ls on 17.09.23.
//

#ifndef NES_NOOPSOURCEDESCRIPTOR_H
#define NES_NOOPSOURCEDESCRIPTOR_H

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {
class NoOpSourceDescriptor : public SourceDescriptor {
  public:
    static SourceDescriptorPtr create(SchemaPtr schemaPtr, std::string logicalSourceName);
    NoOpSourceDescriptor(SchemaPtr schemaPtr, std::string logicalSourceName);

    std::string toString() const override;

    bool equal(const SourceDescriptorPtr& other) const override;

    SourceDescriptorPtr copy() override;
};
}// namespace NES
#endif//NES_NOOPSOURCEDESCRIPTOR_H
