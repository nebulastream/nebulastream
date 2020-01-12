#include <Topology/NESTopologyEntry.hpp>

namespace NES{

void NESTopologyEntry::setNodeProperty(std::string nodeProperties) {
    if (nodeProperties != "")
      this->nodeProperties = std::make_shared<NodeProperties>(nodeProperties);
    else
      this->nodeProperties = std::make_shared<NodeProperties>();
  }

  /**
   * @brief method to get the node properties
   * @return serialized json of the node properties object
   */
  std::string NESTopologyEntry::getNodeProperty() {
    return this->nodeProperties->dump();
  }


  std::string NESTopologyEntry::toString() {
    return "id=" + std::to_string(getId()) + " type=" + getEntryTypeString();
  }

  NodePropertiesPtr NESTopologyEntry::getNodeProperties() {
    return nodeProperties;
  }

}
