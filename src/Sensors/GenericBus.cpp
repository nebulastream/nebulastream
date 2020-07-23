#include <Sensors/GenericBus.hpp>
#include <Util/Logger.hpp>

namespace NES {
namespace Sensors {

GenericBus::GenericBus(const char* filename, BusType type)
    : fileName(filename), busType(type) {
    NES_INFO("Sensor Bus: Initializing " << type << " bus at " << filename);
}

GenericBus::~GenericBus() {
    NES_DEBUG("Sensor Bus: Destroying " << this->busType << " bus at " << fileName);
}

bool GenericBus::init(int address) {
    return this->initBus(address);
}

bool GenericBus::write(int addr, int size, unsigned char* buffer) {
    return this->writeData(addr, size, buffer);
}

bool GenericBus::read(int addr, int size, unsigned char* buffer) {
    return this->readData(addr, size, buffer);
}

BusType GenericBus::getType() {
    return this->busType;
}

}//namespace Sensors
}//namespace NES