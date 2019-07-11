//
// Created by achaudhary on 11.07.19.
//

#ifndef IOTDB_NODECAPACITYENUM_H
#define IOTDB_NODECAPACITYENUM_H

namespace iotdb{
    enum CPUCapacity: int { LOW=1, MEDIUM=2, HIGH=3};

    std::string getCPUCapacityLevel(int capacity) {

        switch(capacity) {
            case LOW:
                return "LOW";
            case MEDIUM:
                return "MEDIUM";
            case HIGH:
                return "HIGH";
            default:
                return "UNKNOWN";
        }
    }
}
#endif //IOTDB_NODECAPACITYENUM_H
