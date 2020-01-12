#ifndef IOTDB_CPUCAPACITY_H
#define IOTDB_CPUCAPACITY_H

namespace NES {
    class CPUCapacity {

    public:
        enum Value : int {
            LOW = 1, MEDIUM = 2, HIGH = 3
        };

        CPUCapacity(Value value) : value(value) {}
        int toInt() {
            return value;
        }

    private:
        Value value;
    };
}
#endif //IOTDB_CPUCAPACITY_H
