#ifndef CPUCAPACITY_H
#define CPUCAPACITY_H

namespace NES {
class CPUCapacity {

  public:
    enum Value : int {
        LOW = 1,
        MEDIUM = 2,
        HIGH = 3
    };

    CPUCapacity(Value value) : value(value) {}
    int toInt() {
        return value;
    }

  private:
    Value value;
};
}// namespace NES
#endif//CPUCAPACITY_H
