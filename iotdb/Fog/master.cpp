#include "iotMaster.hpp"

int main() {
  IotMaster master(1);
  master.serve();
  return 0;
}
