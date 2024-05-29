#include <Execution/Operators/MEOS/Meos.hpp>
#include <iostream>

using namespace std;

namespace NES {

    Meos::Meos() {
        meos_initialize("UTC", NULL);
        cout << "Meos initialized" << endl;
    } 
    Meos::~Meos() {
        meos_finalize();
    }
}