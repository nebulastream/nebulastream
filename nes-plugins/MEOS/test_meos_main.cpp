#include <iostream>
#include "include/MEOSWrapper.hpp"

int main() {
    std::cout << "Testing MEOS Integration..." << std::endl;
    
    try {
        MEOS::Meos meos("UTC");
        std::cout << "✅ MEOS initialized successfully!" << std::endl;
        return 0;
    } catch (...) {
        std::cout << "❌ MEOS test failed" << std::endl;
        return 1;
    }
}
