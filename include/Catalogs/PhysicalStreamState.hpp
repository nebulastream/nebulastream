//
// Created by Vincent Szlang on 16.06.21.
//

// BDAPRO: ADD CopyRight
// BDAPRO ADD states as enum
// BDAPRO add addLog() function, removeLog(), getState(), setState()
// BDAPRO detect miscongig on count == 0


#ifndef NES_PHYSICALSTREAMSTATE_HPP
#define NES_PHYSICALSTREAMSTATE_HPP

#include <string>

namespace NES {
    enum State {misconfigured, regular};

    class PhysicalStreamState {
    public:
        // BDAPRO add constructor
       // PhysicalStreamState();
        /**
         * @brief raises state objects count variable by one if a logical stream is added. Also checks internally if count > 0 now, which changes state to regular.
         *
         */
        void increaseLogicalStreamCount();

        /**
         * @brief lowers state objects count variable by one if a logical stream is added. Also checks internally if count == 0 now, which changes state to misconfigured.
         *
         */
        void lowerLogicalStreamCount();

        /**
         * @brief changes the State in case count has been increased/lowered to a significant value (0 => misconfigured, > 0 => regular)
         *
         */
        void changeState();

        State state;
    private:
        int count;
    };



}// namespace NES

#endif //NES_PHYSICALSTREAMSTATE_HPP
