/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef NES_NAUTILUS_INTERFACE_ROLLOVERINDEX_HPP
#define NES_NAUTILUS_INTERFACE_ROLLOVERINDEX_HPP

namespace NES::Nautilus::Interface
    {

        /**
         * @brief Index with rollover behavior and a callback on rollover.
         *
         * When the index reaches or exceeds the rollover value, it is reset
         * to zero and the callback passed in the constructor is invoked.
         */
        class RolloverIndex
        {
        public:
            /// Callback type: plain function pointer with no arguments and no return.
            typedef void (*Callback)();

            /**
             * @brief Construct a new RolloverIndex.
             *
             * @param rolloverValue Value at which the index rolls over back to 0.
             * @param onRollover    Function that is called whenever a rollover happens.
             */
            RolloverIndex(unsigned long rolloverValue, Callback onRollover);

            /**
             * @brief Explicitly set the current index.
             *
             * If the new index is >= rolloverValue, a rollover is triggered.
             */
            void setIndex(unsigned long index);

            /**
             * @brief Get the current index.
             */
            unsigned long getIndex() const;

            /**
             * @brief Set a new rollover value.
             *
             * The current index is kept unchanged.
             */
            void setRolloverValue(unsigned long rolloverValue);

            /**
             * @brief Get the current rollover value.
             */
            unsigned long getRolloverValue() const;

            /**
             * @brief Convenience function: increment index by 1.
             *
             * If the incremented index reaches or exceeds the rollover value,
             * a rollover is triggered.
             */
            void increment();

        private:
            /**
             * @brief Perform rollover: reset index to 0 and call the callback.
             */
            void rollover();

            unsigned long index_;
            unsigned long rolloverValue_;
            Callback      callback_;
        };

    } // namespace NES::Nautilus::Interface

#endif // NES_NAUTILUS_INTERFACE_ROLLOVERINDEX_HPP
