# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_nes_test(curl-http-backend-test CurlHttpBackendTest.cpp)
target_link_libraries(curl-http-backend-test nes-http)

# Drives the event loop against the dummy server, which is where "many in flight on one thread"
# can actually be observed rather than asserted.
add_nes_test(http-event-loop-test HttpEventLoopTest.cpp)
target_link_libraries(http-event-loop-test nes-http nes-http-testing)
