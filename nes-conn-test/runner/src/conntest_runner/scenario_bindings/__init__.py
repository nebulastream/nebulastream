# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Per-scenario constructors for a plugin's ``conn-test/scenarios.py``.

A connector imports them from the module of its kind, so far only ``source``.
Each module offers only the scenarios that its kind can bind, so binding another kind's scenario fails as an import when it is written.
Each constructor takes only the arguments that its scenario accepts, so an invalid argument is a ``TypeError`` when it is written,
not a defect found at run time.
"""
