# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Scalar UDFs that cost more per call than a single arithmetic operation, written in the subset of Python that
CPython, PyPy and Codon all accept (see PythonUdfLogisticScoreLarge.test).

The CPython/PyPy bridges need numpy in the venv passed via --worker.python_udf.venv; Codon uses its own numpy.
"""

import numpy as np


def logistic_score(price, datetime, auction_id, bidder):
    """Logistic-regression score of a Nexmark bid: sigmoid(w . x + b), with fixed weights."""
    weights = np.array([0.01, 0.000001, -0.000001, 0.000005])
    features = np.array([price, float(datetime), float(auction_id), float(bidder)])
    z = np.sum(weights * features) - 1.0
    return float(1.0 / (1.0 + np.exp(-z)))
