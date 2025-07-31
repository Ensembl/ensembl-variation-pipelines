# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

def logAssert(test, msg):
    """Log the result of a test assertion.

    If the test condition is False, an error message is logged; otherwise, an informational message indicating a pass is logged.

    Args:
        test (bool): The condition to evaluate, where False indicates failure.
        msg (str): The message to log.

    Returns:
        None
    """
    if not test:
        logging.error("FAILED:", msg)
    else:
        logging.info("PASSED:", msg)