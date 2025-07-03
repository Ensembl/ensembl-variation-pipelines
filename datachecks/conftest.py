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

import pytest
import pyBigWig
from cyvcf2 import VCF
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def pytest_addoption(parser):
    parser.addoption("--vcf", type=str, default=None)
    parser.addoption("--bigbed", type=str, default=None)
    parser.addoption("--bigwig", type=str, default=None)
    parser.addoption("--source_vcf", type=str, default=None)
    parser.addoption("--species", type=str, default=None)


def pytest_generate_tests(metafunc):
    if "vcf" in metafunc.fixturenames:
        metafunc.parametrize("vcf", [metafunc.config.getoption("vcf")])
    if "bigbed" in metafunc.fixturenames:
        metafunc.parametrize("bigbed", [metafunc.config.getoption("bigbed")])
    if "bigwig" in metafunc.fixturenames:
        metafunc.parametrize("bigwig", [metafunc.config.getoption("bigwig")])
    if "source_vcf" in metafunc.fixturenames:
        metafunc.parametrize("source_vcf", [metafunc.config.getoption("source_vcf")])
    if "species" in metafunc.fixturenames:
        metafunc.parametrize("species", [metafunc.config.getoption("species")])


@pytest.fixture()
def vcf_reader(vcf):
    vcf_reader = VCF(vcf)
    return vcf_reader


@pytest.fixture()
def bb_reader(bigbed):
    bb_reader = pyBigWig.open(bigbed)
    return bb_reader


@pytest.fixture()
def bw_reader(bigwig):
    bw_reader = pyBigWig.open(bigwig)
    return bw_reader
