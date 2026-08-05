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

.PHONY: clean help install-deps

define target-help
	$1
		$2

endef

define print-help
        $(if $(need-help),$(info $(call target-help,$1,$2)))
endef

need-help := $(filter help,$(MAKECMDGOALS))

help: ; @echo $(if $(need-help),,\
	Type \'$(MAKE)$(dash-f) help\' to get help)

clean: \
	$(call print-help,clean,Delete the local Python virtualenv (.venv/).)
	rm -rf .venv/

.venv/: \
	$(call print-help,.venv/,Create the local Python virtualenv.)
	python -m venv .venv/

install-deps: .venv/ \
	$(call print-help,install-deps,Install package and all dependencies into the virtualenv.)
	.venv/bin/python -m pip install .
