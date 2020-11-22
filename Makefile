# Environments
ENV := env
PIP_COMPILE = $(ENV)/bin/pip-compile --no-emit-index-url --upgrade --build-isolation --output-file
PYTHON_VERSION ?= python3.8

# Logs
PREFECT_SERVER_LOGS = logs/prefect_server.log
PREFECT_AGENT_LOGS = logs/prefect_agent.log

# Local
START_PREFECT_SERVER = $(ENV)/bin/prefect server start >> $(PREFECT_SERVER_LOGS) 2>&1 &
START_PREFECT_AGENT = $(ENV)/bin/prefect agent local start >> $(PREFECT_AGENT_LOGS) 2>&1 &

# -- Taco Grande --

.PHONY: ready
ready: develop start_prefect_server start_prefect_agent ## 🌄 Startup script.

.PHONY: upgrade
upgrade: develop upgrade_reqs test ## Upgrade dependency versions.

.PHONY: kill
kill: kill_prefect_agent kill_prefect_server ## 🔥 Kill background processes.

# -- Install --

env: $(ENV)/bin/activate
$(ENV)/bin/activate: requirements/main.txt requirements/dev.txt
	test -d $(ENV) || ${PYTHON_VERSION} -m venv $(ENV)
	$(ENV)/bin/pip install --upgrade pip-tools pip setuptools
	for f in $^; do $(ENV)/bin/pip install -r $$f; done
	touch $(ENV)/bin/activate

.PHONY: install_brews
install_brews: ## 🍺 Pour beer.
	$(call check_venv)
	@brew update
	@brew bundle
	@brew unlink python@3.9
	@brew link python@3.8
	@brew link docker

.PHONY: install_prefect
install_prefect:
	$(ENV)/bin/prefect backend server

.PHONTY: develop
develop: env ## Install python package.
	$(ENV)/bin/pip install --no-build-isolation -e .

.PHONY: install
install: prompt_danger install_brews develop install_prefect test ## 📐 Set up project.

# -- Testing --

.PHONY: test
test: develop ## 🧪 Run tests.
	$(ENV)/bin/python -m pytest tests/

# -- Upgrade -- 

.PHONY: upgrade_reqs
upgrade_reqs: requirements/main.in requirements/dev.in ## Upgrade dependency versions.
	$(ENV)/bin/pip install --upgrade pip-tools pip setuptools
	$(PIP_COMPILE) requirements/main.txt requirements/main.in
	$(PIP_COMPILE) requirements/dev.txt requirements/dev.in

# -- Clean Up --

.PHONY: clear_temp
clear_temp: prompt_danger ## Clean up cache.

.PHONY: clear_env
clear_env: prompt_danger ## Clean up env.
	@rm -rfv env/*

.PHONY: clear_logs
clear_logs: prompt_danger ## Clean up local log files.
	@rm -rfv logs/*

.PHONY: nuke
nuke: prompt_danger kill clean_env clear_temp clear_logs install ## 💣 This will erase everything, and reinstall.

# -- Prefect --

.PHONY: start_prefect_server
start_prefect_server: ## Use "make prefect". Starts a local prefect server.
	$(call create_file,$(PREFECT_SERVER_LOGS))
	$(call run_if_off,"prefect server",$(START_PREFECT_SERVER))

.PHONY: kill_prefect_server
kill_prefect_server: ## Kills local prefect server.
	$(call kill_process,"prefect server")

.PHONY: start_prefect_agent
start_prefect_agent: ## Use "make prefect". Starts a local prefect agent.
	$(call create_file,$(PREFECT_AGENT_LOGS))
	$(call run_if_off,"prefect agent",$(START_PREFECT_AGENT))

.PHONY: kill_prefect_agent
kill_prefect_agent: ## Kills local prefect agent.
	$(call kill_process,"prefect agent")

.PHONY: prefect
prefect: kill_prefect_agent kill_prefect_server start_prefect_server start_prefect_agent

# -- Utils --

.PHONY: prompt_danger
prompt_danger:
	@echo "Are you sure? [y/N] " && read char && [ $${char:-N} = y ]

# Functions
define check_venv
	@if [ -n "$$VIRTUAL_ENV" ]; then \
		echo "Script can't run inside a virtualenv. Run 'deactivate' and try again"; \
		exit 1; \
	fi
endef

define kill_process
	@for pid in `ps auxww | grep $(1) | grep -v grep | awk '{print $$2}'`; do \
		kill -9 $$pid ; \
	done
endef

define create_file
	@mkdir -p `dirname "$(1)"`
	@test -f $(1) || touch $(1)
endef

define run_if_off
	@ps auxww | grep $(1) | grep -v grep >/dev/null && echo "Already running $(1)" || (echo "Starting $(1)"; $(2))
endef

# Help
.DEFAULT_GOAL := help
.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'