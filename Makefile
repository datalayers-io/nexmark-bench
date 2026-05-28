.PHONY: format format-python format-shell typo

SHELL_FILES := $(shell find . -maxdepth 2 -type f -name '*.sh' | sort)
PYTHON_FILES := $(shell find . -maxdepth 2 -type f -name '*.py' | sort)
SHFMT ?= /home/nsc/.local/bin/shfmt
RUFF ?= /home/nsc/.local/bin/ruff
TYPOS ?= /home/nsc/.cargo/bin/typos

format: format-shell format-python

format-shell:
	@test -x "$(SHFMT)" || { echo "missing shfmt at $(SHFMT)"; exit 1; }
	"$(SHFMT)" -w $(SHELL_FILES)

format-python:
	@test -x "$(RUFF)" || { echo "missing ruff at $(RUFF)"; exit 1; }
	"$(RUFF)" format $(PYTHON_FILES)

typo:
	@test -x "$(TYPOS)" || { echo "missing typos at $(TYPOS)"; exit 1; }
	"$(TYPOS)" -w .
	"$(TYPOS)" .
