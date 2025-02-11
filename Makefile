TARGETS := $(shell ls scripts)

export SRC_BRANCH := $(shell bash -c 'wget -q "https://raw.githubusercontent.com/longhorn/dep-versions/main/scripts/common.sh" -O build-common.sh && source build-common.sh && get_branch')
export SRC_TAG := $(shell git tag --points-at HEAD | head -n 1)

.dapper:
	@echo Downloading dapper
	@curl -sL https://releases.rancher.com/dapper/latest/dapper-`uname -s`-`uname -m` > .dapper.tmp
	@@chmod +x .dapper.tmp
	@./.dapper.tmp -v
	@mv .dapper.tmp .dapper

$(TARGETS): .dapper
	./.dapper $@

trash: .dapper
	./.dapper -m bind trash

trash-keep: .dapper
	./.dapper -m bind trash -k

deps: trash

.DEFAULT_GOAL := ci

.PHONY: $(TARGETS)
