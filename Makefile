PROJECT := longhorn-engine
MACHINE := longhorn
# Define the target platforms that can be used across the ecosystem.
# Note that what would actually be used for a given project will be
# defined in TARGET_PLATFORMS, and must be a subset of the below:
DEFAULT_PLATFORMS := linux/amd64,linux/arm64

export SRC_BRANCH := $(shell bash -c 'source <(curl -s "https://raw.githubusercontent.com/longhorn/dep-versions/master/scripts/common.sh") && get_branch')
export SRC_TAG := $(shell git tag --points-at HEAD | head -n 1)

# Resolve git metadata on the host and pass it into the build. Inside the build container
# `git` does not work when the checkout is a git worktree (or a jj workspace), because .git
# is a file pointing to a path outside the build context. scripts/version falls back to
# running git itself when these are not provided. The values are exported and passed by
# name only, so docker reads them from the environment and no shell quoting is needed.
GIT_COMMIT := $(shell git rev-parse HEAD 2>/dev/null)
GIT_COMMIT_SHORT := $(shell git rev-parse --short HEAD 2>/dev/null)
GIT_DIRTY := $(shell test -n "$$(git status --porcelain --untracked-files=no 2>/dev/null)" && echo -dirty)
GIT_TAG := $(shell git tag -l --contains HEAD 2>/dev/null | head -n 1)
COMMIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null)
COMMIT_TAG := $(SRC_TAG)
export GIT_COMMIT GIT_COMMIT_SHORT GIT_DIRTY GIT_TAG COMMIT_BRANCH COMMIT_TAG
BUILD_DOCKER_ARGS := --build-arg GIT_COMMIT --build-arg GIT_COMMIT_SHORT --build-arg GIT_DIRTY
BUILD_DOCKER_ARGS += --build-arg GIT_TAG --build-arg COMMIT_BRANCH --build-arg COMMIT_TAG

.PHONY: build validate test ci package
build:
	docker buildx build $(BUILD_DOCKER_ARGS) --target build-artifacts --output type=local,dest=. -f Dockerfile .

validate:
	docker buildx build --target validate -f Dockerfile .

test:
	docker buildx build --target test -f Dockerfile .

ci:
	docker buildx build $(BUILD_DOCKER_ARGS) --target ci-artifacts --output type=local,dest=. -f Dockerfile .

package:
	bash scripts/package

.PHONY: integration-test
integration-test:
	docker buildx build --target base -t longhorn-engine-test -f Dockerfile .
	docker run --privileged \
		--tmpfs /go/src/github.com/longhorn/longhorn-engine/integration/.venv:exec \
		--tmpfs /go/src/github.com/longhorn/longhorn-engine/integration/.tox:exec \
		-v /dev:/host/dev -v /proc:/host/proc \
		--mount type=bind,source=/tmp,destination=/host/tmp,bind-propagation=rslave \
		longhorn-engine-test ./scripts/integration-test

.PHONY: sync-grpc-py
sync-grpc-py:
	docker buildx build --target base -t longhorn-engine-test -f Dockerfile .
	docker run longhorn-engine-test ./scripts/sync-grpc-py

.PHONY: buildx-machine
buildx-machine:
	@docker buildx create --name=$(MACHINE) --platform=$(DEFAULT_PLATFORMS) 2>/dev/null || true
	docker buildx inspect $(MACHINE)

# variables needed from GHA caller:
# - REPO: image repo, include $registry/$repo_path
# - TAG: image tag
# - TARGET_PLATFORMS: optional, to be passed for buildx's --platform option
# - IID_FILE_FLAG: optional, options to generate image ID file
.PHONY: workflow-image-build-push workflow-image-build-push-secure workflow-manifest-image
workflow-image-build-push: buildx-machine
	MACHINE=$(MACHINE) PUSH='true' IMAGE_NAME=$(PROJECT) bash scripts/package
workflow-image-build-push-secure: buildx-machine
	MACHINE=$(MACHINE) PUSH='true' IMAGE_NAME=$(PROJECT) IS_SECURE=true bash scripts/package
workflow-manifest-image:
	docker pull --platform linux/amd64 ${REPO}/longhorn-engine:${TAG}-amd64
	docker pull --platform linux/arm64 ${REPO}/longhorn-engine:${TAG}-arm64
	docker buildx imagetools create -t ${REPO}/longhorn-engine:${TAG} \
	  ${REPO}/longhorn-engine:${TAG}-amd64 \
	  ${REPO}/longhorn-engine:${TAG}-arm64

.DEFAULT_GOAL := ci
