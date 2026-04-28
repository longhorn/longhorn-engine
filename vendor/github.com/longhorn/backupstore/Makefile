MACHINE := longhorn

.PHONY: validate test ci

buildx-machine:
	@docker buildx create --name=$(MACHINE) 2>/dev/null || true

validate:
	docker buildx build --target validate-artifacts --output type=local,dest=. -f Dockerfile .

test:
	docker build -t backupstore-build --target base -f Dockerfile .
	@docker rm -f backupstore-test 2>/dev/null || true
	docker run --name backupstore-test --privileged -v /var/run/docker.sock:/var/run/docker.sock backupstore-build ./scripts/test; \
		rc=$$?; \
		docker cp backupstore-test:/go/src/github.com/longhorn/backupstore/coverage.out . 2>/dev/null || true; \
		docker rm backupstore-test 2>/dev/null || true; \
		exit $$rc

ci: validate test

.DEFAULT_GOAL := ci
