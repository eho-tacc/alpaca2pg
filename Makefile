include config.mk

image:
	docker build --rm -t $(DOCKER_IMAGE) .

tests-docker:
	docker run --rm -it $(DOCKER_IMAGE)
tests: tests-docker
	tox --