#!/bin/bash
bumpversion $1
git push && git push --tags
TAG=$(git describe --tags)
#docker build --platform linux/amd64 -t caveconnectome/cavecanary:${TAG} .
#docker push docker.io/caveconnectome/cavecanary:${TAG}
# python setup.py sdist
# twine upload dist/materializationengine-${TAG:1}.tar.gz
