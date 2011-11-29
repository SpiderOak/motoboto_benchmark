#!/bin/bash
CODEBASE="${HOME}/motoboto_benchmarks"
export PYTHONPATH="${CODEBASE}"

pushd "${CODEBASE}/docs"
make html
popd
