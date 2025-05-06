#!/usr/bin/env bash
# See https://github.com/bazelbuild/rules_go/wiki/Editor-setup#3-editor-setup
exec bazel run -- @io_bazel_rules_go//go/tools/gopackagesdriver "${@}"
