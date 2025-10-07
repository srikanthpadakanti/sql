#!/usr/bin/env bash

set -exu  # Enable debugging and exit on error

curl -LO https://artifacts.apple.com/crypto-services-binaries-local/whisperctl/prod/${WHISPER_VERSION}/whisperctl_${WHISPER_VERSION}_linux_amd64.tar.gz
tar -xf whisperctl_${WHISPER_VERSION}_linux_amd64.tar.gz
mv whisperctl /usr/local/bin
