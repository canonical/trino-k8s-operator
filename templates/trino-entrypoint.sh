#!/bin/bash
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

cd /root && \
tar xf ranger-2.4.0-trino-plugin.tar.gz && \
cp -rf /root/install.properties /root/ranger-3.0.0-SNAPSHOT-trino-plugin/ && \
chown root:root -R /root/ranger-3.0.0-SNAPSHOT-trino-plugin/*
