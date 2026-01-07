#!/bin/sh
cd /documents

gem install ruby-grafana-reporter

mkdir -p templates/images
mkdir -p reports

ruby-grafana-reporter -c grafana_reporter.config