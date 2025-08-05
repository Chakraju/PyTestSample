# DBTest Framework

## Overview
This framework replaces DBFit using pytest and YAML snapshots. It supports schema verification and rollback validation (sandbox only).

## Run
python main.py R1

## Jenkins
export RELEASE=R1
mvn test
