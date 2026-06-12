# OrcaGlue

OrcaGlue – Frontline ETL for Pipeline Automation Data Warehouse

## Development

A Python shop!

This project is all about writing a Python script that runs on AWS Glue as an ETL job.

* Typical ETL script should be a small and a focused task. Hence, the repo is structured as in [a monorepo](https://www.google.com/search?q=monorepo) manner.
* There are multiple ETL modules organised into subdirectories. 
* Each module has its own README file to follow.

Create a Python virtual environment (any method) and install the dev toolchain [requirements](requirements-dev.txt).

See [README_DEV.md](README_DEV.md) for Python version requirement and more _comprehensive_ setup details.

```
conda activate oncoglue
make install
make check
```
