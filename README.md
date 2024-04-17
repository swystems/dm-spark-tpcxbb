# dm-spark-tpcxbb

Spark SQL homework based on TPCx-BB for the Data Management course

## Get started

- Read the homework sheet delivery requirements
- Clone this repository
- Perform the setup steps to get a clean working environment

## Setup

The dependency management of the python3.10 project is done using 
[poetry](https://python-poetry.org/docs/).
It will create a virtual environment and install the dependencies in it.

Here a short script to do so assuming an Ubuntu system:
```shell
# install pipx
sudo apt update             && \
sudo apt install pipx       && \
pipx ensurepath             && \

# install poetry
pipx install poetry         && \

# from repository root where pyproject.toml is located
poetry install              && \

# to run the shell within the virtual environment
poetry shell;
```

Refer to its documentation to install it.

Suggestion: 
use the same environment in Pycharm IDE by selecting the python
interpreter from the poetry venv[^1] to benefit from its integration.

[^1]: virtual environment
