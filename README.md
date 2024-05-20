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

If you prefer to manually install the dependencies,
check out the `pyproject.toml`.

Here a script to leverage the automated installation of
the virtual environment and deps on a Debian-based systems:
```shell
# install pipx
sudo apt update             && \
sudo apt install -y pipx    && \
pipx ensurepath             && \

# install poetry
pipx install poetry         && \

# from repository root where pyproject.toml is located
poetry install              && \

# to run the shell within the virtual environment
poetry shell;
```

## Run

To run the Jupyter Lab in order to edit the notebook,
from within the repository root run the following command:

```shell
jupyter lab
```

It will open a browser tab with the Jupyter lab interface.

Suggestion:
use the same python environment in your IDE
by selecting the python interpreter from the poetry venv,
e.g. pycharm [^1],
to benefit from its integration.

[^1]: https://www.jetbrains.com/help/pycharm/poetry.html#existing-poetry-environment
