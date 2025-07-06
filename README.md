<!--- the "--8<--" html comments define what part of the README to add to the index page of the documentation -->
<!--- --8<-- [start:docs] -->
![metro_disruptions_intelligence](resources/logos/title.png)

# Metro Disruptions Intelligence (metro_disruptions_intelligence)

<!--- --8<-- [end:docs] -->

[![Daily CI Build](https://github.com/LuisMartinParraMorales/metro_disruptions_intelligence/actions/workflows/daily-scheduled-ci.yml/badge.svg)](https://github.com/LuisMartinParraMorales/metro_disruptions_intelligence/actions/workflows/daily-scheduled-ci.yml)
[![Documentation](https://github.com/LuisMartinParraMorales/metro_disruptions_intelligence/actions/workflows/pages/pages-build-deployment/badge.svg?branch=gh-pages)](https://LuisMartinParraMorales.github.io/metro_disruptions_intelligence)

## Documentation

For more detailed instructions, see our [documentation](https://LuisMartinParraMorales.github.io/metro_disruptions_intelligence/latest).

## Installation

To install metro_disruptions_intelligence, we recommend using the [conda](https://docs.conda.io/en/latest/) package manager, accessible from the terminal by installing [miniforge](https://github.com/conda-forge/miniforge?tab=readme-ov-file#download).
Arup users on Windows can install `miniforge` from the Arup software shop by downloading "VS Code for Python" and then access `conda` from the VSCode integrated terminal.

### As a user

<!--- --8<-- [start:docs-install-user] -->

``` shell

git clone git@github.com:LuisMartinParraMorales/metro_disruptions_intelligence.git
cd metro_disruptions_intelligence
conda create -n metro_disruptions_intelligence -c conda-forge --file requirements/base.txt
conda activate metro_disruptions_intelligence
pip install --no-deps -e .
```

<!--- --8<-- [end:docs-install-user] -->

### As a developer

<!--- --8<-- [start:docs-install-dev] -->

``` shell
git clone git@github.com:LuisMartinParraMorales/metro_disruptions_intelligence.git
cd metro_disruptions_intelligence
conda create -n metro_disruptions_intelligence -c conda-forge --file requirements/base.txt --file requirements/dev.txt
conda activate metro_disruptions_intelligence
pip install --no-deps -e .
```

<!--- --8<-- [end:docs-install-dev] -->

For more detailed instructions, see our [documentation](https://LuisMartinParraMorales.github.io/metro_disruptions_intelligence/latest/installation/).

## Configuration

The data loader searches for your raw GTFS JSON files in the following order:

1. the ``METRO_DATA_DIR`` environment variable
2. ``config/local.yaml``
3. ``config/default.yaml``

If no valid path is found, an error is raised. Set ``METRO_DATA_DIR`` to the
directory containing the ``RAIL_RT_*`` folders or edit ``config/local.yaml`` to
point at that location.

## Processing realtime data

To convert raw GTFS-Realtime snapshots into partitioned Parquet files use the
``ingest_all_rt`` helper.  The easiest way is to open the notebook
``notebooks/01_process_all_rt.ipynb`` and run the cells.  This will read all
JSON files under ``data/raw`` and overwrite any existing output under
``data/processed/rt``.

Alternatively, the same process can be executed from the command line:

```bash
python -m metro_disruptions_intelligence.etl.ingest_rt data/raw --processed-root data/processed/rt
```

Once ingested, the realtime feeds are stored as Parquet files partitioned by
``year``/``month``/``day`` under ``data/processed/rt``.  Each feed type
(``alerts``, ``trip_updates`` and ``vehicle_positions``) has its own folder with
the same partition structure:

```text
data/processed/rt/alerts/year=2025/month=03/day=06/alerts_2025-06-03-16-49.parquet
```

To load all partitions for analysis, use the helper
``load_rt_dataset``:

```python
from pathlib import Path
from metro_disruptions_intelligence.processed_reader import load_rt_dataset

processed_rt = Path("data/processed/rt")
df = load_rt_dataset(
    processed_rt,
    output_file=processed_rt / "all_feeds.parquet",
)
```

This returns a DataFrame containing all rows across the three feeds with an
additional ``feed_type`` column indicating the source feed.

## Contributing

There are many ways to contribute to metro_disruptions_intelligence.
Before making contributions to the metro_disruptions_intelligence source code, see our contribution guidelines and follow the [development install instructions](#as-a-developer).

If you plan to make changes to the code then please make regular use of the following tools to verify the codebase while you work:

- `pre-commit`: run `pre-commit install` in your command line to load inbuilt checks that will run every time you commit your changes.
  The checks are: 1. check no large files have been staged, 2. lint python files for major errors, 3. format python files to conform with the [pep8 standard](https://peps.python.org/pep-0008/).
  You can also run these checks yourself at any time to ensure staged changes are clean by simple calling `pre-commit`.
- `pytest` - run the unit test suite and check test coverage.
- `pytest -p memray -m "high_mem" --no-cov` (not available on Windows) - after installing memray (`conda install memray pytest-memray`), test that memory and time performance does not exceed benchmarks.

For more information, see our [documentation](https://LuisMartinParraMorales.github.io/metro_disruptions_intelligence/latest/contributing/).

## Building the documentation

If you are unable to access the online documentation, you can build the documentation locally.
First, [install a development environment of metro_disruptions_intelligence](https://LuisMartinParraMorales.github.io/metro_disruptions_intelligence/latest/contributing/coding/), then deploy the documentation using [MkDocs](https://www.mkdocs.org/):

``` shell
mkdocs serve
```

Then you can view the documentation in a browser at <http://localhost:8000/>.

## License

Copyright (c) 2025 Luis Parra.
This repository is not open source.
You will need explicit permission from the repository owner to redistribute or make any modifications to this code.

## Credits

This package was created with [Cookiecutter](https://github.com/audreyr/cookiecutter) and the [arup-group/cookiecutter-pypackage](https://github.com/arup-group/cookiecutter-pypackage) project template.
