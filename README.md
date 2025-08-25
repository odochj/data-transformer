# Data Transformer

## Overview

This project is the result of a thought experiment; if you're in full control of both the incoming data *and* the dimensional data model, to what extent can you automate the transformation?

While [Data Ingestor](https://github.com/odochj/data-ingestor) is a *source-aligned* tool that writes data according to basic [Data Vault](https://en.wikipedia.org/wiki/Data_vault_modeling) principles, **Data Transformer** is a *model-aligned* tool with direct access to the metadata collected during the ingestion process. As such, while the **Data Ingestor** is a truly standalone tool, the **Data Transformer** is an extension of the former's capabilities. 

The aim of this tool is to be able to automatically produce the following:
1. A unified dimensional model, fed using canonical columns derived from all sources
2. Source-aligned Data Marts where *all* data is serviced

## Strategy

In **Data Transformer** counterpart to **Data Ingestor**'s `Source` object created is the `Table`. Rather than being explicitly defined however, a `Table` must instead be inferred from the results of the data ingestion.

`Tables` are designed as to allow for a flexible data model and so explicit definitions are not required. Each `Source` is expected to contain transactional or event-based data and so can easily conform to a `Fact` table in the resulting model. Canonical `Dimensions` are explicitly defined mapped to `Source` columns in the **Data Ingestor**, and can be derived by the **Data Transformer** using simple deduplication.   

The functions defined in `api_helper.py` are used to retrieve source-aligned metadata from the **Data Ingestor API**, while `TableBuilder` takes this data to produce `Tables`; one `Fact` table and multiple `Dimensions` as per the above, and as well as multiple dimensions. Each `Table` must then be registered in the `TableRegistry`

As a final step, a [SQLMesh](https://sqlmesh.readthedocs.io/en/stable/) Generator iterates over the `TableRegistry`, accessing the text stored in the `sql` attribute of each table, and saving these as SQLMesh models


## Quickstart (for MacOS)

```bash
# install dependencies
uv venv
source .venv/bin/activate
uv pip install -e . 

# Run SQLMesh
sqlmesh preview
sqlmesh plan
sqlmesh apply
```

## Project Structure

- `models/`: SQLMesh models
- `macros/`: Jinja macros
- `tests/`: Python-based tests
- `scripts/`: Model generation 
