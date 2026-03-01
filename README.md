# Netflix Data Warehouse (Medallion)

A simple medallion pipeline using MinIO + Python + Jupyter.

## Quick Start

1. Clone and enter the project:
   `git clone https://github.com/RakhaRabbani/netflix-datawarehouse && cd netflix-datawarehouse`

2. Start all services:
   `docker compose up -d --build`

3. Open services:
   - Jupyter: `http://localhost:8888`
   - MinIO Console: `http://localhost:9001`

## Run the Pipeline

Open a shell in the compute container:
`docker exec -it bigdata-compute bash`

Run transformations:
`python3 scripts/transform.py`

Run aggregation:
`python3 scripts/aggregate.py`
