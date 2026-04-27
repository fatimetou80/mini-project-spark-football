# Soccer Results Analytics with Spark Streaming and ML

This project implements an end-to-end mini data pipeline for Mauritanian league soccer analytics:
- historical batch preparation with Spark;
- current-season web scraping enrichment;
- Kafka event simulation;
- Spark Structured Streaming aggregations;
- Parquet storage with checkpointing;
- Spark MLlib model training on historical data and evaluation on scraped current-season matches.

## Project Architecture

### Data Flow
1. **Historical batch layer** (`notebooks/prepare_data.ipynb`)
   - Loads `rim_championnat_results_2007-2025.csv`.
   - Cleans data types and dates.
   - Builds derived columns:
     - `total_goals`
     - `goal_difference`
     - `result` (`home_win`, `draw`, `away_win`)
     - `match_year`
   - Stores prepared historical data in Parquet.

2. **Current-season enrichment (web scraping)** (`notebooks/prepare_data.ipynb`)
   - Scrapes current 2025-2026 results from SoccerPunter.
   - Standardizes team names and schema.
   - Exports cleaned current-season dataset to CSV for testing/evaluation.

3. **Streaming simulation layer** (`notebooks/src/producer.py`)
   - Reads the cleaned current-season matches.
   - Publishes one JSON event per match to Kafka topic `mauritania_matches`.
   - Uses a short delay to emulate real-time ingestion.

4. **Structured Streaming analytics layer** (`notebooks/src/stream_job.py`)
   - Consumes Kafka events.
   - Uses `startingOffsets=latest` to process only new events after startup.
   - Parses JSON payload with explicit schema.
   - Applies event-time processing with watermark + window.
   - Computes real-time team goal aggregates.
   - Writes streaming outputs to Parquet with checkpointing.

5. **ML layer** (`notebooks/train_model.ipynb`)
   - Trains a match-result classifier on historical Parquet.
   - Evaluates on scraped current-season matches.
   - Reports `accuracy` and `F1-score`, plus sample predictions.
   - Includes bonus work: `form_gap` feature, model comparison (Logistic Regression vs Random Forest), and final visualizations.

## Repository Structure

**Important path note (Docker Compose alignment): scripts are kept in `notebooks/src` and data outputs are written under `data/outputs` because these are the directories mounted inside the Jupyter/Spark containers (`/home/jovyan/work` and `/workspace/data`).**

```text
mini-project-spark-football/
|-- README.md
|-- data/
|-- notebooks/
|   |-- prepare_data.ipynb
|   `-- train_model.ipynb
|-- notebooks/src/
|   |-- producer.py
|   `-- stream_job.py
|-- data/outputs/
|   |-- prepared/
|   |-- streaming/
|   `-- models/
|-- checkpoints/
`-- report.pdf
```

## Technical Choices

- **Storage format**: Parquet for compact columnar analytics and Spark compatibility.
- **Streaming source**: Kafka with JSON messages.
- **Streaming reliability**: Spark checkpoint directory to recover state.
- **Event-time handling**: watermark + tumbling window aggregation.
- **Model**: Spark MLlib Logistic Regression for 3-class match outcome prediction.

## How to Run

### 1) Start infrastructure
```bash
docker compose up -d --build
```

### 2) Prepare and enrich data
- Open Jupyter and run `notebooks/prepare_data.ipynb` from top to bottom.
- Verify outputs:
  - historical Parquet in `data/outputs/prepared/historical/`
  - scraped current-season CSV in `data/outputs/prepared/current_season/current_season_clean.csv`

### 3) Start streaming analytics
Run the Spark streaming consumer first:
```bash
python /home/jovyan/work/src/stream_job.py
```

In another terminal, run the Kafka producer:
```bash
python /home/jovyan/work/src/producer.py
```

Streaming outputs are written to:
- `data/outputs/streaming/football_stats`
- checkpoints: `checkpoints/football_stats`

Note: because the stream starts from `latest`, launch the consumer before the producer.  
For full topic replay during debugging, switch to `startingOffsets=earliest`.

### 4) Train and evaluate ML model
- Run `notebooks/train_model.ipynb`.
- Training uses historical prepared Parquet.
- Testing uses scraped current-season CSV.
- Trained models are saved to:
  - `data/outputs/models/logistic_regression_pipeline`
  - `data/outputs/models/random_forest_pipeline`

## Scraping Source

- Website: [SoccerPunter - Mauritania Super D1 2025-2026](http://www.soccerpunter.com/results/26572/Mauritania-Super-D1-2025-2026)
- Approach: requests + BeautifulSoup, extraction of completed matches with final score, date parsing, team normalization, schema alignment.

## Bonus Elements Included

1. **Recent form indicator**: `form_gap` feature in `notebooks/train_model.ipynb`.
2. **Comparison of several ML models**: Logistic Regression vs Random Forest in `notebooks/train_model.ipynb`.
3. **Final small visualization in notebook**: metric comparison chart and top scoring teams chart in `notebooks/train_model.ipynb`.


