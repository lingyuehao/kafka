# Real-Time Flight Event Streaming System

## Overview

This project implements a real-time data streaming system using: Apache Kafka for event streaming, PostgreSQL for persistent storage, and Streamlit for interactive, auto-refreshing dashboards.

The system simulates real-time flight activity events, streams them into Kafka, stores them in PostgreSQL, and visualizes them live in Streamlit.

## Data Domain — Flight Events

I generate synthetic real-time aviation events, including:

| Field           | Example                                 | Description              |
| --------------- | --------------------------------------- | ------------------------ |
| `flight_id`     | `"DL1234"`                              | Unique flight identifier |
| `airline`       | `"Delta"`                               | Airline name             |
| `origin`        | `"SEA"`                                 | Departure airport        |
| `destination`   | `"SFO"`                                 | Arrival airport          |
| `status`        | `"On Time"`, `"Delayed"`, `"Cancelled"` | Live flight status       |
| `delay_minutes` | 0–120                                   | Delay duration           |
| `weather`       | `"Sunny"`, `"Rain"`, `"Storm"`          | Weather condition        |
| `gate`          | `"C16"`                                 | Departure gate           |
| `event_time`    | timestamp                               | Event generation time    |

## Architecture
```
Flight Event Generator (Producer)
          ↓
       Apache Kafka
          ↓
   Kafka Consumer → PostgreSQL
          ↓
     Streamlit Dashboard
```

## How to Run

### 1. Install dependencies
```
kafka-python==2.0.2
faker==19.3.1
psycopg2-binary==2.9.7
pandas==2.0.3
plotly==5.16.1
streamlit==1.26.0
sqlalchemy==2.0.20
```

### 2. Start Kafka + PostgreSQL
```
docker-compose up -d
```

### 3. Start the Producer (flight event generator)
```
python producer.py
```

### 4. Start the Kafka Consumer (database loader)
```
python consumer.py
```

### 5. Launch the Streamlit dashboard
```
streamlit run dashboard.py
```

## Screenshots

![](screenshots/1.png)

![](screenshots/2.png)

![](screenshots/3.png)



