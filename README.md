# 5G Network Performance Analytics & Failure Prediction Platform

##  Overview

An end-to-end telecom analytics project designed to simulate how network operations teams monitor service quality, detect abnormal sessions, and analyze dropped connections using large-scale telemetry data.

Built using **PySpark, Python, Machine Learning, and Power BI**, this project transforms raw 5G network session data into actionable operational insights.


## Business Problem

Telecom providers handle thousands of daily network sessions across multiple carriers, devices, cities, and network technologies.

Poor service quality can lead to:

- High latency  
- Weak signal strength  
- Slow download/upload speeds  
- Congestion issues  
- Dropped connections  
- Poor customer experience  

### This project answers:

- Which carriers and cities perform best?
- What factors are associated with dropped connections?
- Can abnormal network sessions be detected automatically?
- Can connection failures be predicted using ML?



##  Architecture

    Raw CSV Dataset
        ↓
    PySpark Data Ingestion
        ↓
    Data Cleaning & Validation
        ↓
    Feature Engineering
        ↓
    KPI & Root Cause Analytics
        ↓
    Anomaly Detection
        ↓
    Predictive Modeling
        ↓
    Power BI Dashboard


## Tech Stack

### Languages & Libraries
- Python
- PySpark
- Pandas
- NumPy
- Scikit-learn

### Visualization
- Power BI
    
### Development Tools
- VS Code
- Jupyter Notebook
- GitHub

## Project Structure

5g-network-analytics/
│── data/
│   ├── raw/
│   ├── processed/
│   └── output/
│
│── src/
│   ├── data_ingestion.py
│   ├── data_cleaning.py
│   ├── feature_engineering.py
│   ├── kpi_analysis.py
│   ├── root_cause_analysis.py
│   ├── anomaly_detection.py
│   └── predictive_model.py
│
│── dashboard/
│── README.md
│── requirements.txt

## Dataset Summary

- 50,000 network session records
- Multiple cities and carriers
- 4G / 5G network types
- Device and performance telemetry

### Key Fields

- Timestamp
- Location
- Carrier
- Network Type
- Signal Strength
- Download Speed
- Upload Speed
- Latency
- Jitter
- Congestion Level
- Dropped Connection

### Key Features

- Data Engineering
    - Processed raw telecom telemetry using PySpark
    - Cleaned schemas and standardized column names
    - Created analytics-ready datasets

- Feature Engineering

    - Built derived metrics such as:
    
        Signal quality groups
        
        Latency categories
        
        Congestion flags
        
        Poor signal indicators
        
        Total latency
        
        Speed difference

- KPI Analytics

    - Analyzed:
    
        Avg download/upload speed
        
        Avg latency and jitter
        
        Drop rate %
        
        Carrier performance
        
        City performance
        
        4G vs 5G comparison

- Root Cause Analysis

    - Explored relationships between:
    
        Congestion and failures
        
        Signal strength and service quality
        
        Carrier performance differences
        
        Latency hotspots

- Anomaly Detection

    - Used Isolation Forest to identify abnormal sessions such as:
    
        Latency spikes
        
        Poor throughput
        
        Weak signal events

- Predictive Modeling
    - Built a Random Forest classifier to predict dropped connections and evaluate feature importance.

### Key Results

- KPI Highlights
    
    - Total Sessions: 50,000
    
    - Avg Download Speed: 551 Mbps
    
    - Avg Upload Speed: 84 Mbps
    
    - Avg Latency: 10.5 ms
    
    - Drop Rate: 50%

- Anomaly Detection
    
    - Normal Sessions: 48,500
    
    - Anomalies Detected: 1,500   

- Model Output
    
    - Random Forest Accuracy: 49%
    
    - Demonstrated importance of data quality and target signal in ML workflows


## Power BI Dashboard

Built an interactive operations dashboard with:

- Executive Summary
    
    - Total Sessions
    
    - Avg Speed
    
    - Avg Latency
    
    - Drop Rate
    
    - Anomaly Count

- Performance Analysis
    
    - Carrier comparison
    
    - Speed by city
    
    - Latency trends
    
    - Signal quality insights

- Failure Intelligence
    
    - Drop rate by congestion
    
    - Drop rate by signal quality
    
    - Root cause visuals

- AI Monitoring

    - Anomaly trends

    - Feature importance

    - Risk indicators


## How to Run

- Install Dependencies
    
    pip install -r requirements.txt

- Run Pipeline
    
    python src/data_ingestion.py
    
    python src/data_cleaning.py
    
    python src/feature_engineering.py
    
    python src/kpi_analysis.py
    
    python src/root_cause_analysis.py
    
    python src/anomaly_detection.py
    
    python src/predictive_model.py

- Future Improvements
    
    Real-time streaming with Kafka
    
    Azure Databricks deployment
    
    MLflow experiment tracking
    
    Grafana monitoring dashboards
    
    Time-series forecasting
    
    Real production telecom data integration


- Author

Akash Deep Singh

Barrie, Ontario, Canada

Data Analyst | Machine Learning | Big Data | AI Enthusiast