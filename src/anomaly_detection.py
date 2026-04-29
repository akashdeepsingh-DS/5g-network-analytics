import pandas as pd
from sklearn.ensemble import IsolationForest

# Load processed data
df = pd.read_csv("data/processed/feature_engineered_5g_data.csv")

# Features for anomaly detection
features = df[
    [
        "signal_strength_dbm",
        "download_speed_mbps",
        "upload_speed_mbps",
        "latency_ms",
        "jitter_ms",
        "ping_to_google_ms",
        "handover_count",
        "data_usage_mb"
    ]
]

# Train Isolation Forest
model = IsolationForest(
    n_estimators=100,
    contamination=0.03,
    random_state=42
)

df["anomaly_flag"] = model.fit_predict(features)

# Convert labels
df["anomaly_flag"] = df["anomaly_flag"].map({
    1: "Normal",
    -1: "Anomaly"
})

# Summary
print("Anomaly Summary:")
print(df["anomaly_flag"].value_counts())

# Save output
df.to_csv(
    "data/output/telemetry_with_anomalies.csv",
    index=False
)

print("\nSaved file:")
print("data/output/telemetry_with_anomalies.csv")