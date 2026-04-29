import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.ensemble import RandomForestClassifier

# Load data
df = pd.read_csv("data/processed/feature_engineered_5g_data.csv")

# Select columns
model_df = df[
    [
        "signal_strength_dbm",
        "download_speed_mbps",
        "upload_speed_mbps",
        "latency_ms",
        "jitter_ms",
        "network_type",
        "carrier",
        "battery_level_percent",
        "handover_count",
        "data_usage_mb",
        "network_congestion_level",
        "ping_to_google_ms",
        "dropped_connection"
    ]
].copy()

# Encode categorical columns
categorical_cols = [
    "network_type",
    "carrier",
    "network_congestion_level"
]

le_dict = {}

for col in categorical_cols:
    le = LabelEncoder()
    model_df[col] = le.fit_transform(model_df[col])
    le_dict[col] = le

# Target convert
model_df["dropped_connection"] = model_df["dropped_connection"].astype(int)

# Split X y
X = model_df.drop("dropped_connection", axis=1)
y = model_df["dropped_connection"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y,
    test_size=0.2,
    random_state=42,
    stratify=y
)

# Train model
model = RandomForestClassifier(
    n_estimators=200,
    random_state=42
)

model.fit(X_train, y_train)

# Predict
y_pred = model.predict(X_test)

# Metrics
acc = accuracy_score(y_test, y_pred)

print("Accuracy:", round(acc, 4))
print("\nConfusion Matrix:")
print(confusion_matrix(y_test, y_pred))

print("\nClassification Report:")
print(classification_report(y_test, y_pred))

# Feature importance
importance = pd.DataFrame({
    "Feature": X.columns,
    "Importance": model.feature_importances_
}).sort_values(by="Importance", ascending=False)

print("\nTop Features:")
print(importance)

# Save importance
importance.to_csv(
    "data/output/model_feature_importance.csv",
    index=False
)

print("\nSaved:")
print("data/output/model_feature_importance.csv")