import streamlit as st
import numpy as np
import joblib
import os
import matplotlib.pyplot as plt

# === Danh sách đặc trưng đầu vào ===
features = [
    "anchor_age", "gender", "icu_los", "num_diagnoses", "num_procedures", "num_drugs", "num_admissions",
    "50912", "50809", "51516", "50811", "220045", "220050", "220051", "220210", "223762", "220277"
]

# === Hàm nhập dữ liệu ===
def render_input_form(prefix=""):
    inputs = []
    for f in features:
        widget_key = f"{prefix}_{f}"
        if f == "gender":
            val = st.selectbox("Giới tính", ["Nam", "Nữ"], key=widget_key)
            val = 1 if val == "Nam" else 0
        else:
            val = st.number_input(f"{f}", value=0.0, key=widget_key)
        inputs.append(val)
    return np.array([inputs])

# === Tabs ===
st.title("🏥 Ứng dụng y tế: Phân cụm & Dự đoán tử vong")

tab1, tab2 = st.tabs(["🔍 Phân cụm KMeans", "💀 Dự đoán tử vong (RandomForest)"])

# === TAB 1: KMeans ===
with tab1:
    st.subheader("🔍 Phân cụm bệnh nhân bằng KMeans")
    input_array = render_input_form(prefix="kmeans")

    # Load models
    kmeans = joblib.load("models/kmeans/kmeans_model.pkl")
    scaler_km = joblib.load("models/kmeans/scaler.pkl")
    pca = joblib.load("models/kmeans/pca_model.pkl")

    input_scaled = scaler_km.transform(input_array)
    cluster = kmeans.predict(input_scaled)[0]

    st.success(f"Bệnh nhân thuộc cụm **#{cluster}**")

    # PCA plot
    input_pca = pca.transform(input_scaled)
    fig, ax = plt.subplots()
    ax.scatter(0, 0, c='gray', label='Gốc tọa độ')
    ax.scatter(input_pca[0][0], input_pca[0][1], c='blue', label='Bệnh nhân mới')
    ax.set_title("Vị trí bệnh nhân trên không gian PCA")
    ax.set_xlabel("PC1")
    ax.set_ylabel("PC2")
    ax.legend()
    st.pyplot(fig)

# === TAB 2: Random Forest ===
with tab2:
    st.subheader("💀 Dự đoán nguy cơ tử vong")
    input_array = render_input_form(prefix="rf")

    rf = joblib.load("models/rf/rf_model.pkl")
    scaler_rf = joblib.load("models/rf/scaler.pkl")

    input_scaled = scaler_rf.transform(input_array)
    proba = rf.predict_proba(input_scaled)[0][1]
    pred = rf.predict(input_scaled)[0]

    st.metric("Xác suất tử vong", f"{proba:.2%}")
    if pred == 1:
        st.error("⚠️ Có nguy cơ tử vong")
    else:
        st.success("✅ Không có nguy cơ tử vong")
