# GSU Attendance Dashboard

A modular Flask web application for managing and visualizing ROTC attendance, availability, and OML rankings.

## 🚀 Deployment

1. **Clone this repo** to your GitHub.
2. **Add your environment variables** on Render (use the same ones from your local `.env`).
3. Deploy — Render will automatically build and launch using `gunicorn`.

## 🧠 Features

- Caches Google Sheet data daily at **0500 CST** for speed.
- Dashboard with daily attendance & Chart.js visualization.
- Tabs for Directory, Reports, Availability Checker, OML, Writer, and Waterfall Matrix.
- Password-protected sections (toggle via env vars).
- Modular routes for easy troubleshooting — each feature is fully isolated.

## 🧰 Stack

- **Flask 3.0**
- **gspread + google-auth**
- **pandas**
- **Chart.js** (client-side only)
- **Render** (deployment)

## 🗂 Structure
