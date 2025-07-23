# Vendor Performance Data Analysis Project

## Overview

This project aims to analyze and report on vendor performance using a complete data pipeline built with **Python**, **MySQL**, and **Power BI**. The project includes data extraction, transformation and analysis, and the creation of an interactive report for deep insights into vendor efficiency and compliance.

## Table of Contents

- [Project Objectives](#project-objectives)
- [Tech Stack](#tech-stack)
- [Setup & Installation](#setup--installation)
- [Workflow](#workflow)
- [Key Features](#key-features)
- [Project Structure](#project-structure)
- [Usage](#usage)
- [Deliverables](#deliverables)
- [License](#license)

## Project Objectives

- Assess and compare vendor performance on key metrics.
- Identify top and low-performing vendors using visual analytics.
- Generate actionable insights to improve procurement and supply chain decisions.

## Tech Stack

| Technology | Purpose                       |
|------------|-------------------------------|
| Python     | Data extraction, cleaning, ETL|
| MySQL      | Data storage and management   |
| Power BI   | Data visualization & reporting|

## Setup & Installation

### Prerequisites

- Python 3.8+ (with `pandas`, `sqlalchemy`, `mysql-connector-python`)
- MySQL Server (with vendor data loaded)
- Power BI Desktop

### Steps

1. **Clone Repository**
   - `git clone `
2. **Set Up Python Environment**
   - Create virtual environment (optional):  
     `python -m venv venv`
   - Activate virtual environment
   - Install dependencies:  
     `pip install -r requirements.txt`
3. **Configure Database**
   - Update database credentials in `config.py`
   - Run provided SQL scripts to initialize sample tables if needed.
4. **Prepare Power BI**
   - Open the `.pbix` file in Power BI Desktop.
   - Configure data source settings to point to your MySQL server.
5. **Run Data Scripts**
   - Execute ETL scripts:  
     `python etl_vendor_data.py`

## Workflow

1. **Data Extraction**: Python scripts connect to MySQL and retrieve raw vendor data.
2. **Data Cleaning & Transformation**: Data is processed using pandas for missing value handling, KPI calculation, and normalization.
3. **Database Update**: Cleaned and transformed data is written back to MySQL.
4. **Visualization**: Power BI connects to MySQL, pulling clean data for dashboard and report creation.
5. **Reporting**: Project report summarizes findings, methodology, and actionable recommendations.

## Key Features

- Automated **data cleaning and preparation**.
- **Interactive dashboards** for performance tracking.
- Vendor benchmarking on metrics such as:
  - On-time delivery rate
  - Quality compliance
  - Cost efficiency
- Exportable reports in PDF and PowerPoint.

## Project Structure

```
├── data/
│   └── Vendor_Raw_Data.csv
├── scripts/
│   ├── etl_vendor_data.py
│   └── analysis.py
├── powerbi/
│   └── VendorPerformanceDashboard.pbix
├── report/
│   └── Vendor_Performance_Report.pdf
├── requirements.txt
├── README.md
└── config.py
```

## Usage

- Update your MySQL credentials and data paths in `config.py`.
- Run the ETL script to process and store data.
- Visualize and analyze data in Power BI using the provided dashboard.
- Refer to the `report/` directory for the detailed project report.

## Deliverables

- Python ETL and analysis scripts
- MySQL schema and setup scripts
- Power BI interactive dashboard
- Comprehensive project report (PDF)

## License

Distributed under the MIT License. See `LICENSE` for more details.
