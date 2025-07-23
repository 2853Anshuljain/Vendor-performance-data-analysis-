import sqlite3
import pandas as pd 
import logging
from ingestion_db import ingest_db  # ensure this is defined correctly

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(conn):
    '''This function will merge the different tables to get the overall vendor summary and add new columns in the resultant data.'''
    vendor_sales_summary = pd.read_sql_query("""
    WITH 
    PurchaseSummary AS (
        SELECT 
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            pp.Volume,
            pp.Price AS ActualPrice,
            p.PurchasePrice,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM Purchases p
        JOIN purchase_prices pp ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, pp.Volume, pp.Price, p.PurchasePrice, p.Description
    ),
    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT 
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.Volume,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
    ORDER BY ps.TotalPurchaseDollars DESC
    """, conn)
    return vendor_sales_summary

def clean_data(df):
    '''This function will clean and enrich the data.'''
    df['Volume'] = df['Volume'].astype(float)
    df.fillna(0, inplace=True)
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['SalestoPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100

    return df

if __name__ == '__main__':
    try:
        conn = sqlite3.connect('inventory.db')
        logging.info('Creating Vendor Summary Table...')
        summary_df = create_vendor_summary(conn)
        logging.info(summary_df.head().to_string())

        logging.info('Cleaning Data...')
        clean_df = clean_data(summary_df)
        logging.info(clean_df.head().to_string())

        logging.info('Ingesting Data...')
        ingest_db(clean_df, 'vendor_sales_summary', conn)
        logging.info('Ingestion Completed')

    except Exception as e:
        logging.error(f"Error occurred: {e}")
