import requests
import json
import pandas as pd

def roc_to_ad(roc_date: str) -> str:
    """Convert ROC date (e.g. 114/07/01) to AD string (e.g. 2025/07/01)."""
    parts = roc_date.split("/")
    year = int(parts[0]) + 1911
    return f"{year}/{parts[1]}/{parts[2]}"
    
def get_otc_intraday_lending_info(date: str) -> pd.DataFrame:
    url = "https://www.tpex.org.tw/www/zh-tw/intraday/fee"
    data = {"date": date.replace("-", "/"), "id": "", "response": "json"}
    headers = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}

    r = requests.post(url, data=data, headers=headers, timeout=15)
    r.raise_for_status()
    resp = r.json() if r.headers.get("content-type","").startswith("application/json") else json.loads(r.text)
    df = pd.DataFrame(resp['tables'][0]['data'])
    df.columns = ['date', 'symbol', 'stock_name', 'lending_quantity', 'lending_fee']
    df['lending_quantity'] = df['lending_quantity'].apply(lambda x: int(x.replace(',', '')))
    df['lending_fee'] = df['lending_fee'].astype(float) / 100
    df["date"] = pd.to_datetime(
        df["date"].apply(roc_to_ad),
        format="%Y/%m/%d"
    )
    return df


def get_twse_intraday_lending_info(date: str) -> pd.DataFrame:
    url = f"https://www.twse.com.tw/rwd/zh/dayTrading/BFIF8U?date={date.replace('-', '')}&response=json"
    r = requests.get(url, timeout=15)

    r.raise_for_status()
    resp = r.json() if r.headers.get("content-type","").startswith("application/json") else json.loads(r.text)
    df = pd.DataFrame(resp['data'])
    df.columns = ['date', 'symbol', 'stock_name', 'lending_quantity', 'lending_fee']
    df['symbol'] = df['symbol'].apply(lambda x: x.strip())
    df['lending_quantity'] = df['lending_quantity'].apply(lambda x: int(x.replace(',', '')))
    df['lending_fee'] = df['lending_fee'].apply(lambda x: float(x.replace('%', ''))) / 100
    df["date"] = pd.to_datetime(
        df["date"].apply(roc_to_ad),
        format="%Y/%m/%d"
    )
    return df

def get_intraday_lending_info(date: str) -> pd.DataFrame:
    df_otc = get_otc_intraday_lending_info(date)
    df_twse = get_twse_intraday_lending_info(date)
    df_params = pd.concat([df_twse, df_otc], ignore_index=True)
    return df_params