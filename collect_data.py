import pandas as pd
import cloudscraper
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datetime
import os
import io
import time

# Cloudflare 우회용 스크래퍼 생성
scraper = cloudscraper.create_scraper(
    browser={
        'browser': 'chrome',
        'platform': 'windows',
        'desktop': True
    }
)

def fetch_stablecoin_mcap():
    """DeFiLlama: Cloudscraper 적용"""
    print("Fetching Stablecoin Data...")
    try:
        url = "https://stablecoins.llama.fi/stablecoincharts/all"
        response = scraper.get(url, timeout=15).json()
        
        df = pd.DataFrame(response)
        df['date'] = pd.to_datetime(df['date'].astype(float), unit='s')
        df.set_index('date', inplace=True)
        return df['totalCirculating'].rename("Stablecoin_Market_Cap")
    except Exception as e:
        print(f"Error fetching Stablecoins: {e}")
        return pd.Series(dtype=float, name="Stablecoin_Market_Cap")

def fetch_etf_volume():
    """
    대체 전략: Farside 크롤링이 막히면 Yahoo Finance API 사용
    (순유입 데이터 대신 거래량으로 대체 - 차단 가능성 거의 없음)
    """
    print("Fetching ETF Volume (IBIT)...")
    try:
        # 블랙록 ETF (IBIT) 데이터 가져오기
        ibit = yf.Ticker("IBIT")
        # 최근 1년 데이터
        hist = ibit.history(period="1y")
        
        # 거래대금 = 거래량 * 종가 (대략적 추정)
        # 순유입과 비례하는 경향이 있음
        etf_flow_proxy = hist['Volume'] * hist['Close']
        return etf_flow_proxy.rename("ETF_Volume_Proxy")
    except Exception as e:
        print(f"Error fetching ETF Data: {e}")
        return pd.Series(dtype=float, name="ETF_Volume_Proxy")

def fetch_realized_cap():
    """CoinMetrics: 여전히 차단될 수 있으나 Scraper로 시도"""
    print("Fetching Realized Cap...")
    try:
        url = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
        params = {
            "assets": "btc",
            "metrics": "CapRealizedUSD",
            "frequency": "1d",
            "page_size": 365
        }
        # Scraper 사용
        response = scraper.get(url, params=params, timeout=15).json()
        
        if 'data' not in response:
            return pd.Series(dtype=float, name="BTC_Realized_Cap")

        df = pd.DataFrame(response['data'])
        df['time'] = pd.to_datetime(df['time'])
        df.set_index('time', inplace=True)
        df['CapRealizedUSD'] = pd.to_numeric(df['CapRealizedUSD'])
        return df['CapRealizedUSD'].rename("BTC_Realized_Cap")
    except Exception as e:
        print(f"Error fetching Realized Cap: {e}")
        return pd.Series(dtype=float, name="BTC_Realized_Cap")

def fetch_open_interest():
    """Binance: API 차단 시 Coinglass 대체 불가(유료) -> 데이터 없을 시 0 처리"""
    print("Fetching Open Interest...")
    try:
        url = "https://fapi.binance.com/fapi/v1/openInterestHist"
        params = {"symbol": "BTCUSDT", "period": "1d", "limit": 365}
        
        # Scraper 사용
        response = scraper.get(url, params=params, timeout=15).json()

        if isinstance(response, dict): # 에러 메시지인 경우
             print("Binance blocked this IP.")
             return pd.Series(dtype=float, name="Binance_BTC_OI")

        df = pd.DataFrame(response)
        df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float), unit='ms')
        df.set_index('timestamp', inplace=True)
        return df['sumOpenInterestValue'].astype(float).rename("Binance_BTC_OI")
    except Exception as e:
        print(f"Error fetching OI: {e}")
        return pd.Series(dtype=float, name="Binance_BTC_OI")

def update_data():
    s1 = fetch_stablecoin_mcap()
    s2 = fetch_etf_volume() # ETF 순유입 -> 거래량으로 대체
    s3 = fetch_realized_cap()
    s4 = fetch_open_interest()
    
    dfs = [s1, s2, s3, s4]
    final_df = pd.concat(dfs, axis=1)
    
    # 컬럼 누락 방지
    expected = ["Stablecoin_Market_Cap", "ETF_Volume_Proxy", "BTC_Realized_Cap", "Binance_BTC_OI"]
    for col in expected:
        if col not in final_df.columns:
            final_df[col] = float('nan')

    # 최근 1년 필터링
    one_year_ago = pd.Timestamp.now(tz=datetime.timezone.utc).tz_localize(None) - pd.DateOffset(days=365)
    # 인덱스 TZ 정보 제거 (병합 시 문제 방지)
    final_df.index = final_df.index.tz_localize(None)
    final_df = final_df[final_df.index >= one_year_ago]
    
    final_df.sort_index(inplace=True)
    final_df.ffill(inplace=True)
    
    os.makedirs('data', exist_ok=True)
    final_df.to_csv('data/crypto_data.csv')
    return final_df

def generate_html(df):
    if df.empty: return

    fig = make_subplots(
        rows=4, cols=1,
        shared_xaxes=True,
        subplot_titles=(
            "Stablecoin Liquidity (DeFiLlama)", 
            "IBIT ETF Volume (Yahoo Finance Proxy)",
            "BTC Realized Cap (CoinMetrics)",
            "Binance Open Interest"
        )
    )

    fig.add_trace(go.Scatter(x=df.index, y=df['Stablecoin_Market_Cap'], name="Stablecoin", line=dict(color='green')), row=1, col=1)
    fig.add_trace(go.Bar(x=df.index, y=df['ETF_Volume_Proxy'], name="ETF Volume", marker_color='orange'), row=2, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['BTC_Realized_Cap'], name="Realized Cap", line=dict(color='blue')), row=3, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['Binance_BTC_OI'], name="Open Interest", line=dict(color='red')), row=4, col=1)

    fig.update_layout(height=1200, title_text="Crypto Dashboard (Cloudscraper Ver.)", template="plotly_dark")
    
    os.makedirs('docs', exist_ok=True)
    fig.write_html("docs/index.html")

if __name__ == "__main__":
    df = update_data()
    generate_html(df)
