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
        # 1. 날짜 변환
        df['date'] = pd.to_datetime(df['date'].astype(float), unit='s')
        # 2. 시간대 정보 제거 (UTC -> Naive)
        if df['date'].dt.tz is not None:
            df['date'] = df['date'].dt.tz_localize(None)
            
        df.set_index('date', inplace=True)
        return df['totalCirculating'].rename("Stablecoin_Market_Cap")
    except Exception as e:
        print(f"Error fetching Stablecoins: {e}")
        return pd.Series(dtype=float, name="Stablecoin_Market_Cap")

def fetch_etf_volume():
    """
    Yahoo Finance API 사용 (IBIT 거래량)
    * yfinance는 timezone이 포함된 데이터를 반환하므로 제거 필수
    """
    print("Fetching ETF Volume (IBIT)...")
    try:
        # 블랙록 ETF (IBIT) 데이터 가져오기
        ibit = yf.Ticker("IBIT")
        # 최근 1년 데이터
        hist = ibit.history(period="1y")
        
        if hist.empty:
            print("IBIT data is empty.")
            return pd.Series(dtype=float, name="ETF_Volume_Proxy")

        # 1. 인덱스(날짜)의 시간대 정보 제거 (America/New_York -> Naive)
        hist.index = hist.index.tz_localize(None)
        
        # 거래대금 = 거래량 * 종가 (대략적 추정)
        etf_flow_proxy = hist['Volume'] * hist['Close']
        return etf_flow_proxy.rename("ETF_Volume_Proxy")
    except Exception as e:
        print(f"Error fetching ETF Data: {e}")
        return pd.Series(dtype=float, name="ETF_Volume_Proxy")

def fetch_realized_cap():
    """CoinMetrics"""
    print("Fetching Realized Cap...")
    try:
        url = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
        params = {
            "assets": "btc",
            "metrics": "CapRealizedUSD",
            "frequency": "1d",
            "page_size": 365
        }
        response = scraper.get(url, params=params, timeout=15).json()
        
        if 'data' not in response:
            return pd.Series(dtype=float, name="BTC_Realized_Cap")

        df = pd.DataFrame(response['data'])
        # 1. 날짜 변환
        df['time'] = pd.to_datetime(df['time'])
        # 2. 시간대 정보 제거
        if df['time'].dt.tz is not None:
            df['time'] = df['time'].dt.tz_localize(None)
            
        df.set_index('time', inplace=True)
        df['CapRealizedUSD'] = pd.to_numeric(df['CapRealizedUSD'])
        return df['CapRealizedUSD'].rename("BTC_Realized_Cap")
    except Exception as e:
        print(f"Error fetching Realized Cap: {e}")
        return pd.Series(dtype=float, name="BTC_Realized_Cap")

def fetch_open_interest():
    """Binance"""
    print("Fetching Open Interest...")
    try:
        url = "https://fapi.binance.com/fapi/v1/openInterestHist"
        params = {"symbol": "BTCUSDT", "period": "1d", "limit": 365}
        
        response = scraper.get(url, params=params, timeout=15).json()

        if isinstance(response, dict): 
             print("Binance blocked this IP (returned dict).")
             return pd.Series(dtype=float, name="Binance_BTC_OI")
        
        if not isinstance(response, list):
             print("Binance response format error.")
             return pd.Series(dtype=float, name="Binance_BTC_OI")

        df = pd.DataFrame(response)
        if df.empty:
            return pd.Series(dtype=float, name="Binance_BTC_OI")

        # 1. 날짜 변환
        df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float), unit='ms')
        # 2. 시간대 정보 제거
        if df['timestamp'].dt.tz is not None:
            df['timestamp'] = df['timestamp'].dt.tz_localize(None)

        df.set_index('timestamp', inplace=True)
        return df['sumOpenInterestValue'].astype(float).rename("Binance_BTC_OI")
    except Exception as e:
        print(f"Error fetching OI: {e}")
        return pd.Series(dtype=float, name="Binance_BTC_OI")

def update_data():
    s1 = fetch_stablecoin_mcap()
    s2 = fetch_etf_volume() 
    s3 = fetch_realized_cap()
    s4 = fetch_open_interest()
    
    dfs = [s1, s2, s3, s4]
    
    # 병합 전 다시 한 번 인덱스 Timezone 제거 (안전장치)
    cleaned_dfs = []
    for s in dfs:
        if not s.empty and s.index.tz is not None:
            s.index = s.index.tz_localize(None)
        cleaned_dfs.append(s)

    # 데이터 병합
    try:
        final_df = pd.concat(cleaned_dfs, axis=1)
    except Exception as e:
        print(f"Critical Error during concat: {e}")
        # 빈 데이터프레임 리턴하여 중단 방지
        final_df = pd.DataFrame()

    # 컬럼 누락 방지
    expected = ["Stablecoin_Market_Cap", "ETF_Volume_Proxy", "BTC_Realized_Cap", "Binance_BTC_OI"]
    for col in expected:
        if col not in final_df.columns:
            final_df[col] = float('nan')

    # 데이터가 비어있지 않다면 필터링 수행
    if not final_df.empty:
        # 최근 1년 필터링 (Naive Timestamp 사용)
        one_year_ago = pd.Timestamp.now() - pd.DateOffset(days=365)
        
        # 인덱스 정렬
        final_df.sort_index(inplace=True)
        
        # 날짜 필터링
        final_df = final_df[final_df.index >= one_year_ago]
        
        # 결측치 채우기
        final_df.ffill(inplace=True)
    
    os.makedirs('data', exist_ok=True)
    final_df.to_csv('data/crypto_data.csv')
    return final_df

def generate_html(df):
    if df.empty: 
        print("Dataframe is empty, skipping HTML generation.")
        return

    # 값이 모두 NaN인 경우(데이터 수집 전멸) 체크
    if df.dropna(how='all').empty:
        print("Dataframe contains only NaN, skipping HTML generation.")
        return

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

    # 각 지표별로 데이터가 있는지 확인 후 그리기
    if not df['Stablecoin_Market_Cap'].isna().all():
        fig.add_trace(go.Scatter(x=df.index, y=df['Stablecoin_Market_Cap'], name="Stablecoin", line=dict(color='green')), row=1, col=1)
    
    if not df['ETF_Volume_Proxy'].isna().all():
        fig.add_trace(go.Bar(x=df.index, y=df['ETF_Volume_Proxy'], name="ETF Volume", marker_color='orange'), row=2, col=1)
    
    if not df['BTC_Realized_Cap'].isna().all():
        fig.add_trace(go.Scatter(x=df.index, y=df['BTC_Realized_Cap'], name="Realized Cap", line=dict(color='blue')), row=3, col=1)
    
    if not df['Binance_BTC_OI'].isna().all():
        fig.add_trace(go.Scatter(x=df.index, y=df['Binance_BTC_OI'], name="Open Interest", line=dict(color='red')), row=4, col=1)

    fig.update_layout(height=1200, title_text="Crypto Dashboard (GitHub Actions Ver.)", template="plotly_dark")
    
    os.makedirs('docs', exist_ok=True)
    fig.write_html("docs/index.html")
    print("HTML Generated successfully.")

if __name__ == "__main__":
    df = update_data()
    generate_html(df)
