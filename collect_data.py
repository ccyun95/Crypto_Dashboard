import pandas as pd
import requests
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datetime
import os
import time

# --- 1. 데이터 수집 함수 ---

def fetch_stablecoin_mcap():
    """DeFiLlama: 전체 스테이블코인 시가총액"""
    print("Fetching Stablecoin Data...")
    try:
        url = "https://stablecoins.llama.fi/stablecoincharts/all"
        response = requests.get(url).json()
        df = pd.DataFrame(response)
        df['date'] = pd.to_datetime(df['date'], unit='s')
        df.set_index('date', inplace=True)
        df = df['totalCirculating'].rename("Stablecoin_Market_Cap")
        return df
    except Exception as e:
        print(f"Error fetching Stablecoins: {e}")
        return pd.Series(dtype=float)

def fetch_etf_flows():
    """Farside: 비트코인 현물 ETF 순유입 (크롤링)"""
    print("Fetching ETF Flows...")
    try:
        # User-Agent 설정 필수
        headers = {'User-Agent': 'Mozilla/5.0'}
        url = "https://farside.co.uk/btc/"
        # pandas의 read_html로 테이블 추출
        dfs = pd.read_html(requests.get(url, headers=headers).text)
        
        # Farside 테이블 구조에 맞춰 데이터 정제 (가장 큰 테이블이 데이터일 확률 높음)
        df = dfs[0]
        # 날짜 컬럼과 Total 컬럼만 필요 (구조 변경 가능성 있음)
        # 예시: Date, Total Flow 컬럼 찾기 (실제 컬럼명 확인 필요, 여기선 가정하에 작성)
        df.columns = df.columns.astype(str) # 컬럼명을 문자로 통일
        
        # 'Date'와 'Total'이 포함된 컬럼 찾기
        date_col = [c for c in df.columns if 'Date' in c][0]
        total_col = [c for c in df.columns if 'Total' in c][0]
        
        df = df[[date_col, total_col]].copy()
        df.columns = ['date', 'ETF_Net_Flow']
        
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df.dropna(subset=['date'], inplace=True)
        df.set_index('date', inplace=True)
        
        # 값에 있는 '$', ',' 제거 후 숫자 변환
        df['ETF_Net_Flow'] = df['ETF_Net_Flow'].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False)
        df['ETF_Net_Flow'] = pd.to_numeric(df['ETF_Net_Flow'], errors='coerce')
        
        return df['ETF_Net_Flow']
    except Exception as e:
        print(f"Error fetching ETF Flows: {e}")
        return pd.Series(dtype=float)

def fetch_realized_cap():
    """CoinMetrics: BTC 실현 시가총액 (Community API)"""
    print("Fetching Realized Cap...")
    try:
        url = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
        params = {
            "assets": "btc",
            "metrics": "CapRealizedUSD",
            "frequency": "1d",
            "page_size": 365 # 최근 1년
        }
        response = requests.get(url, params=params).json()
        data = response['data']
        
        df = pd.DataFrame(data)
        df['time'] = pd.to_datetime(df['time'])
        df.set_index('time', inplace=True)
        df['CapRealizedUSD'] = pd.to_numeric(df['CapRealizedUSD'])
        return df['CapRealizedUSD'].rename("BTC_Realized_Cap")
    except Exception as e:
        print(f"Error fetching Realized Cap: {e}")
        return pd.Series(dtype=float)

def fetch_open_interest():
    """Binance Futures: BTCUSDT 미결제 약정"""
    print("Fetching Open Interest...")
    try:
        url = "https://fapi.binance.com/fapi/v1/openInterestHist"
        params = {
            "symbol": "BTCUSDT",
            "period": "1d",
            "limit": 365
        }
        response = requests.get(url, params=params).json()
        df = pd.DataFrame(response)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        # sumOpenInterestValue: 달러 환산 가치
        return df['sumOpenInterestValue'].astype(float).rename("Binance_BTC_OI")
    except Exception as e:
        print(f"Error fetching OI: {e}")
        return pd.Series(dtype=float)

# Glassnode는 유료 API 키가 없으면 데이터 수집 불가하므로 제외하거나
# 키가 있을 때만 작동하도록 구성
def fetch_glassnode_reserves(api_key):
    if not api_key:
        print("Skipping Glassnode (No API Key)")
        return pd.Series(dtype=float)
    # ... (API 호출 로직) ...
    return pd.Series(dtype=float)

# --- 2. 데이터 병합 및 CSV 저장 ---

def update_data():
    # 각 데이터 수집
    stablecoin = fetch_stablecoin_mcap()
    etf = fetch_etf_flows()
    realized_cap = fetch_realized_cap()
    oi = fetch_open_interest()
    
    # 데이터프레임 병합 (Outer Join으로 날짜 맞춤)
    dfs = [stablecoin, etf, realized_cap, oi]
    final_df = pd.concat(dfs, axis=1)
    
    # 최근 1년치 데이터로 필터링
    one_year_ago = pd.Timestamp.now() - pd.DateOffset(days=365)
    final_df = final_df[final_df.index >= one_year_ago]
    
    # 날짜순 정렬 및 결측치 보간 (ETF 주말 비는 것 등 처리)
    final_df.sort_index(inplace=True)
    final_df.fillna(method='ffill', inplace=True) # 전일 데이터로 채움
    
    # CSV 저장
    os.makedirs('data', exist_ok=True)
    final_df.to_csv('data/crypto_data.csv')
    print("CSV Saved.")
    return final_df

# --- 3. HTML 차트 생성 ---

def generate_html(df):
    print("Generating HTML...")
    
    # 4개의 서브플롯 생성
    fig = make_subplots(
        rows=4, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=(
            "Stablecoin Market Cap (Liquidity)", 
            "Bitcoin Spot ETF Net Flows (Institutional)",
            "BTC Realized Cap (On-Chain Value)",
            "Binance Open Interest (Speculation)"
        )
    )

    # 1. Stablecoin
    fig.add_trace(go.Scatter(x=df.index, y=df['Stablecoin_Market_Cap'], name="Stablecoin Cap", line=dict(color='green')), row=1, col=1)
    
    # 2. ETF (Bar chart)
    colors = ['red' if x < 0 else 'green' for x in df['ETF_Net_Flow']]
    fig.add_trace(go.Bar(x=df.index, y=df['ETF_Net_Flow'], name="ETF Flows", marker_color=colors), row=2, col=1)
    
    # 3. Realized Cap
    fig.add_trace(go.Scatter(x=df.index, y=df['BTC_Realized_Cap'], name="Realized Cap", line=dict(color='orange')), row=3, col=1)
    
    # 4. Open Interest
    fig.add_trace(go.Scatter(x=df.index, y=df['Binance_BTC_OI'], name="Open Interest", line=dict(color='blue')), row=4, col=1)

    fig.update_layout(height=1200, title_text=f"Crypto Market Flows Dashboard (Updated: {datetime.datetime.now().strftime('%Y-%m-%d')})", template="plotly_dark")
    
    os.makedirs('docs', exist_ok=True)
    fig.write_html("docs/index.html")
    print("HTML Generated.")

if __name__ == "__main__":
    df = update_data()
    generate_html(df)
  
