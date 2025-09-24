#!/usr/bin/env python3
"""
J-Quants APIを使用して指定された銘柄コードの過去1年株価を取得するスクリプト

使用方法:
    python test_17_stock_price.py <銘柄コード>
    
例:
    python test_17_stock_price.py 7203  # トヨタ自動車
    python test_17_stock_price.py 6758  # ソニーグループ

前提条件:
    - token.txtファイルにAPIキー（リフレッシュトークン）が記述されていること
    - jquants-api-clientライブラリがインストールされていること
"""

import os
import sys
import json
from pathlib import Path
import jquantsapi
import pandas as pd
from datetime import datetime, timedelta


def load_api_key(token_file_path):
    """
    APIキーをファイルから読み込む
    
    Args:
        token_file_path (str): APIキーファイルのパス
        
    Returns:
        str: APIキー（リフレッシュトークン）
        
    Raises:
        FileNotFoundError: ファイルが見つからない場合
        ValueError: ファイルが空の場合
    """
    try:
        with open(token_file_path, 'r', encoding='utf-8') as file:
            token = file.read().strip()
            
        if not token:
            raise ValueError("APIキーファイルが空です")
            
        return token
        
    except FileNotFoundError:
        raise FileNotFoundError(f"APIキーファイルが見つかりません: {token_file_path}")
    except Exception as e:
        raise Exception(f"APIキーの読み込み中にエラーが発生しました: {e}")


def get_stock_price_one_year(client, code):
    """
    J-Quants APIから指定された銘柄の過去1年株価を取得する
    
    Args:
        client: J-Quants APIクライアント
        code (str): 銘柄コード
        
    Returns:
        pandas.DataFrame: 株価データのデータフレーム
        
    Raises:
        Exception: API接続エラー
    """
    try:
        # 過去1年の日付範囲を設定
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        
        print(f"銘柄コード {code} の過去1年株価データを取得中...")
        print(f"期間: {start_date} ～ {end_date}")
        
        # 株価データの取得
        price_data = client.get_prices_daily_quotes(
            code=code,
            from_yyyymmdd=start_date,
            to_yyyymmdd=end_date
        )
        
        # データフレームに変換
        df = pd.DataFrame(price_data)
        
        if df.empty:
            print(f"警告: 銘柄コード {code} のデータが見つかりませんでした")
            return df
        
        # 日付でソート（最新が最初に来るように降順）
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.sort_values('Date', ascending=False).reset_index(drop=True)
            
        print(f"株価データを取得しました: {len(df)} 件")
        return df
        
    except Exception as e:
        raise Exception(f"株価データの取得中にエラーが発生しました: {e}")


def analyze_new_highs(df):
    """
    過去1年のデータから新高値の分析を行う
    
    Args:
        df (pandas.DataFrame): 株価データのデータフレーム（日付順にソート済み）
        
    Returns:
        dict: 新高値分析結果
    """
    if df.empty or 'High' not in df.columns:
        return {
            'is_latest_new_high': False,
            'new_high_count': 0,
            'new_high_dates': [],
            'highest_price': None,
            'latest_high': None
        }
    
    # 高値データを取得（欠損値を除外）
    high_prices = df['High'].dropna()
    if high_prices.empty:
        return {
            'is_latest_new_high': False,
            'new_high_count': 0,
            'new_high_dates': [],
            'highest_price': None,
            'latest_high': None
        }
    
    # 過去最高値を初期化
    past_highest = 0
    new_high_count = 0
    new_high_dates = []
    
    # 日付順（古い順）にソートして処理
    df_sorted = df.sort_values('Date', ascending=True).reset_index(drop=True)
    
    for i, row in df_sorted.iterrows():
        current_high = row.get('High')
        current_date = row.get('Date')
        
        if current_high is not None and not pd.isna(current_high):
            # 新高値かどうかを判定
            if current_high > past_highest:
                new_high_count += 1
                new_high_dates.append({
                    'date': current_date,
                    'high': current_high
                })
                past_highest = current_high
    
    # 最新日が新高値かどうかを判定
    latest_high = df.iloc[0].get('High') if not df.empty else None
    is_latest_new_high = (latest_high is not None and 
                         not pd.isna(latest_high) and 
                         latest_high == past_highest)
    
    return {
        'is_latest_new_high': is_latest_new_high,
        'new_high_count': new_high_count,
        'new_high_dates': new_high_dates,
        'highest_price': past_highest,
        'latest_high': latest_high
    }


def display_stock_price_summary(df, code):
    """
    株価データの要約を表示する
    
    Args:
        df (pandas.DataFrame): 株価データのデータフレーム
        code (str): 銘柄コード
    """
    if df.empty:
        print(f"銘柄コード {code} のデータが見つかりませんでした。")
        return
        
    print(f"\n【銘柄コード {code} の過去1年株価データ要約】")
    print("=" * 60)
    print(f"取得データ数: {len(df)} 件")
    
    if len(df) > 0:
        # 最新のデータを表示
        latest_data = df.iloc[0]
        print(f"最新日付: {latest_data.get('Date', 'N/A')}")
        print(f"最新終値: {latest_data.get('Close', 'N/A'):,}円" if latest_data.get('Close') else "最新終値: N/A")
        
        # 最古のデータを表示
        oldest_data = df.iloc[-1]
        print(f"最古日付: {oldest_data.get('Date', 'N/A')}")
        print(f"最古終値: {oldest_data.get('Close', 'N/A'):,}円" if oldest_data.get('Close') else "最古終値: N/A")
        
        # 価格の統計情報
        if 'Close' in df.columns and not df['Close'].isna().all():
            close_prices = df['Close'].dropna()
            if not close_prices.empty:
                print(f"最高値: {close_prices.max():,}円")
                print(f"最安値: {close_prices.min():,}円")
                print(f"平均値: {close_prices.mean():.2f}円")
        
        # データの期間を表示
        if 'Date' in df.columns and not df['Date'].isna().all():
            dates = df['Date'].dropna()
            if not dates.empty:
                print(f"データ期間: {dates.min().strftime('%Y-%m-%d')} ～ {dates.max().strftime('%Y-%m-%d')}")
    
    print("=" * 60)


def display_new_high_analysis(new_high_analysis, code):
    """
    新高値分析結果を表示する
    
    Args:
        new_high_analysis (dict): 新高値分析結果
        code (str): 銘柄コード
    """
    print(f"\n【銘柄コード {code} の新高値分析結果】")
    print("=" * 60)
    
    # 最新日での新高値判定
    is_latest_new_high = new_high_analysis['is_latest_new_high']
    latest_high = new_high_analysis['latest_high']
    highest_price = new_high_analysis['highest_price']
    
    print(f"最新日での新高値: {'はい' if is_latest_new_high else 'いいえ'}")
    if latest_high is not None:
        print(f"最新日の高値: {latest_high:,}円")
    if highest_price is not None:
        print(f"過去1年間の最高値: {highest_price:,}円")
    
    # 過去1年間の新高値回数
    new_high_count = new_high_analysis['new_high_count']
    print(f"過去1年間の新高値回数: {new_high_count} 回")
    
    # 新高値をつけた日付の詳細（最新5件まで）
    new_high_dates = new_high_analysis['new_high_dates']
    if new_high_dates:
        print(f"\n新高値をつけた日付（最新5件）:")
        for i, record in enumerate(new_high_dates[-5:], 1):
            date_str = record['date'].strftime('%Y-%m-%d') if hasattr(record['date'], 'strftime') else str(record['date'])
            print(f"  {i}. {date_str}: {record['high']:,}円")
        
        if len(new_high_dates) > 5:
            print(f"  ... 他 {len(new_high_dates) - 5} 件")
    
    print("=" * 60)


def validate_stock_code(code):
    """
    銘柄コードの形式を検証する
    
    Args:
        code (str): 銘柄コード
        
    Returns:
        bool: 有効な形式かどうか
    """
    if not code:
        return False
    
    # 4桁の数字かチェック
    if not code.isdigit() or len(code) != 4:
        return False
        
    return True


def main():
    """メイン処理"""
    try:
        # 引数の確認
        if len(sys.argv) != 2:
            print("使用方法: python test_17_stock_price.py <銘柄コード>")
            print("例: python test_17_stock_price.py 7203")
            sys.exit(1)
        
        stock_code = sys.argv[1]
        
        # 銘柄コードの検証
        if not validate_stock_code(stock_code):
            print(f"エラー: 無効な銘柄コードです: {stock_code}")
            print("銘柄コードは4桁の数字で入力してください。")
            sys.exit(1)
        
        # スクリプトのディレクトリを取得
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        token_file_path = project_root / "token.txt"
        
        print("J-Quants API 過去1年株価取得スクリプト")
        print("=" * 50)
        
        # APIキーの読み込み
        print(f"APIキーを読み込み中: {token_file_path}")
        refresh_token = load_api_key(token_file_path)
        print("APIキーの読み込み完了")
        
        # クライアントの初期化
        client = jquantsapi.Client(refresh_token=refresh_token)
        
        # 株価データの取得
        df = get_stock_price_one_year(client, stock_code)
        
        # 結果の表示
        display_stock_price_summary(df, stock_code)
        
        # 新高値分析の実行
        new_high_analysis = analyze_new_highs(df)
        
        # 新高値分析結果の表示
        display_new_high_analysis(new_high_analysis, stock_code)
        
        # 取得した株価データの件数を出力
        print(f"\n【結果】")
        print(f"銘柄コード {stock_code} の過去1年株価データ取得件数: {len(df)} 件")
        print(f"過去1年間の新高値回数: {new_high_analysis['new_high_count']} 回")
        print(f"最新日での新高値: {'はい' if new_high_analysis['is_latest_new_high'] else 'いいえ'}")
        
        print("\n処理が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
