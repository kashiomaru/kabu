#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
過去3ヶ月でストップ高をつけた銘柄を検出するスクリプト（フェーズ1: 基本実装）

処理内容:
1. 指定された銘柄コードの過去3ヶ月の株価データを取得
2. ストップ高を検出（前日比13%以上上昇した日をストップ高と判定）
3. 結果を表示

使用方法:
    python test_40_stop_high_stocks.py <銘柄コード>
    
例:
    python test_40_stop_high_stocks.py 7203  # トヨタ自動車
    python test_40_stop_high_stocks.py 6758  # ソニーグループ

前提条件:
    - apikey.txtファイルにAPIキー（V2 APIキー）が記述されていること
    - requestsライブラリがインストールされていること
"""

import os
import sys
from pathlib import Path
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def load_api_key(apikey_file_path):
    """
    APIキーをファイルから読み込む
    
    Args:
        apikey_file_path (str): APIキーファイルのパス
        
    Returns:
        str: APIキー
        
    Raises:
        FileNotFoundError: ファイルが見つからない場合
        ValueError: ファイルが空の場合
    """
    try:
        with open(apikey_file_path, 'r', encoding='utf-8') as file:
            api_key = file.read().strip()
            
        if not api_key:
            raise ValueError("APIキーファイルが空です")
            
        return api_key
        
    except FileNotFoundError:
        raise FileNotFoundError(f"APIキーファイルが見つかりません: {apikey_file_path}")
    except Exception as e:
        raise Exception(f"APIキーの読み込み中にエラーが発生しました: {e}")


def get_stock_price_three_months(api_key, code, months=3):
    """
    J-Quants API V2から指定された銘柄の過去3ヶ月の株価データを取得する
    
    Args:
        api_key (str): J-Quants APIキー
        code (str): 銘柄コード
        months (int): 取得する月数（デフォルト: 3）
        
    Returns:
        pandas.DataFrame: 株価データのデータフレーム（Date, High, Close列を含む）
        
    Raises:
        Exception: API接続エラー
    """
    try:
        # 過去Nヶ月の日付範囲を設定
        end_date = datetime.now()
        start_date = end_date - timedelta(days=months * 30)  # 約Nヶ月前
        
        end_date_str = end_date.strftime('%Y%m%d')
        start_date_str = start_date.strftime('%Y%m%d')
        
        print(f"銘柄コード {code} の過去{months}ヶ月株価データを取得中...")
        print(f"期間: {start_date_str} ～ {end_date_str}")
        
        # V2 APIエンドポイント
        base_url = "https://api.jquants.com/v2/equities/bars/daily"
        
        # ヘッダーにAPIキーを設定
        headers = {
            'X-API-Key': api_key
        }
        
        # パラメータ
        params = {
            'code': code,
            'from': start_date_str,
            'to': end_date_str
        }
        
        # APIリクエスト
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()
        
        # レスポンスをJSONとして取得
        data = response.json()
        
        # V2 APIのレスポンス形式: {"data": [...], "pagination_key": "..."}
        if 'data' not in data or not data['data']:
            print(f"警告: 銘柄コード {code} のデータが見つかりませんでした")
            return pd.DataFrame()
        
        # データフレームに変換
        df = pd.DataFrame(data['data'])
        
        if df.empty:
            print(f"警告: 銘柄コード {code} のデータが見つかりませんでした")
            return df
        
        # V2 APIのカラム名をV1形式に変換（互換性のため）
        # V2: H, C, O, L, Vo → V1: High, Close, Open, Low, Volume
        column_mapping = {
            'H': 'High',
            'C': 'Close',
            'O': 'Open',
            'L': 'Low',
            'Vo': 'Volume'
        }
        
        # カラム名を変換
        df = df.rename(columns=column_mapping)
        
        # 日付でソート（古い順にソート、ストップ高判定のため）
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.sort_values('Date', ascending=True).reset_index(drop=True)
        
        print(f"株価データを取得しました: {len(df)} 件")
        return df
        
    except requests.exceptions.HTTPError as e:
        error_msg = f"HTTPエラー: {e.response.status_code}"
        if e.response.text:
            try:
                error_data = e.response.json()
                error_msg += f" - {error_data}"
            except:
                error_msg += f" - {e.response.text}"
        raise Exception(f"株価データの取得中にエラーが発生しました: {error_msg}")
    except Exception as e:
        raise Exception(f"株価データの取得中にエラーが発生しました: {e}")


def detect_stop_high(df, threshold_rate=0.13):
    """
    株価データからストップ高を検出する
    
    ストップ高の判定: 前日比で一定率（デフォルト13%）以上上昇した日をストップ高と判定
    
    Args:
        df (pandas.DataFrame): 日次株価データ（Date, High, Close列を含む）
        threshold_rate (float): ストップ高判定の閾値（デフォルト: 0.13 = 13%）
        
    Returns:
        pandas.DataFrame: ストップ高をつけた日のデータフレーム
    """
    if df.empty or 'High' not in df.columns or 'Close' not in df.columns:
        return pd.DataFrame()
    
    # 必要な列が存在するか確認
    required_columns = ['Date', 'High', 'Close']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"警告: 必要な列が見つかりません: {missing_columns}")
        return pd.DataFrame()
    
    # 前日の終値を計算（shift(1)で1行ずらす）
    df = df.copy()
    df['前日終値'] = df['Close'].shift(1)
    
    # 前日比上昇率を計算: (当日高値 - 前日終値) / 前日終値
    df['前日比上昇率'] = (df['High'] - df['前日終値']) / df['前日終値']
    
    # ストップ高の判定: 前日比上昇率 >= 閾値
    df['ストップ高'] = df['前日比上昇率'] >= threshold_rate
    
    # ストップ高をつけた日を抽出
    stop_high_df = df[df['ストップ高'] == True].copy()
    
    # 必要な列のみを残す
    result_columns = ['Date', 'High', 'Close', '前日終値', '前日比上昇率']
    available_columns = [col for col in result_columns if col in stop_high_df.columns]
    
    if stop_high_df.empty:
        return pd.DataFrame(columns=available_columns)
    
    result_df = stop_high_df[available_columns].copy()
    
    # 前日比上昇率をパーセント表示に変換（表示用）
    if '前日比上昇率' in result_df.columns:
        result_df['前日比上昇率(%)'] = (result_df['前日比上昇率'] * 100).round(2)
    
    return result_df


def display_stop_high_results(stop_high_df, code, months=3):
    """
    ストップ高検出結果を表示する
    
    Args:
        stop_high_df (pandas.DataFrame): ストップ高をつけた日のデータフレーム
        code (str): 銘柄コード
        months (int): 対象期間（月数）
    """
    print(f"\n【銘柄コード {code} の過去{months}ヶ月ストップ高検出結果】")
    print("=" * 70)
    
    if stop_high_df.empty:
        print(f"過去{months}ヶ月間でストップ高をつけた日はありませんでした。")
        print("=" * 70)
        return
    
    print(f"ストップ高回数: {len(stop_high_df)} 回")
    print()
    
    # ストップ高をつけた日の詳細を表示
    print("ストップ高をつけた日:")
    print("-" * 70)
    
    for i, (_, row) in enumerate(stop_high_df.iterrows(), 1):
        date_str = row['Date'].strftime('%Y-%m-%d') if hasattr(row['Date'], 'strftime') else str(row['Date'])
        high = row.get('High', 'N/A')
        close = row.get('Close', 'N/A')
        prev_close = row.get('前日終値', 'N/A')
        rate = row.get('前日比上昇率(%)', 'N/A')
        
        print(f"  {i:2d}. {date_str}")
        print(f"      高値: {high:,.0f}円" if isinstance(high, (int, float)) else f"      高値: {high}")
        print(f"      終値: {close:,.0f}円" if isinstance(close, (int, float)) else f"      終値: {close}")
        print(f"      前日終値: {prev_close:,.0f}円" if isinstance(prev_close, (int, float)) else f"      前日終値: {prev_close}")
        print(f"      前日比上昇率: {rate}%" if isinstance(rate, (int, float)) else f"      前日比上昇率: {rate}")
        print()
    
    # 統計情報
    if '前日比上昇率' in stop_high_df.columns:
        rates = stop_high_df['前日比上昇率'].dropna()
        if not rates.empty:
            print("統計情報:")
            print(f"  平均上昇率: {(rates.mean() * 100):.2f}%")
            print(f"  最大上昇率: {(rates.max() * 100):.2f}%")
            print(f"  最小上昇率: {(rates.min() * 100):.2f}%")
            print()
    
    # 最新のストップ高日
    latest_stop_high = stop_high_df.iloc[-1]
    latest_date_str = latest_stop_high['Date'].strftime('%Y-%m-%d') if hasattr(latest_stop_high['Date'], 'strftime') else str(latest_stop_high['Date'])
    print(f"最新ストップ高日: {latest_date_str}")
    
    print("=" * 70)


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
            print("使用方法: python test_40_stop_high_stocks.py <銘柄コード>")
            print("例: python test_40_stop_high_stocks.py 7203")
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
        apikey_file_path = project_root / "apikey.txt"
        
        print("J-Quants API ストップ高検出スクリプト（フェーズ1: 基本実装）")
        print("=" * 70)
        
        # APIキーの読み込み
        print(f"APIキーを読み込み中: {apikey_file_path}")
        api_key = load_api_key(apikey_file_path)
        print("APIキーの読み込み完了")
        
        # 過去3ヶ月の株価データを取得（V2 API直接呼び出し）
        df = get_stock_price_three_months(api_key, stock_code, months=3)
        
        if df.empty:
            print(f"エラー: 銘柄コード {stock_code} の株価データが取得できませんでした")
            sys.exit(1)
        
        # ストップ高の検出
        print(f"\nストップ高を検出中...")
        stop_high_df = detect_stop_high(df, threshold_rate=0.13)
        
        # 結果の表示
        display_stop_high_results(stop_high_df, stock_code, months=3)
        
        # 結果サマリー
        print(f"\n【結果サマリー】")
        print(f"銘柄コード: {stock_code}")
        print(f"対象期間: 過去3ヶ月")
        print(f"取得データ数: {len(df)} 件")
        print(f"ストップ高回数: {len(stop_high_df)} 回")
        
        if not stop_high_df.empty:
            latest_date = stop_high_df.iloc[-1]['Date']
            latest_date_str = latest_date.strftime('%Y-%m-%d') if hasattr(latest_date, 'strftime') else str(latest_date)
            print(f"最新ストップ高日: {latest_date_str}")
        
        print("\n処理が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
