#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
過去3ヶ月でストップ高をつけた銘柄を検出するスクリプト（フェーズ2: 最適化版）

処理内容:
1. 最新取引日の全銘柄株価を一括取得（1回のAPIコール）
2. 600円以下の銘柄をフィルタリング
3. フィルタリングされた銘柄の過去3ヶ月の株価データを取得
4. ストップ高を検出（前日比13%以上上昇した日をストップ高と判定）
5. 結果をCSV出力

使用方法:
    python test_41_high_stocks.py [--min-price MIN] [--max-price MAX] [--delay DELAY] [--max-errors MAX] [--output OUTPUT] [--max-stocks MAX]

例:
    python test_41_high_stocks.py
    python test_41_high_stocks.py --min-price 100 --max-price 600 --delay 0.6
    python test_41_high_stocks.py --max-stocks 100  # テスト用

前提条件:
    - apikey.txtファイルにAPIキー（V2 APIキー）が記述されていること
    - requestsライブラリがインストールされていること
"""

import os
import sys
import time
import argparse
from pathlib import Path
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional


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


def get_stock_list_v2(api_key):
    """
    J-Quants API V2から銘柄一覧を取得する
    
    Args:
        api_key (str): J-Quants APIキー
        
    Returns:
        pandas.DataFrame: 銘柄一覧のデータフレーム
    """
    try:
        # V2 APIエンドポイント
        base_url = "https://api.jquants.com/v2/equities/master"
        
        # ヘッダーにAPIキーを設定
        headers = {
            'X-API-Key': api_key
        }
        
        print("銘柄一覧を取得中...")
        
        # APIリクエスト
        response = requests.get(base_url, headers=headers)
        response.raise_for_status()
        
        # レスポンスをJSONとして取得
        data = response.json()
        
        # V2 APIのレスポンス形式: {"data": [...], "pagination_key": "..."}
        if 'data' not in data or not data['data']:
            print("警告: 銘柄一覧データが見つかりませんでした")
            return pd.DataFrame()
        
        # データフレームに変換
        df = pd.DataFrame(data['data'])
        
        print(f"銘柄一覧を取得しました: {len(df)} 件")
        return df
        
    except requests.exceptions.HTTPError as e:
        error_msg = f"HTTPエラー: {e.response.status_code}"
        if e.response.text:
            try:
                error_data = e.response.json()
                error_msg += f" - {error_data}"
            except:
                error_msg += f" - {e.response.text}"
        raise Exception(f"銘柄一覧の取得中にエラーが発生しました: {error_msg}")
    except Exception as e:
        raise Exception(f"銘柄一覧の取得中にエラーが発生しました: {e}")


def filter_target_markets(df):
    """
    対象市場（プライム、スタンダード、グロース）の銘柄のみをフィルタリングする
    
    Args:
        df (pandas.DataFrame): 銘柄一覧のデータフレーム
        
    Returns:
        pandas.DataFrame: フィルタリングされたデータフレーム
    """
    # V2 APIのカラム名: MktNm（市場区分名）
    market_col = 'MktNm'
    
    if market_col not in df.columns:
        print("警告: MktNm列が見つかりません。全銘柄を対象とします。")
        return df
    
    # 対象市場のリスト
    target_markets = ['プライム', 'スタンダード', 'グロース']
    
    # 市場名に「プライム」「スタンダード」「グロース」を含む銘柄をフィルタリング
    market_filter = df[market_col].str.contains('|'.join(target_markets), na=False)
    filtered_df = df[market_filter].copy()
    
    print(f"市場フィルタリング結果:")
    print(f"  全銘柄数: {len(df)} 件")
    print(f"  対象市場銘柄数: {len(filtered_df)} 件")
    
    return filtered_df


def get_all_stocks_latest_prices(api_key, max_days=7):
    """
    最新取引日の全銘柄株価を一括取得する
    
    Args:
        api_key (str): J-Quants APIキー
        max_days (int): 最大遡り日数（デフォルト: 7日）
        
    Returns:
        tuple: (全銘柄の株価データフレーム, 取引日)
    """
    try:
        # V2 APIエンドポイント
        base_url = "https://api.jquants.com/v2/equities/bars/daily"
        
        # ヘッダーにAPIキーを設定
        headers = {
            'X-API-Key': api_key
        }
        
        # 最新日から順に取得し、データが取得できた時点で終了
        end_date = datetime.now()
        start_date = end_date - timedelta(days=max_days)
        
        print(f"最新取引日の全銘柄株価データを取得中...")
        print(f"  最新日から順に取得を開始（最大{max_days}日間遡ります）...")
        
        current_date = end_date
        while current_date >= start_date:
            date_str = current_date.strftime('%Y%m%d')
            print(f"  日付 {date_str} のデータを取得中...")
            
            try:
                # 日付指定で全銘柄のデータを取得（codeを指定しない）
                params = {
                    'date': date_str
                }
                
                response = requests.get(base_url, headers=headers, params=params)
                response.raise_for_status()
                
                data = response.json()
                
                if 'data' in data and data['data']:
                    df = pd.DataFrame(data['data'])
                    
                    if not df.empty:
                        # V2 APIのカラム名をV1形式に変換
                        column_mapping = {
                            'H': 'High',
                            'C': 'Close',
                            'O': 'Open',
                            'L': 'Low',
                            'Vo': 'Volume'
                        }
                        df = df.rename(columns=column_mapping)
                        
                        print(f"    → {len(df)} 件のデータを取得")
                        print(f"    → 最新日のデータを取得できたため、処理を終了します")
                        return df, current_date
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    # 404エラーは取引日ではない可能性があるので、次の日に遡る
                    print(f"    → 取引日ではない可能性があります（次の日に遡ります）")
                else:
                    print(f"    → エラー: {e.response.status_code} (次の日に遡ります)")
            except Exception as e:
                print(f"    → エラー: {e} (次の日に遡ります)")
            
            current_date -= timedelta(days=1)
            # APIレート制限を考慮して少し待機
            time.sleep(0.2)
        
        print("警告: データが取得できませんでした")
        return pd.DataFrame(), None
        
    except Exception as e:
        raise Exception(f"全銘柄データの取得中にエラーが発生しました: {e}")


def filter_stocks_by_price(price_df, stock_list_df, min_price=100, max_price=600):
    """
    指定価格範囲の銘柄を抽出する
    
    Args:
        price_df (pandas.DataFrame): 全銘柄の株価データフレーム
        stock_list_df (pandas.DataFrame): 銘柄一覧のデータフレーム
        min_price (float): 最小価格（デフォルト: 100円）
        max_price (float): 最大価格（デフォルト: 600円）
        
    Returns:
        List[Dict]: フィルタリングされた銘柄のリスト（code, company_name, marketを含む）
    """
    results = []
    
    if price_df.empty:
        print("エラー: 株価データが空です")
        return results
    
    print(f"\n価格でフィルタリング中...")
    print(f"価格範囲: {min_price:,.0f}円 〜 {max_price:,.0f}円")
    
    # 銘柄コード列を文字列に統一（5桁→4桁に正規化）
    if 'Code' in price_df.columns:
        price_df['Code'] = price_df['Code'].astype(str).str.zfill(5)
        # 5桁の場合は末尾の0を削除して4桁に変換
        price_df['Code'] = price_df['Code'].apply(
            lambda x: x[:-1] if len(x) == 5 and x.endswith('0') else x
        )
    
    # 各銘柄の最新日の終値を取得
    if 'Date' in price_df.columns:
        price_df['Date'] = pd.to_datetime(price_df['Date'])
        price_df_sorted = price_df.sort_values(['Code', 'Date'], ascending=[True, False])
        latest_prices = price_df_sorted.groupby('Code').first().reset_index()
    else:
        latest_prices = price_df.copy()
    
    # 指定価格範囲の銘柄を抽出
    if 'Close' in latest_prices.columns:
        filtered_prices = latest_prices[
            (latest_prices['Close'] >= min_price) & 
            (latest_prices['Close'] <= max_price)
        ].copy()
    else:
        print("警告: Close列が見つかりません")
        return results
    
    print(f"  価格フィルタリング結果: {len(filtered_prices)} 件")
    
    # 銘柄リストと結合して、会社名・市場情報を取得
    if not stock_list_df.empty and 'Code' in stock_list_df.columns:
        stock_list_df['Code'] = stock_list_df['Code'].astype(str).str.zfill(5)
        stock_list_df['Code'] = stock_list_df['Code'].apply(
            lambda x: x[:-1] if len(x) == 5 and x.endswith('0') else x
        )
        
        # V2 APIのカラム名: CoName（会社名）、MktNm（市場区分名）
        company_name_col = 'CoName' if 'CoName' in stock_list_df.columns else None
        market_col = 'MktNm' if 'MktNm' in stock_list_df.columns else None
        
        # 結合に使用するカラムを決定
        merge_columns = ['Code']
        if company_name_col:
            merge_columns.append(company_name_col)
        if market_col:
            merge_columns.append(market_col)
        
        # 株価データと銘柄リストを結合
        available_columns = [col for col in merge_columns if col in stock_list_df.columns]
        if available_columns:
            merged_df = filtered_prices.merge(
                stock_list_df[available_columns],
                on='Code',
                how='left'
            )
            
            for _, row in merged_df.iterrows():
                results.append({
                    'code': row['Code'],
                    'company_name': row.get(company_name_col, '') if company_name_col else '',
                    'market': row.get(market_col, '') if market_col else '',
                    'latest_price': row.get('Close', None)
                })
        else:
            # 結合できない場合はコードのみ
            for _, row in filtered_prices.iterrows():
                results.append({
                    'code': row['Code'],
                    'company_name': '',
                    'market': '',
                    'latest_price': row.get('Close', None)
                })
    else:
        # 銘柄リストがない場合はコードのみ
        for _, row in filtered_prices.iterrows():
            results.append({
                'code': row['Code'],
                'company_name': '',
                'market': '',
                'latest_price': row.get('Close', None)
            })
    
    return results


def get_stock_price_three_months(api_key, code, months=3):
    """
    J-Quants API V2から指定された銘柄の過去3ヶ月の株価データを取得する
    
    Args:
        api_key (str): J-Quants APIキー
        code (str): 銘柄コード
        months (int): 取得する月数（デフォルト: 3）
        
    Returns:
        pandas.DataFrame: 株価データのデータフレーム（Date, High, Close列を含む）
    """
    try:
        # 過去Nヶ月の日付範囲を設定
        end_date = datetime.now()
        start_date = end_date - timedelta(days=months * 30)  # 約Nヶ月前
        
        end_date_str = end_date.strftime('%Y%m%d')
        start_date_str = start_date.strftime('%Y%m%d')
        
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
            return pd.DataFrame()
        
        # データフレームに変換
        df = pd.DataFrame(data['data'])
        
        if df.empty:
            return df
        
        # V2 APIのカラム名をV1形式に変換（互換性のため）
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
        
        return df
        
    except requests.exceptions.HTTPError as e:
        # 404エラーなどは空のDataFrameを返す
        if e.response.status_code == 404:
            return pd.DataFrame()
        raise Exception(f"株価データの取得中にエラーが発生しました: {e.response.status_code}")
    except Exception as e:
        raise Exception(f"株価データの取得中にエラーが発生しました: {e}")


def detect_stop_high(df, threshold_rate=0.13):
    """
    株価データからストップ高を検出する
    
    ストップ高の判定: 前日比で一定率（デフォルト13%）以上上昇した日をストップ高と判定
    
    Args:
        df (pandas.DataFrame): 日次株価データ（Date, High, Close, Open列を含む）
        threshold_rate (float): ストップ高判定の閾値（デフォルト: 0.13 = 13%）
        
    Returns:
        dict: ストップ高検出結果（回数、最新日、最新価格、追加判定項目など）
    """
    if df.empty or 'High' not in df.columns or 'Close' not in df.columns:
        return {
            'count': 0,
            'latest_date': None,
            'latest_price': None,
            'prev_day_stop_high': False,
            'closed_at_stop_high': False,
            'opening_stop_high': False
        }
    
    # 必要な列が存在するか確認
    required_columns = ['Date', 'High', 'Close']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        return {
            'count': 0,
            'latest_date': None,
            'latest_price': None,
            'prev_day_stop_high': False,
            'closed_at_stop_high': False,
            'opening_stop_high': False
        }
    
    # 前日の終値を計算（shift(1)で1行ずらす）
    df = df.copy()
    df['前日終値'] = df['Close'].shift(1)
    
    # 前日比上昇率を計算: (当日高値 - 前日終値) / 前日終値
    df['前日比上昇率'] = (df['High'] - df['前日終値']) / df['前日終値']
    
    # 終値の前日比上昇率も計算（ストップ高で終わったかの判定用）
    df['終値前日比上昇率'] = (df['Close'] - df['前日終値']) / df['前日終値']
    
    # ストップ高の判定: 前日比上昇率 >= 閾値
    df['ストップ高'] = df['前日比上昇率'] >= threshold_rate
    
    # ストップ高をつけた日を抽出
    stop_high_df = df[df['ストップ高'] == True].copy()
    
    if stop_high_df.empty:
        return {
            'count': 0,
            'latest_date': None,
            'latest_price': None,
            'prev_day_stop_high': False,
            'closed_at_stop_high': False,
            'opening_stop_high': False
        }
    
    # 最新のストップ高日を取得
    latest_stop_high = stop_high_df.iloc[-1]
    latest_date = latest_stop_high['Date']
    latest_price = latest_stop_high.get('High', None)
    
    # 元のデータフレームでのインデックスを取得
    latest_date_str = latest_date.strftime('%Y-%m-%d') if hasattr(latest_date, 'strftime') else str(latest_date)
    latest_idx_in_df = df[df['Date'] == latest_date].index
    if len(latest_idx_in_df) > 0:
        latest_idx = latest_idx_in_df[0]
    else:
        latest_idx = None
    
    # 直前の取引日もストップ高だったか
    prev_day_stop_high = False
    if latest_idx is not None and latest_idx > 0:
        prev_row = df.iloc[latest_idx - 1]
        prev_day_stop_high = prev_row.get('ストップ高', False)
    
    # ストップ高で終わったか（終値が前日比13%以上上昇している）
    closed_at_stop_high = latest_stop_high.get('終値前日比上昇率', 0) >= threshold_rate
    
    # 寄り付きストップ高（始値=終値 かつ ストップ高）
    opening_stop_high = False
    if 'Open' in df.columns:
        latest_open = latest_stop_high.get('Open', None)
        latest_close = latest_stop_high.get('Close', None)
        if latest_open is not None and latest_close is not None:
            # 始値と終値が一致（または非常に近い）かつストップ高
            opening_stop_high = abs(latest_open - latest_close) < 0.01 and latest_stop_high.get('ストップ高', False)
    
    return {
        'count': len(stop_high_df),
        'latest_date': latest_date,
        'latest_price': latest_price,
        'prev_day_stop_high': prev_day_stop_high,
        'closed_at_stop_high': closed_at_stop_high,
        'opening_stop_high': opening_stop_high
    }


def save_results_to_csv(results: List[Dict], output_path: Path):
    """
    結果をCSVファイルに保存する
    
    Args:
        results (List[Dict]): ストップ高検出結果のリスト
        output_path (Path): 出力ファイルのパス
    """
    if not results:
        print("警告: 保存するデータがありません")
        return
    
    # 出力ディレクトリを作成
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # データフレームに変換
    df = pd.DataFrame(results)
    
    # 列の順序を指定
    columns_order = [
        '銘柄コード',
        '銘柄名',
        '市場',
        'ストップ高回数',
        '最新ストップ高日',
        '最新ストップ高価格',
        '最新終値',
        '直前取引日もストップ高',
        'ストップ高で終了',
        '寄り付きストップ高'
    ]
    
    # 存在する列のみを使用
    available_columns = [col for col in columns_order if col in df.columns]
    df = df[available_columns]
    
    # CSVファイルに保存（既存ファイルは上書き、ダブルクオートで囲む）
    df.to_csv(output_path, index=False, encoding='utf-8-sig', quoting=1)
    
    print(f"\n結果をCSVファイルに保存しました: {output_path}")
    print(f"保存した件数: {len(df)} 件")


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(
        description='過去3ヶ月でストップ高をつけた銘柄を検出するスクリプト（フェーズ2: 最適化版）',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--min-price',
        type=float,
        default=100.0,
        help='最小価格（デフォルト: 100円）'
    )
    
    parser.add_argument(
        '--max-price',
        type=float,
        default=600.0,
        help='最大価格（デフォルト: 600円）'
    )
    
    parser.add_argument(
        '--delay',
        type=float,
        default=0.6,
        help='API呼び出し間隔（秒）（デフォルト: 0.6）'
    )
    
    parser.add_argument(
        '--max-errors',
        type=int,
        default=10,
        help='最大エラー許容数（デフォルト: 10）'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='出力CSVファイルパス（デフォルト: 自動生成）'
    )
    
    parser.add_argument(
        '--max-stocks',
        type=int,
        default=None,
        help='テスト用の最大処理銘柄数（デフォルト: 全銘柄）'
    )
    
    args = parser.parse_args()
    
    try:
        # スクリプトのディレクトリを取得
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        apikey_file_path = project_root / "apikey.txt"
        
        print("J-Quants API ストップ高検出スクリプト（フェーズ2: 最適化版）")
        print("=" * 80)
        print(f"価格範囲: {args.min_price:,.0f}円 〜 {args.max_price:,.0f}円")
        print(f"API呼び出し間隔: {args.delay}秒")
        print(f"最大エラー許容数: {args.max_errors}")
        if args.max_stocks:
            print(f"テストモード: 最大処理銘柄数 {args.max_stocks} 件")
        print("=" * 80)
        
        # APIキーの読み込み
        print(f"\nAPIキーを読み込み中: {apikey_file_path}")
        api_key = load_api_key(apikey_file_path)
        print("APIキーの読み込み完了")
        
        # ステップ1: 銘柄一覧を取得
        print("\n【ステップ1】銘柄一覧を取得中...")
        stock_list_df = get_stock_list_v2(api_key)
        if stock_list_df.empty:
            print("エラー: 銘柄一覧が取得できませんでした")
            sys.exit(1)
        
        # 対象市場でフィルタリング
        stock_list_df = filter_target_markets(stock_list_df)
        if stock_list_df.empty:
            print("エラー: 対象市場の銘柄が見つかりませんでした")
            sys.exit(1)
        
        # ステップ2: 最新取引日の全銘柄株価を一括取得
        print("\n【ステップ2】最新取引日の全銘柄株価を一括取得中...")
        price_df, trade_date = get_all_stocks_latest_prices(api_key, max_days=7)
        if price_df.empty:
            print("エラー: 株価データが取得できませんでした")
            sys.exit(1)
        
        print(f"取得した取引日: {trade_date.strftime('%Y-%m-%d') if trade_date else 'N/A'}")
        
        # ステップ3: 価格でフィルタリング
        print("\n【ステップ3】価格でフィルタリング中...")
        filtered_stocks = filter_stocks_by_price(price_df, stock_list_df, min_price=args.min_price, max_price=args.max_price)
        
        if not filtered_stocks:
            print("エラー: 条件に合致する銘柄が見つかりませんでした")
            sys.exit(1)
        
        print(f"フィルタリング結果: {len(filtered_stocks)} 銘柄")
        
        # テスト用に銘柄数を制限
        if args.max_stocks and args.max_stocks < len(filtered_stocks):
            filtered_stocks = filtered_stocks[:args.max_stocks]
            print(f"テストモード: 処理銘柄数を {args.max_stocks} 件に制限しました")
        
        # ステップ4: 各銘柄のストップ高を検出
        print(f"\n【ステップ4】各銘柄のストップ高を検出中...")
        print(f"処理対象銘柄数: {len(filtered_stocks)} 件")
        print("-" * 80)
        
        results = []
        error_count = 0
        start_time = datetime.now()
        
        for i, stock_info in enumerate(filtered_stocks, 1):
            code = stock_info['code']
            print(f"[{i}/{len(filtered_stocks)}] 処理中: {code} ({stock_info.get('company_name', '')})")
            
            try:
                # 過去3ヶ月の株価データを取得
                df = get_stock_price_three_months(api_key, code, months=3)
                
                if df.empty:
                    print(f"  → データなし")
                    continue
                
                # ストップ高を検出
                stop_high_result = detect_stop_high(df, threshold_rate=0.13)
                
                if stop_high_result['count'] > 0:
                    # 最新終値を取得
                    latest_close = df.iloc[-1].get('Close', None) if not df.empty else None
                    
                    results.append({
                        '銘柄コード': code,
                        '銘柄名': stock_info.get('company_name', ''),
                        '市場': stock_info.get('market', ''),
                        'ストップ高回数': stop_high_result['count'],
                        '最新ストップ高日': stop_high_result['latest_date'].strftime('%Y-%m-%d') if stop_high_result['latest_date'] else '',
                        '最新ストップ高価格': stop_high_result['latest_price'],
                        '最新終値': latest_close,
                        '直前取引日もストップ高': '○' if stop_high_result.get('prev_day_stop_high', False) else '×',
                        'ストップ高で終了': '○' if stop_high_result.get('closed_at_stop_high', False) else '×',
                        '寄り付きストップ高': '○' if stop_high_result.get('opening_stop_high', False) else '×'
                    })
                    print(f"  → ストップ高検出: {stop_high_result['count']} 回")
                else:
                    print(f"  → ストップ高なし")
                
                # 進捗表示（10件ごと）
                if i % 10 == 0:
                    elapsed_time = datetime.now() - start_time
                    print(f"  進捗: {i}/{len(filtered_stocks)} 件完了 (経過時間: {elapsed_time})")
                
                # APIレート制限対策
                if i < len(filtered_stocks):
                    time.sleep(args.delay)
                
            except Exception as e:
                error_count += 1
                print(f"  → エラー: {e}")
                
                # エラー数が上限に達した場合は処理を停止
                if error_count >= args.max_errors:
                    print(f"エラー数が上限（{args.max_errors}）に達しました。処理を停止します。")
                    break
                
                # エラー時も少し待機
                if i < len(filtered_stocks):
                    time.sleep(args.delay)
        
        # ステップ5: 結果をCSV出力
        if results:
            print(f"\n【ステップ5】結果をCSV出力中...")
            
            # 出力ファイルパスの決定
            if args.output:
                output_path = Path(args.output)
            else:
                output_dir = project_root / "data" / "stop_high"
                date_str = datetime.now().strftime('%Y%m%d')
                output_path = output_dir / f"stop_high_stocks_{date_str}.csv"
            
            save_results_to_csv(results, output_path)
        else:
            print("\nストップ高をつけた銘柄は見つかりませんでした。")
        
        # 結果サマリー
        end_time = datetime.now()
        total_time = end_time - start_time
        
        print("\n" + "=" * 80)
        print("【処理完了】")
        print("=" * 80)
        print(f"処理対象銘柄数: {len(filtered_stocks)} 件")
        print(f"ストップ高検出銘柄数: {len(results)} 件")
        print(f"エラー数: {error_count} 件")
        print(f"処理時間: {total_time}")
        print("=" * 80)
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
