#!/usr/bin/env python3
"""
J-Quants APIを使用して指定価格帯の銘柄を抽出し、各種指標を計算してCSV出力するスクリプト

処理フロー:
1. 指定価格帯（デフォルト: 3000〜5000円）の銘柄を抽出
2. 各銘柄の技術指標を計算
3. 結果をCSVファイルに出力

計算する指標:
- 直近5日平均売買代金
- ATR%（True Rangeの5日移動平均を終値で割ったもの）
- 高値ブレイク（過去20日間の最高値 < 当日の終値）
- 5日MA（移動平均）
- 25日MA（移動平均）
- ボラティリティ判定（ATR率 >= 3.0）
- 上昇トレンド判定（終値 >= 5日MA >= 25日MA）
- 陽線引け判定（当日始値 < 当日終値）

使用方法:
    python test_22_stock_price_range_with_indicators.py [--min-price MIN] [--max-price MAX]

例:
    python test_22_stock_price_range_with_indicators.py
    python test_22_stock_price_range_with_indicators.py --min-price 3000 --max-price 5000

前提条件:
    - token.txtファイルにAPIキー（リフレッシュトークン）が記述されていること
    - jquants-api-clientライブラリがインストールされていること
"""

import os
import sys
import time
import argparse
import csv
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import jquantsapi
import pandas as pd
import numpy as np


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


def get_stock_list(client):
    """
    J-Quants APIから銘柄一覧を取得する
    
    Args:
        client: J-Quants APIクライアント
        
    Returns:
        pandas.DataFrame: 銘柄一覧のデータフレーム
    """
    try:
        print("銘柄一覧を取得中...")
        stock_list = client.get_listed_info()
        df = pd.DataFrame(stock_list)
        print(f"銘柄一覧を取得しました: {len(df)}件")
        return df
        
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
    if 'MarketCodeName' not in df.columns:
        print("警告: MarketCodeName列が見つかりません。全銘柄を対象とします。")
        return df
    
    # 対象市場のリスト
    target_markets = ['プライム', 'スタンダード', 'グロース']
    
    # 市場名に「プライム」「スタンダード」「グロース」を含む銘柄をフィルタリング
    market_filter = df['MarketCodeName'].str.contains('|'.join(target_markets), na=False)
    filtered_df = df[market_filter].copy()
    
    print(f"\n市場フィルタリング結果:")
    print(f"  全銘柄数: {len(df)}件")
    print(f"  対象市場銘柄数: {len(filtered_df)}件")
    
    # 市場別の銘柄数を表示
    if len(filtered_df) > 0:
        market_counts = filtered_df['MarketCodeName'].value_counts()
        print(f"  市場別銘柄数:")
        for market, count in market_counts.items():
            print(f"    {market}: {count}件")
    
    return filtered_df


def parse_date(date_str: str) -> datetime:
    """
    日付文字列をパースする（YYYY-MM-DD または YYYYMMDD 形式に対応）
    
    Args:
        date_str (str): 日付文字列
        
    Returns:
        datetime: パースされた日付
        
    Raises:
        ValueError: 日付形式が不正な場合
    """
    # YYYY-MM-DD 形式を試す
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        pass
    
    # YYYYMMDD 形式を試す
    try:
        return datetime.strptime(date_str, '%Y%m%d')
    except ValueError:
        pass
    
    raise ValueError(f"日付形式が不正です: {date_str} (YYYY-MM-DD または YYYYMMDD 形式で指定してください)")


def get_all_stocks_prices_for_period(client, days=7, target_date: Optional[datetime] = None):
    """
    J-Quants APIから過去指定日数間の全銘柄の株価データを一括取得する
    
    Args:
        client: J-Quants APIクライアント
        days (int): 取得する日数（デフォルト7日、target_dateが指定されている場合は無視）
        target_date (Optional[datetime]): 指定日付（Noneの場合は最新取引日を自動取得）
        
    Returns:
        tuple: (全銘柄の株価データ, 取引日)
    """
    try:
        if target_date is not None:
            # 指定日付のデータを取得
            print(f"指定日付 {target_date.strftime('%Y-%m-%d')} の全銘柄株価データを取得中...")
            
            # 未来の日付でないかチェック
            if target_date > datetime.now():
                print(f"エラー: 指定日付が未来です: {target_date.strftime('%Y-%m-%d')}")
                return pd.DataFrame(), None
            
            date_str = target_date.strftime('%Y%m%d')
            print(f"  日付 {date_str} のデータを取得中...")
            
            try:
                price_data = client.get_prices_daily_quotes(
                    date_yyyymmdd=date_str
                )
                
                if price_data is not None and not price_data.empty:
                    print(f"    → {len(price_data)} 件のデータを取得")
                    return price_data, target_date
                else:
                    print(f"    → エラー: 指定日付 {target_date.strftime('%Y-%m-%d')} のデータが見つかりませんでした（取引日ではない可能性があります）")
                    return pd.DataFrame(), None
                    
            except Exception as e:
                print(f"    → エラー: {e}")
                return pd.DataFrame(), None
        else:
            # 最新取引日を自動取得（既存処理）
            print(f"過去{days}日間の全銘柄株価データを一括取得中...")
            
            # 日付範囲を設定
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            all_data = []
            latest_trade_date = None
            
            # 最新日から順に取得し、データが取得できた時点で終了
            print(f"  最新日から順に取得を開始（最大{days}日間遡ります）...")
            current_date = end_date
            
            while current_date >= start_date:
                date_str = current_date.strftime('%Y%m%d')
                print(f"  日付 {date_str} のデータを取得中...")
                
                try:
                    # 日付指定で全銘柄のデータを取得（codeを指定しない）
                    price_data = client.get_prices_daily_quotes(
                        date_yyyymmdd=date_str
                    )
                    
                    if price_data is not None and not price_data.empty:
                        all_data.append(price_data)
                        latest_trade_date = current_date
                        print(f"    → {len(price_data)} 件のデータを取得")
                        print(f"    → 最新日のデータを取得できたため、処理を終了します")
                        break  # データが取得できた時点で終了
                    
                except Exception as e:
                    print(f"    → エラー: {e} (次の日に遡ります)")
                
                current_date -= timedelta(days=1)
                # APIレート制限を考慮して少し待機
                time.sleep(0.2)
            
            if not all_data:
                print("警告: データが取得できませんでした")
                return pd.DataFrame(), None
            
            # 全データを結合
            combined_df = pd.concat(all_data, ignore_index=True)
            print(f"\n合計 {len(combined_df)} 件のデータを取得しました")
            
            return combined_df, latest_trade_date
        
    except Exception as e:
        print(f"エラー: 全銘柄データの取得中にエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(), None


def filter_stocks_by_price_range(stock_list_df, price_df, min_price: float, max_price: float):
    """
    株価データから指定価格帯の銘柄を抽出する
    
    Args:
        stock_list_df (pandas.DataFrame): 銘柄一覧のデータフレーム
        price_df (pandas.DataFrame): 全銘柄の株価データ
        min_price (float): 最小価格
        max_price (float): 最大価格
        
    Returns:
        List[Dict]: 条件に合致する銘柄のリスト（code, company_nameを含む）
    """
    results = []
    
    if price_df.empty:
        print("エラー: 株価データが空です")
        return results
    
    print(f"\n価格範囲でフィルタリング中...")
    print(f"価格範囲: {min_price:,.0f}円 〜 {max_price:,.0f}円")
    print("=" * 80)
    
    # 日付列をdatetime型に変換
    if 'Date' in price_df.columns:
        price_df['Date'] = pd.to_datetime(price_df['Date'])
    
    # 銘柄コード列を文字列に統一（5桁→4桁に正規化）
    if 'Code' in price_df.columns:
        price_df['Code'] = price_df['Code'].astype(str).str.zfill(5)
        # 5桁の場合は末尾の0を削除して4桁に変換
        price_df['Code'] = price_df['Code'].apply(
            lambda x: x[:-1] if len(x) == 5 and x.endswith('0') else x
        )
    
    # 各銘柄の最新日の終値を取得
    # 日付でソートして、各銘柄の最新データを取得
    price_df_sorted = price_df.sort_values(['Code', 'Date'], ascending=[True, False])
    latest_prices = price_df_sorted.groupby('Code').first().reset_index()
    
    print(f"最新日のデータがある銘柄数: {len(latest_prices)} 件")
    
    # 銘柄リストと結合して、市場情報を取得
    stock_list_df['Code'] = stock_list_df['Code'].astype(str).str.zfill(5)
    stock_list_df['Code'] = stock_list_df['Code'].apply(
        lambda x: x[:-1] if len(x) == 5 and x.endswith('0') else x
    )
    
    # 株価データと銘柄リストを結合
    merged_df = latest_prices.merge(
        stock_list_df[['Code', 'CompanyName']],
        on='Code',
        how='inner'
    )
    
    # 価格範囲でフィルタリング
    if 'Close' in merged_df.columns:
        filtered_df = merged_df[
            (merged_df['Close'] >= min_price) & 
            (merged_df['Close'] <= max_price)
        ]
        
        # 結果をリスト形式に変換
        for _, row in filtered_df.iterrows():
            results.append({
                'code': str(row['Code']),
                'company_name': row.get('CompanyName', 'N/A'),
            })
    
    print(f"該当銘柄数: {len(results)} 件")
    return results


def get_stock_price_data(client, code, days=50, end_date: Optional[datetime] = None):
    """
    J-Quants APIから指定された銘柄の株価データを取得する
    
    Args:
        client: J-Quants APIクライアント
        code (str): 銘柄コード
        days (int): 取得する日数（デフォルト50日、土日祝日を考慮して多めに設定）
        end_date (Optional[datetime]): 終了日（Noneの場合は現在日）
        
    Returns:
        pandas.DataFrame: 株価データのデータフレーム（日付昇順でソート済み）
        
    Raises:
        Exception: API接続エラー
    """
    try:
        # 終了日を設定
        if end_date is not None:
            end_date_str = end_date.strftime('%Y%m%d')
            start_date = (end_date - timedelta(days=days)).strftime('%Y%m%d')
        else:
            end_date_str = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
        
        # 株価データの取得（1回のAPI呼び出し）
        price_data = client.get_prices_daily_quotes(
            code=code,
            from_yyyymmdd=start_date,
            to_yyyymmdd=end_date_str
        )
        
        # データフレームに変換
        df = pd.DataFrame(price_data)
        
        if df.empty:
            raise ValueError(f"銘柄コード {code} のデータが見つかりませんでした")
        
        # 日付でソート（昇順：古い日付から新しい日付へ）
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.sort_values('Date', ascending=True).reset_index(drop=True)
        
        return df
        
    except Exception as e:
        raise Exception(f"株価データの取得中にエラーが発生しました: {e}")


def calculate_true_range(df, high_col='High', low_col='Low', close_col='Close'):
    """
    True Rangeを計算する
    
    True Range = Max(高値-安値, |高値-前日終値|, |安値-前日終値|)
    
    Args:
        df (pandas.DataFrame): 株価データのデータフレーム
        high_col (str): 高値の列名
        low_col (str): 安値の列名
        close_col (str): 終値の列名
        
    Returns:
        pandas.Series: True Rangeのシリーズ
    """
    # 前日終値を計算（shift(1)で1行シフト）
    prev_close = df[close_col].shift(1)
    
    # True Rangeの各要素を計算
    tr1 = df[high_col] - df[low_col]  # 高値 - 安値
    tr2 = abs(df[high_col] - prev_close)  # |高値 - 前日終値|
    tr3 = abs(df[low_col] - prev_close)   # |安値 - 前日終値|
    
    # 最大値を取る
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
    return true_range


def calculate_indicators(df, code):
    """
    各種指標を計算する
    
    Args:
        df (pandas.DataFrame): 株価データのデータフレーム（日付昇順）
        code (str): 銘柄コード
        
    Returns:
        Dict: 計算結果の辞書
    """
    if df.empty:
        return None
    
    # 最新日（最後の行）を取得
    latest_row = df.iloc[-1]
    latest_date = latest_row['Date']
    
    # 列名の確認（大文字小文字や別名に対応）
    close_col = None
    open_col = None
    high_col = None
    low_col = None
    
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in ['close', 'closingprice', 'closing_price']:
            close_col = col
        elif col_lower in ['open', 'openingprice', 'opening_price']:
            open_col = col
        elif col_lower in ['high', 'highprice', 'high_price']:
            high_col = col
        elif col_lower in ['low', 'lowprice', 'low_price']:
            low_col = col
    
    # デフォルトで標準的な列名を試す
    if close_col is None and 'Close' in df.columns:
        close_col = 'Close'
    if open_col is None and 'Open' in df.columns:
        open_col = 'Open'
    if high_col is None and 'High' in df.columns:
        high_col = 'High'
    if low_col is None and 'Low' in df.columns:
        low_col = 'Low'
    
    # 必要な列の存在確認
    missing_columns = []
    if close_col is None:
        missing_columns.append('Close')
    if open_col is None:
        missing_columns.append('Open')
    if high_col is None:
        missing_columns.append('High')
    if low_col is None:
        missing_columns.append('Low')
    
    if missing_columns:
        raise ValueError(f"必要な列が見つかりません: {missing_columns}")
    
    latest_close_raw = latest_row[close_col]
    latest_open_raw = latest_row[open_col]
    
    # 数値型に変換
    latest_close = float(latest_close_raw) if pd.notna(latest_close_raw) else None
    latest_open = float(latest_open_raw) if pd.notna(latest_open_raw) else None
    
    results = {
        'code': code,
        'date': latest_date.strftime('%Y-%m-%d') if hasattr(latest_date, 'strftime') else str(latest_date),
        'close': latest_close,
    }
    
    # 1. 直近5日平均売買代金
    # 売買代金の列名を確認（TurnoverValue, TradingValue, Turnover等の可能性）
    turnover_columns = ['TurnoverValue', 'TradingValue', 'Turnover', 'TradingAmount']
    turnover_col = None
    for col in turnover_columns:
        if col in df.columns:
            turnover_col = col
            break
    
    if turnover_col:
        # 直近5日のデータを取得
        last_5_days = df.tail(5)
        avg_turnover = last_5_days[turnover_col].mean()
        results['avg_turnover_5days'] = float(avg_turnover) if pd.notna(avg_turnover) else None
    else:
        results['avg_turnover_5days'] = None
    
    # 2. ATR%の計算
    # True Rangeを計算（列名を渡す）
    true_range = calculate_true_range(df, high_col, low_col, close_col)
    df['TrueRange'] = true_range
    
    # ATR = True Rangeの5日移動平均
    atr = true_range.rolling(window=5, min_periods=1).mean()
    df['ATR'] = atr
    
    # 最新日のATR
    latest_atr = atr.iloc[-1]
    
    # ATR% = (ATR / 終値) * 100
    if pd.notna(latest_atr) and pd.notna(latest_close) and latest_close != 0:
        atr_percent = (latest_atr / latest_close) * 100
        results['atr_percent'] = float(atr_percent)
    else:
        results['atr_percent'] = None
    
    # 3. 高値ブレイクの判定
    # 最新日を除く過去20日間の最高値
    if len(df) >= 21:  # 最新日 + 過去20日 = 21日分必要
        past_20_days = df.iloc[-21:-1]  # 最新日を除く過去20日
        max_high_20days = past_20_days[high_col].max()
        
        # 当日の終値 > 過去20日間の最高値
        if latest_close is not None and pd.notna(max_high_20days):
            is_breakout = latest_close > max_high_20days
            # numpy.bool_を通常のboolに変換
            results['is_high_breakout'] = bool(is_breakout)
            results['max_high_20days'] = float(max_high_20days)
        else:
            results['is_high_breakout'] = None
            results['max_high_20days'] = None
    elif len(df) >= 2:  # データが不足しているが、最低限のデータがある場合
        # 利用可能な最大日数で判定を試みる
        available_days = len(df) - 1  # 最新日を除く
        past_days = df.iloc[-(available_days+1):-1]  # 最新日を除く過去N日
        max_high_available = past_days[high_col].max()
        
        if latest_close is not None and pd.notna(max_high_available):
            is_breakout = latest_close > max_high_available
            # numpy.bool_を通常のboolに変換
            results['is_high_breakout'] = bool(is_breakout)
            results['max_high_20days'] = float(max_high_available)
        else:
            results['is_high_breakout'] = None
            results['max_high_20days'] = None
    else:
        results['is_high_breakout'] = None
        results['max_high_20days'] = None
    
    # 4. 5日MA
    ma5 = df[close_col].rolling(window=5, min_periods=1).mean()
    latest_ma5 = ma5.iloc[-1]
    results['ma5'] = float(latest_ma5) if pd.notna(latest_ma5) else None
    
    # 5. 25日MA
    ma25 = df[close_col].rolling(window=25, min_periods=1).mean()
    latest_ma25 = ma25.iloc[-1]
    results['ma25'] = float(latest_ma25) if pd.notna(latest_ma25) else None
    
    # 6. 売買代金判定（直近5日平均売買代金 >= 500,000,000）
    if results['avg_turnover_5days'] is not None:
        results['is_turnover_high'] = results['avg_turnover_5days'] >= 500000000
    else:
        results['is_turnover_high'] = None
    
    # 7. ボラティリティ判定（ATR率 >= 3.0）
    if results['atr_percent'] is not None:
        results['is_volatility_high'] = results['atr_percent'] >= 3.0
    else:
        results['is_volatility_high'] = None
    
    # 8. 上昇トレンド判定（終値 >= 5日MA >= 25日MA）
    if all(x is not None for x in [results['close'], results['ma5'], results['ma25']]):
        results['is_uptrend'] = (results['close'] >= results['ma5']) and (results['ma5'] >= results['ma25'])
    else:
        results['is_uptrend'] = None
    
    # 9. 陽線引け判定（当日始値 < 当日終値）
    if latest_open is not None and latest_close is not None:
        is_positive = latest_open < latest_close
        # numpy.bool_を通常のboolに変換
        results['is_positive_candle'] = bool(is_positive)
    else:
        results['is_positive_candle'] = None
    
    return results


def format_boolean_result(value: Optional[bool]) -> str:
    """
    真偽値を◯/✕形式に変換する
    
    Args:
        value (Optional[bool]): 真偽値（Noneの場合は判定不可）
        
    Returns:
        str: "◯"（True）、"✕"（False）、"判定不可"（None）
    """
    if value is True:
        return "◯"
    elif value is False:
        return "✕"
    else:
        return "判定不可"


def process_stocks_with_indicators(client, stock_list: List[Dict], days: int, target_date: Optional[datetime] = None):
    """
    価格帯でフィルタリングされた銘柄リストに対して指標を計算する
    
    Args:
        client: J-Quants APIクライアント
        stock_list (List[Dict]): 銘柄リスト（code, company_nameを含む）
        days (int): 取得する日数
        target_date (Optional[datetime]): 対象日付（Noneの場合は現在日）
        
    Returns:
        list: 計算結果の辞書のリスト
    """
    all_results = []
    
    for idx, stock in enumerate(stock_list, 1):
        code = stock['code']
        company_name = stock['company_name']
        
        print(f"\n[{idx}/{len(stock_list)}] 銘柄コード {code} ({company_name}) を処理中...")
        try:
            # 株価データの取得
            df = get_stock_price_data(client, code, days=days, end_date=target_date)
            
            # 指標の計算
            results = calculate_indicators(df, code)
            
            if results is None:
                print(f"警告: 銘柄コード {code} の指標計算に失敗しました")
                continue
            
            # 銘柄名を追加
            results['company_name'] = company_name
            
            all_results.append(results)
            
        except Exception as e:
            print(f"エラー: 銘柄コード {code} の処理中にエラーが発生しました: {e}")
            continue
    
    return all_results


def save_results_to_csv(all_results: list, output_filepath: Path) -> Optional[str]:
    """
    結果をCSVファイルに保存する（指定された順序で）
    
    Args:
        all_results (list): 計算結果の辞書のリスト
        output_filepath (Path): 出力ファイルパス
        
    Returns:
        Optional[str]: 保存したファイルパス（エラー時はNone）
    """
    if not all_results:
        print("保存するデータがありません。")
        return None
    
    try:
        # 出力ディレクトリを作成
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
        
        # CSVファイルに書き込み
        with open(output_filepath, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            
            # ヘッダー（指定された順序）
            writer.writerow([
                '日付',
                '銘柄コード',
                '銘柄名',
                '終値',
                '直近5日平均売買代金',
                'ATR%',
                '過去20日間最高値',
                '5日MA',
                '25日MA',
                '売買代金判定',
                'ボラティリティ判定',
                '上昇トレンド判定',
                '高値ブレイク',
                '陽線引け判定'
            ])
            
            # データ行
            for results in all_results:
                if results is None:
                    continue
                writer.writerow([
                    results.get('date', ''),
                    results['code'],
                    results.get('company_name', ''),
                    int(results['close']) if results['close'] is not None else '',
                    int(results['avg_turnover_5days']) if results['avg_turnover_5days'] is not None else '',
                    f"{results['atr_percent']:.2f}" if results['atr_percent'] is not None else '',
                    int(results['max_high_20days']) if results['max_high_20days'] is not None else '',
                    int(results['ma5']) if results['ma5'] is not None else '',
                    int(results['ma25']) if results['ma25'] is not None else '',
                    format_boolean_result(results.get('is_turnover_high')),
                    format_boolean_result(results.get('is_volatility_high')),
                    format_boolean_result(results.get('is_uptrend')),
                    format_boolean_result(results.get('is_high_breakout')),
                    format_boolean_result(results.get('is_positive_candle'))
                ])
        
        print(f"\nCSVファイルを保存しました: {output_filepath}")
        print(f"保存件数: {len(all_results)}件")
        return str(output_filepath)
        
    except Exception as e:
        print(f"エラー: CSVファイルの保存中にエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(
        description='指定価格帯の銘柄を抽出し、各種指標を計算してCSV出力するスクリプト'
    )
    parser.add_argument(
        '--min-price',
        type=float,
        default=3000.0,
        help='最小価格（デフォルト: 3000）'
    )
    parser.add_argument(
        '--max-price',
        type=float,
        default=5000.0,
        help='最大価格（デフォルト: 5000）'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=50,
        help='株価データ取得日数（デフォルト: 50日、土日祝日を考慮して多めに設定）'
    )
    parser.add_argument(
        '--date',
        type=str,
        default=None,
        help='対象日付（YYYY-MM-DD または YYYYMMDD 形式、指定しない場合は最新取引日を自動取得）'
    )
    
    args = parser.parse_args()
    
    # 日付のパース・検証
    target_date = None
    if args.date is not None:
        try:
            target_date = parse_date(args.date)
            # 未来の日付でないかチェック
            if target_date > datetime.now():
                print(f"エラー: 指定日付が未来です: {target_date.strftime('%Y-%m-%d')}")
                sys.exit(1)
            print(f"対象日付: {target_date.strftime('%Y-%m-%d')}")
        except ValueError as e:
            print(f"エラー: {e}")
            sys.exit(1)
    
    # 価格範囲の検証
    if args.min_price < 0 or args.max_price < 0:
        print("エラー: 価格は0以上である必要があります。")
        sys.exit(1)
    
    if args.min_price > args.max_price:
        print("エラー: 最小価格が最大価格を超えています。")
        sys.exit(1)
    
    try:
        # スクリプトのディレクトリを取得
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        token_file_path = project_root / "token.txt"
        
        print("J-Quants API 価格帯別銘柄リスト取得・指標計算スクリプト")
        print("=" * 80)
        
        # APIキーの読み込み
        print(f"APIキーを読み込み中: {token_file_path}")
        refresh_token = load_api_key(token_file_path)
        print("APIキーの読み込み完了")
        
        # クライアントの初期化
        client = jquantsapi.Client(refresh_token=refresh_token)
        
        # 銘柄一覧の取得
        stock_list_df = get_stock_list(client)
        
        # 対象市場（プライム、スタンダード、グロース）の銘柄のみをフィルタリング
        print(f"\n対象市場の銘柄をフィルタリング中...")
        stock_list_df = filter_target_markets(stock_list_df)
        
        if stock_list_df.empty:
            print("エラー: 対象市場の銘柄が見つかりませんでした")
            sys.exit(1)
        
        # 全銘柄株価データを取得
        price_df, trade_date = get_all_stocks_prices_for_period(client, days=7, target_date=target_date)
        
        if price_df.empty:
            print("エラー: 株価データが取得できませんでした")
            sys.exit(1)
        
        if trade_date is None:
            print("エラー: 取引日が取得できませんでした")
            sys.exit(1)
        
        # 価格帯でフィルタリング
        filtered_stocks = filter_stocks_by_price_range(
            stock_list_df,
            price_df,
            args.min_price,
            args.max_price
        )
        
        if not filtered_stocks:
            print("エラー: 価格範囲に該当する銘柄が見つかりませんでした")
            sys.exit(1)
        
        print(f"\n{len(filtered_stocks)} 件の銘柄に対して指標を計算します...")
        
        # 各銘柄の指標を計算
        all_results = process_stocks_with_indicators(
            client,
            filtered_stocks,
            days=args.days,
            target_date=trade_date
        )
        
        if not all_results:
            print("エラー: 処理できた銘柄がありませんでした")
            sys.exit(1)
        
        print(f"\n{len(all_results)} 件の銘柄の処理が完了しました")
        
        # CSVファイルに保存
        output_dir = project_root / "data" / "one_shot_trade"
        
        # ファイル名を生成（<対象日>.csv）
        target_date_str = trade_date.strftime('%Y_%m_%d')
        filename = f"{target_date_str}.csv"
        output_filepath = output_dir / filename
        
        csv_path = save_results_to_csv(all_results, output_filepath)
        
        if csv_path:
            print(f"\n処理が完了しました。CSVファイル: {csv_path}")
        else:
            print("\n処理が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

