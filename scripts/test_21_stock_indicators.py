#!/usr/bin/env python3
"""
J-Quants APIを使用して特定銘柄の各種指標を計算するスクリプト

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
    python test_21_stock_indicators.py <銘柄コード>

例:
    python test_21_stock_indicators.py 7203  # トヨタ自動車
    python test_21_stock_indicators.py 6758  # ソニーグループ

前提条件:
    - token.txtファイルにAPIキー（リフレッシュトークン）が記述されていること
    - jquants-api-clientライブラリがインストールされていること
"""

import os
import sys
import argparse
import csv
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Optional
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


def get_stock_price_data(client, code, days=50):
    """
    J-Quants APIから指定された銘柄の株価データを取得する
    
    Args:
        client: J-Quants APIクライアント
        code (str): 銘柄コード
        days (int): 取得する日数（デフォルト50日、土日祝日を考慮して多めに設定）
        
    Returns:
        pandas.DataFrame: 株価データのデータフレーム（日付昇順でソート済み）
        
    Raises:
        Exception: API接続エラー
    """
    try:
        # 日付範囲を設定（過去指定日数間）
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
        
        print(f"銘柄コード {code} の株価データを取得中...")
        print(f"期間: {start_date} ～ {end_date}（{days}日間）")
        
        # 株価データの取得（1回のAPI呼び出し）
        price_data = client.get_prices_daily_quotes(
            code=code,
            from_yyyymmdd=start_date,
            to_yyyymmdd=end_date
        )
        
        # データフレームに変換
        df = pd.DataFrame(price_data)
        
        if df.empty:
            raise ValueError(f"銘柄コード {code} のデータが見つかりませんでした")
        
        # 日付でソート（昇順：古い日付から新しい日付へ）
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.sort_values('Date', ascending=True).reset_index(drop=True)
        
        print(f"データ取得完了: {len(df)} 件（取引日のみ）")
        if len(df) < 21:
            print(f"警告: 高値ブレイク判定には21日分のデータが必要ですが、{len(df)}件しか取得できていません。")
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
        print(f"警告: 必要な列が見つかりません: {missing_columns}")
        print(f"利用可能な列: {df.columns.tolist()}")
        raise ValueError(f"必要な列が見つかりません: {missing_columns}")
    
    latest_close_raw = latest_row[close_col]
    latest_open_raw = latest_row[open_col]
    
    # 数値型に変換
    latest_close = float(latest_close_raw) if pd.notna(latest_close_raw) else None
    latest_open = float(latest_open_raw) if pd.notna(latest_open_raw) else None
    
    # データの妥当性チェック
    if latest_open is None:
        print(f"警告: 始値がNaNです。列名: {open_col}, 利用可能な列: {df.columns.tolist()}")
    if latest_close is None:
        print(f"警告: 終値がNaNです。列名: {close_col}, 利用可能な列: {df.columns.tolist()}")
    
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
        print(f"警告: 売買代金の列が見つかりません。利用可能な列: {df.columns.tolist()}")
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
            print(f"警告: 高値ブレイク判定に必要なデータが不足しています（終値: {latest_close}, 過去20日最高値: {max_high_20days}）")
            results['is_high_breakout'] = None
            results['max_high_20days'] = None
    elif len(df) >= 2:  # データが不足しているが、最低限のデータがある場合
        # 利用可能な最大日数で判定を試みる
        available_days = len(df) - 1  # 最新日を除く
        past_days = df.iloc[-(available_days+1):-1]  # 最新日を除く過去N日
        max_high_available = past_days[high_col].max()
        
        print(f"警告: データが不足しています（{len(df)}件）。過去{available_days}日間のデータで判定します。")
        if latest_close is not None and pd.notna(max_high_available):
            is_breakout = latest_close > max_high_available
            # numpy.bool_を通常のboolに変換
            results['is_high_breakout'] = bool(is_breakout)
            results['max_high_20days'] = float(max_high_available)
        else:
            print(f"警告: 高値ブレイク判定に必要なデータが不足しています（終値: {latest_close}, 過去{available_days}日最高値: {max_high_available}）")
            results['is_high_breakout'] = None
            results['max_high_20days'] = None
    else:
        print(f"エラー: データが不足しています（{len(df)}件）。高値ブレイク判定には最低2日分のデータが必要です。")
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
    
    # 6. ボラティリティ判定（ATR率 >= 3.0）
    if results['atr_percent'] is not None:
        results['is_volatility_high'] = results['atr_percent'] >= 3.0
    else:
        results['is_volatility_high'] = None
    
    # 7. 上昇トレンド判定（終値 >= 5日MA >= 25日MA）
    if all(x is not None for x in [results['close'], results['ma5'], results['ma25']]):
        results['is_uptrend'] = (results['close'] >= results['ma5']) and (results['ma5'] >= results['ma25'])
    else:
        results['is_uptrend'] = None
    
    # 8. 陽線引け判定（当日始値 < 当日終値）
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


def display_results(results: Dict):
    """
    結果をコンソールに表示する
    
    Args:
        results (Dict): 計算結果の辞書
    """
    if results is None:
        print("エラー: 結果が取得できませんでした")
        return
    
    print("\n" + "=" * 80)
    print(f"銘柄コード: {results['code']}")
    print(f"日付: {results['date']}")
    print("=" * 80)
    
    print(f"\n【最新終値】")
    if results['close'] is not None:
        print(f"  {results['close']:,.0f}円")
    else:
        print("  データなし")
    
    print(f"\n【直近5日平均売買代金】")
    if results['avg_turnover_5days'] is not None:
        print(f"  {results['avg_turnover_5days']:,.0f}円")
    else:
        print("  データなし")
    
    print(f"\n【ATR%】")
    if results['atr_percent'] is not None:
        print(f"  {results['atr_percent']:.2f}%")
    else:
        print("  データなし")
    
    print(f"\n【高値ブレイク】")
    is_breakout = results.get('is_high_breakout')
    if is_breakout is True:
        print(f"  {format_boolean_result(is_breakout)}（過去20日間の最高値: {results['max_high_20days']:,.0f}円 < 当日終値: {results['close']:,.0f}円）")
    elif is_breakout is False:
        print(f"  {format_boolean_result(is_breakout)}（過去20日間の最高値: {results['max_high_20days']:,.0f}円 >= 当日終値: {results['close']:,.0f}円）")
    else:
        print(f"  {format_boolean_result(is_breakout)}（データ不足）")
    
    print(f"\n【5日MA】")
    if results['ma5'] is not None:
        print(f"  {results['ma5']:,.0f}円")
    else:
        print("  データなし")
    
    print(f"\n【25日MA】")
    if results['ma25'] is not None:
        print(f"  {results['ma25']:,.0f}円")
    else:
        print("  データなし")
    
    print(f"\n【ボラティリティ判定（ATR率 >= 3.0）】")
    print(f"  {format_boolean_result(results.get('is_volatility_high'))}")
    if results.get('atr_percent') is not None:
        print(f"  （ATR率: {results['atr_percent']:.2f}%）")
    
    print(f"\n【上昇トレンド判定（終値 >= 5日MA >= 25日MA）】")
    print(f"  {format_boolean_result(results.get('is_uptrend'))}")
    if all(x is not None for x in [results.get('close'), results.get('ma5'), results.get('ma25')]):
        print(f"  （終値: {results['close']:,.0f}円, 5日MA: {results['ma5']:,.0f}円, 25日MA: {results['ma25']:,.0f}円）")
    
    print(f"\n【陽線引け判定（当日始値 < 当日終値）】")
    print(f"  {format_boolean_result(results.get('is_positive_candle'))}")
    
    print("\n" + "=" * 80)


def save_batch_results_to_csv(all_results: list, output_filepath: Path) -> Optional[str]:
    """
    複数の結果を1つのCSVファイルに保存する
    
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
            
            # ヘッダー
            writer.writerow([
                '銘柄コード',
                '日付',
                '終値',
                '直近5日平均売買代金',
                'ATR%',
                '高値ブレイク',
                '過去20日間最高値',
                '5日MA',
                '25日MA',
                'ボラティリティ判定',
                '上昇トレンド判定',
                '陽線引け判定'
            ])
            
            # データ行
            for results in all_results:
                if results is None:
                    continue
                writer.writerow([
                    results['code'],
                    results['date'],
                    int(results['close']) if results['close'] is not None else '',
                    int(results['avg_turnover_5days']) if results['avg_turnover_5days'] is not None else '',
                    f"{results['atr_percent']:.2f}" if results['atr_percent'] is not None else '',
                    format_boolean_result(results.get('is_high_breakout')),
                    int(results['max_high_20days']) if results['max_high_20days'] is not None else '',
                    int(results['ma5']) if results['ma5'] is not None else '',
                    int(results['ma25']) if results['ma25'] is not None else '',
                    format_boolean_result(results.get('is_volatility_high')),
                    format_boolean_result(results.get('is_uptrend')),
                    format_boolean_result(results.get('is_positive_candle'))
                ])
        
        print(f"\nCSVファイルを保存しました: {output_filepath}")
        return str(output_filepath)
        
    except Exception as e:
        print(f"エラー: CSVファイルの保存中にエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return None


def save_to_csv(results: Dict, output_dir: Path, code: str) -> Optional[str]:
    """
    結果をCSVファイルに保存する
    
    Args:
        results (Dict): 計算結果の辞書
        output_dir (Path): 出力ディレクトリのパス
        code (str): 銘柄コード
        
    Returns:
        Optional[str]: 保存したファイルパス（エラー時はNone）
    """
    if results is None:
        print("保存するデータがありません。")
        return None
    
    try:
        # 出力ディレクトリを作成
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # ファイル名を生成（銘柄コード_YYYY_M_DD.csv）
        today = datetime.now()
        filename = f"{code}_{today.year}_{today.month:02d}_{today.day:02d}.csv"
        filepath = output_dir / filename
        
        # CSVファイルに書き込み
        with open(filepath, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            
            # ヘッダー
            writer.writerow([
                '銘柄コード',
                '日付',
                '終値',
                '直近5日平均売買代金',
                'ATR%',
                '高値ブレイク',
                '過去20日間最高値',
                '5日MA',
                '25日MA',
                'ボラティリティ判定',
                '上昇トレンド判定',
                '陽線引け判定'
            ])
            
            # データ行
            writer.writerow([
                results['code'],
                results['date'],
                int(results['close']) if results['close'] is not None else '',
                int(results['avg_turnover_5days']) if results['avg_turnover_5days'] is not None else '',
                f"{results['atr_percent']:.2f}" if results['atr_percent'] is not None else '',
                format_boolean_result(results.get('is_high_breakout')),
                int(results['max_high_20days']) if results['max_high_20days'] is not None else '',
                int(results['ma5']) if results['ma5'] is not None else '',
                int(results['ma25']) if results['ma25'] is not None else '',
                format_boolean_result(results.get('is_volatility_high')),
                format_boolean_result(results.get('is_uptrend')),
                format_boolean_result(results.get('is_positive_candle'))
            ])
        
        print(f"\nCSVファイルを保存しました: {filepath}")
        return str(filepath)
        
    except Exception as e:
        print(f"エラー: CSVファイルの保存中にエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return None


def read_stock_codes_from_csv(csv_path: Path) -> list:
    """
    CSVファイルから銘柄コードを読み込む
    
    Args:
        csv_path (Path): CSVファイルのパス
        
    Returns:
        list: 銘柄コードのリスト
    """
    codes = []
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # 銘柄コード列を探す（複数の可能性に対応）
                code = None
                for col_name in ['銘柄コード', 'code', 'Code', 'CODE']:
                    if col_name in row:
                        code = row[col_name].strip()
                        break
                
                if code:
                    # 銘柄コードの正規化（4桁に統一）
                    if len(code) == 5 and code.endswith('0'):
                        code = code[:-1]  # 末尾の0を削除
                    codes.append(code)
        
        return codes
    except Exception as e:
        raise Exception(f"CSVファイルの読み込み中にエラーが発生しました: {e}")


def process_csv_file(csv_path: Path, client, days: int) -> list:
    """
    CSVファイルに記載された銘柄に対して判定を実行する
    
    Args:
        csv_path (Path): CSVファイルのパス
        client: J-Quants APIクライアント
        days (int): 取得する日数
        
    Returns:
        list: 計算結果の辞書のリスト
    """
    # 銘柄コードを読み込む
    codes = read_stock_codes_from_csv(csv_path)
    
    if not codes:
        raise ValueError("CSVファイルに銘柄コードが見つかりませんでした")
    
    print(f"CSVファイルから {len(codes)} 件の銘柄コードを読み込みました")
    
    all_results = []
    
    for idx, code in enumerate(codes, 1):
        print(f"\n[{idx}/{len(codes)}] 銘柄コード {code} を処理中...")
        try:
            # 株価データの取得
            df = get_stock_price_data(client, code, days=days)
            
            # 指標の計算
            results = calculate_indicators(df, code)
            
            if results is None:
                print(f"警告: 銘柄コード {code} の指標計算に失敗しました")
                continue
            
            all_results.append(results)
            
        except Exception as e:
            print(f"エラー: 銘柄コード {code} の処理中にエラーが発生しました: {e}")
            continue
    
    return all_results


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(
        description='特定銘柄の各種指標を計算するスクリプト'
    )
    parser.add_argument(
        'input',
        type=str,
        help='銘柄コード（例: 7203）またはCSVファイルパス（例: data/one_shot_trade/2025_12_06.csv）'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=50,
        help='取得する日数（デフォルト: 50日、土日祝日を考慮して多めに設定）'
    )
    parser.add_argument(
        '--no-csv',
        action='store_true',
        help='CSVファイルを保存しない'
    )
    
    args = parser.parse_args()
    
    input_path = Path(args.input)
    
    # 入力がCSVファイルか銘柄コードかを判定
    is_csv_file = input_path.exists() and input_path.suffix.lower() == '.csv'
    
    try:
        # スクリプトのディレクトリを取得
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        token_file_path = project_root / "token.txt"
        
        print("J-Quants API 銘柄指標計算スクリプト")
        print("=" * 80)
        
        # APIキーの読み込み
        print(f"APIキーを読み込み中: {token_file_path}")
        refresh_token = load_api_key(token_file_path)
        print("APIキーの読み込み完了")
        
        # クライアントの初期化
        client = jquantsapi.Client(refresh_token=refresh_token)
        
        if is_csv_file:
            # CSVファイルモード
            print(f"\nCSVファイルモード: {input_path}")
            
            # 各銘柄に対して判定を実行
            all_results = process_csv_file(input_path, client, days=args.days)
            
            if not all_results:
                print("エラー: 処理できた銘柄がありませんでした")
                sys.exit(1)
            
            print(f"\n{len(all_results)} 件の銘柄の処理が完了しました")
            
            # CSVファイルに保存（オプション）
            if not args.no_csv:
                # 出力ファイル名: <引数ファイル名>_indi.csv
                output_filename = input_path.stem + "_indi.csv"
                output_filepath = input_path.parent / output_filename
                
                csv_path = save_batch_results_to_csv(all_results, output_filepath)
                
                if csv_path:
                    print(f"\n処理が完了しました。CSVファイル: {csv_path}")
                else:
                    print("\n処理が完了しました。")
            else:
                print("\n処理が完了しました。")
        else:
            # 単一銘柄モード
            # 銘柄コードの正規化（4桁に統一）
            code = str(args.input).strip()
            if len(code) == 5 and code.endswith('0'):
                code = code[:-1]  # 末尾の0を削除
            
            print(f"\n単一銘柄モード: 銘柄コード {code}")
            
            # 株価データの取得（1回のAPI呼び出し）
            df = get_stock_price_data(client, code, days=args.days)
            
            # 指標の計算
            print(f"\n指標を計算中...")
            results = calculate_indicators(df, code)
            
            if results is None:
                print("エラー: 指標の計算に失敗しました")
                sys.exit(1)
            
            # 結果の表示
            display_results(results)
            
            # CSVファイルに保存（オプション）
            if not args.no_csv:
                output_dir = project_root / "data" / "stock_indicators"
                csv_path = save_to_csv(results, output_dir, code)
                
                if csv_path:
                    print(f"\n処理が完了しました。CSVファイル: {csv_path}")
                else:
                    print("\n処理が完了しました。")
            else:
                print("\n処理が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

