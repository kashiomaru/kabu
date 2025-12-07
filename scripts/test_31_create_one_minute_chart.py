#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tick_chartフォルダ内の歩み値データから1分足データを作成するスクリプト

処理内容:
1. tick_chartフォルダ内のCSVファイルを読み込み
2. 各銘柄の9時〜10時の歩み値データを1分足に集約
3. 1分足データ（始値、高値、安値、終値、出来高）をCSVで出力

使用方法:
    python test_31_create_one_minute_chart.py [--input-dir INPUT_DIR]

例:
    python test_31_create_one_minute_chart.py
    python test_31_create_one_minute_chart.py --input-dir ../data/tick_chart
"""

import os
import sys
import csv
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional
import pandas as pd
import numpy as np


def parse_price(price_str: str) -> Optional[float]:
    """
    価格文字列を数値に変換（カンマ区切り対応）
    
    Args:
        price_str: 価格文字列（例: "3,460"）
        
    Returns:
        float: 価格の数値、変換失敗時はNone
    """
    if not price_str or price_str == "":
        return None
    
    try:
        # カンマとダブルクォートを除去
        cleaned = price_str.replace(",", "").replace('"', "").strip()
        return float(cleaned)
    except (ValueError, AttributeError):
        return None


def parse_volume(volume_str: str) -> Optional[int]:
    """
    出来高文字列を数値に変換（カンマ区切り対応）
    
    Args:
        volume_str: 出来高文字列（例: "100" または "1,300"）
        
    Returns:
        int: 出来高の数値、変換失敗時はNone
    """
    if not volume_str or volume_str == "":
        return None
    
    try:
        # カンマとダブルクォートを除去
        cleaned = volume_str.replace(",", "").replace('"', "").strip()
        return int(cleaned)
    except (ValueError, AttributeError):
        return None


def load_tick_data(csv_path: Path) -> Optional[pd.DataFrame]:
    """
    tick_chartのCSVファイルを読み込んでDataFrameに変換
    
    Args:
        csv_path: CSVファイルのパス
        
    Returns:
        pd.DataFrame: 読み込んだデータ、失敗時はNone
    """
    try:
        data = []
        
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader)  # ヘッダー行をスキップ
            
            # 時間列のインデックスを取得
            time_col_idx = None
            price_col_idx = None
            volume_col_idx = None
            
            for idx, col in enumerate(header):
                if '時間' in col:
                    time_col_idx = idx
                elif '約定値' in col or '約定' in col:
                    price_col_idx = idx
                elif '出来高' in col:
                    volume_col_idx = idx
            
            if time_col_idx is None or price_col_idx is None or volume_col_idx is None:
                print(f"  警告: 必要な列が見つかりません。スキップします。")
                return None
            
            # データ行を処理
            for row in reader:
                if len(row) <= max(time_col_idx, price_col_idx, volume_col_idx):
                    continue
                
                time_str = row[time_col_idx].strip()
                price_str = row[price_col_idx].strip()
                volume_str = row[volume_col_idx].strip()
                
                # 時間をパース
                try:
                    time_obj = datetime.strptime(time_str, "%H:%M:%S").time()
                    # 9時〜10時のデータのみを抽出
                    if 9 <= time_obj.hour < 10:
                        price = parse_price(price_str)
                        volume = parse_volume(volume_str)
                        
                        if price is not None and volume is not None:
                            # 日時を結合（日付はファイル名から取得）
                            date_str = csv_path.stem.split('_')[-1]  # 例: "20251203"
                            if len(date_str) == 8:
                                date_obj = datetime.strptime(date_str, "%Y%m%d").date()
                                datetime_obj = datetime.combine(date_obj, time_obj)
                                
                                data.append({
                                    'datetime': datetime_obj,
                                    'time': time_obj,
                                    'price': price,
                                    'volume': volume
                                })
                except ValueError:
                    continue
        
        if not data:
            return None
        
        df = pd.DataFrame(data)
        df = df.sort_values('datetime')
        df = df.reset_index(drop=True)
        
        return df
        
    except Exception as e:
        print(f"  エラー: ファイル読み込み中にエラーが発生しました: {e}")
        return None


def create_one_minute_chart(df: pd.DataFrame) -> pd.DataFrame:
    """
    歩み値データから1分足データを作成
    
    Args:
        df: 歩み値データのDataFrame（datetime, price, volume列が必要）
        
    Returns:
        pd.DataFrame: 1分足データ（時刻、始値、高値、安値、終値、出来高）
    """
    if df.empty:
        return pd.DataFrame()
    
    # datetimeをインデックスに設定
    df_indexed = df.set_index('datetime')
    
    # 1分単位でリサンプリング（個別に集約）
    resampled = df_indexed.resample('1min')
    
    # 各集約値を計算
    one_minute_data = pd.DataFrame({
        '始値': resampled['price'].first(),  # 最初の価格
        '高値': resampled['price'].max(),    # 最高価格
        '安値': resampled['price'].min(),    # 最低価格
        '終値': resampled['price'].last(),   # 最後の価格
        '出来高': resampled['volume'].sum()  # 出来高の合計
    })
    
    # データが存在する行のみを残す（NaNを除去）
    one_minute_data = one_minute_data.dropna()
    
    # インデックスをリセットして時刻列として追加
    one_minute_data = one_minute_data.reset_index()
    one_minute_data['時刻'] = one_minute_data['datetime'].dt.strftime('%H:%M')
    
    # 列の順序を整理（時刻、始値、高値、安値、終値、出来高）
    result = one_minute_data[['時刻', '始値', '高値', '安値', '終値', '出来高']].copy()
    
    # 数値を整数または適切な小数に変換
    result['始値'] = result['始値'].astype(float)
    result['高値'] = result['高値'].astype(float)
    result['安値'] = result['安値'].astype(float)
    result['終値'] = result['終値'].astype(float)
    result['出来高'] = result['出来高'].astype(int)
    
    return result


def process_single_file(csv_path: Path) -> bool:
    """
    単一のCSVファイルを処理して1分足データを作成
    
    Args:
        csv_path: CSVファイルのパス
        
    Returns:
        bool: 処理が成功したかどうか
    """
    # ファイル名から出力ファイル名を生成
    filename = csv_path.stem  # 拡張子なし
    output_filename = f"{filename}_one.csv"
    output_path = csv_path.parent / output_filename
    
    print(f"処理中: {csv_path.name}")
    
    # データを読み込み
    df = load_tick_data(csv_path)
    
    if df is None or df.empty:
        print(f"  警告: データが読み込めませんでした")
        return False
    
    # 1分足データを作成
    one_minute_df = create_one_minute_chart(df)
    
    if one_minute_df.empty:
        print(f"  警告: 1分足データが作成できませんでした")
        return False
    
    # CSVファイルに保存
    try:
        one_minute_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"  保存完了: {output_filename} ({len(one_minute_df)}行)")
        return True
    except Exception as e:
        print(f"  エラー: ファイル保存中にエラーが発生しました: {e}")
        return False


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(description='tick_chartデータから1分足データを作成')
    parser.add_argument('--input-dir', type=str, default='../data/tick_chart',
                        help='入力ディレクトリ（tick_chartフォルダ）')
    
    args = parser.parse_args()
    
    input_dir = Path(args.input_dir)
    
    if not input_dir.exists():
        print(f"エラー: 入力ディレクトリが見つかりません: {input_dir}")
        return
    
    # CSVファイルを取得
    csv_files = list(input_dir.glob("*.csv"))
    
    if not csv_files:
        print(f"CSVファイルが見つかりません: {input_dir}")
        return
    
    print(f"処理対象ファイル数: {len(csv_files)}")
    print("=" * 80)
    
    # 各ファイルを処理
    success_count = 0
    error_count = 0
    
    for csv_file in csv_files:
        try:
            if process_single_file(csv_file):
                success_count += 1
            else:
                error_count += 1
        except Exception as e:
            print(f"  エラー: {e}")
            error_count += 1
    
    print("\n" + "=" * 80)
    print(f"処理完了")
    print(f"成功: {success_count}件, エラー: {error_count}件")


if __name__ == '__main__':
    main()

