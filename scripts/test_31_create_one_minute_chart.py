#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tick_chartフォルダ内の歩み値データから1分足データを作成するスクリプト

処理内容:
1. tick_chartフォルダ内のCSVファイルを読み込み
2. 歩み値データを1分足に集約（時間範囲を指定可能）
3. 1分足データ（始値、高値、安値、終値、出来高）をCSVで出力

使用方法:
    python test_31_create_one_minute_chart.py [--input-dir INPUT_DIR] [--start-time START] [--end-time END]

例:
    python test_31_create_one_minute_chart.py
    python test_31_create_one_minute_chart.py --input-dir ../data/tick_chart
    python test_31_create_one_minute_chart.py --start-time 09:00 --end-time 10:00
    python test_31_create_one_minute_chart.py --start-time 09:00:00 --end-time 15:00:00
"""

import os
import sys
import csv
import argparse
from pathlib import Path
from datetime import datetime, time
from typing import Optional, Tuple
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


def load_tick_data(csv_path: Path, start_time: Optional[time] = None, end_time: Optional[time] = None) -> Optional[pd.DataFrame]:
    """
    tick_chartのCSVファイルを読み込んでDataFrameに変換
    
    Args:
        csv_path: CSVファイルのパス
        start_time: 開始時刻（Noneの場合は全時間帯）
        end_time: 終了時刻（Noneの場合は全時間帯）
        
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
                    
                    # 時間範囲のフィルタリング
                    should_include = True
                    if start_time is not None or end_time is not None:
                        should_include = False
                        if start_time is not None and end_time is not None:
                            # 開始時刻と終了時刻が指定されている場合
                            if start_time <= end_time:
                                # 通常の範囲（例: 09:00-15:00）
                                should_include = start_time <= time_obj <= end_time
                            else:
                                # 日をまたぐ範囲（例: 22:00-02:00）
                                should_include = time_obj >= start_time or time_obj <= end_time
                        elif start_time is not None:
                            # 開始時刻のみ指定
                            should_include = time_obj >= start_time
                        elif end_time is not None:
                            # 終了時刻のみ指定
                            should_include = time_obj <= end_time
                    
                    if should_include:
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
        pd.DataFrame: 1分足データ（時刻、高値、始値、終値、安値、出来高、VWAP、SMA5、出来高MA5、価格帯、下落幅、下ヒゲ、実体、抽出条件、当足で勝ち、次足で勝ち、勝ち、最低下落幅）
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
    
    # 各1分間のVWAPを計算（その1分間の価格×出来高の合計 / 出来高の合計）
    def calculate_minute_vwap(group):
        """各1分間のVWAPを計算"""
        if group.empty:
            return np.nan
        price_volume_sum = (group['price'] * group['volume']).sum()
        volume_sum = group['volume'].sum()
        if volume_sum == 0:
            return np.nan
        return price_volume_sum / volume_sum
    
    # resampledの各グループに対してVWAPを計算し、インデックスを保持
    minute_vwap_dict = {}
    for name, group in resampled:
        vwap = calculate_minute_vwap(group)
        minute_vwap_dict[name] = vwap
    
    # one_minute_dataのインデックスに対応するVWAPを取得
    one_minute_data['分VWAP'] = one_minute_data.index.map(lambda x: minute_vwap_dict.get(x, np.nan))
    
    # データが存在する行のみを残す（NaNを除去）
    one_minute_data = one_minute_data.dropna()
    
    # インデックスをリセットして時刻列として追加
    one_minute_data = one_minute_data.reset_index()
    one_minute_data['時刻'] = one_minute_data['datetime'].dt.strftime('%H:%M')
    
    # 累積VWAPを計算（取引開始時からの累積）
    # 累積VWAP = Σ(各1分間のVWAP × 各1分間の出来高) / Σ(各1分間の出来高)
    cumulative_price_volume = (one_minute_data['分VWAP'] * one_minute_data['出来高']).cumsum()
    cumulative_volume = one_minute_data['出来高'].cumsum()
    one_minute_data['VWAP'] = cumulative_price_volume / cumulative_volume
    
    # SMA5（終値の5期間移動平均）を計算
    one_minute_data['SMA5'] = one_minute_data['終値'].rolling(window=5, min_periods=1).mean()
    
    # 出来高移動平均5（出来高の5期間移動平均）を計算
    one_minute_data['出来高MA5'] = one_minute_data['出来高'].rolling(window=5, min_periods=1).mean()
    
    # 価格帯を計算（前の足の終値を100で割って切り捨て、100を掛ける）
    # ROUNDDOWN(前の足の終値/100,0)*100
    prev_close = one_minute_data['終値'].shift(1)  # 前の足の終値
    # 最初の行は前の足がないので、その行自身の終値を使う
    prev_close = prev_close.fillna(one_minute_data['終値'])
    one_minute_data['価格帯'] = (np.floor(prev_close / 100) * 100).astype(int)
    
    # 下落幅を計算（前の足の終値 - 安値）
    one_minute_data['下落幅'] = prev_close - one_minute_data['安値']
    
    # 下ヒゲを計算（MIN(始値,終値) - 安値）
    one_minute_data['下ヒゲ'] = np.minimum(one_minute_data['始値'], one_minute_data['終値']) - one_minute_data['安値']
    
    # 実体を計算（ABS(始値-終値)）
    one_minute_data['実体'] = np.abs(one_minute_data['始値'] - one_minute_data['終値'])
    
    # 抽出条件を計算（IF(前の終値-安値>=MAX(価格帯/100+1,3),"◯","✕")）
    # 前の終値-安値は既に「下落幅」として計算済み
    max_threshold = np.maximum(one_minute_data['価格帯'] / 100 + 1, 3)
    one_minute_data['抽出条件'] = np.where(one_minute_data['下落幅'] >= max_threshold, "◯", "✕")
    
    # 当足で勝ちを計算（IF(終値-安値>=3,"◯","✕")）
    one_minute_data['当足で勝ち'] = np.where(one_minute_data['終値'] - one_minute_data['安値'] >= 3, "◯", "✕")
    
    # 次足で勝ちを計算（IF(AND(次足の安値-安値>=-1,次足の高値-安値>=3),"◯","✕")）
    next_low = one_minute_data['安値'].shift(-1)  # 次足の安値
    next_high = one_minute_data['高値'].shift(-1)  # 次足の高値
    condition1 = (next_low - one_minute_data['安値'] >= -1)  # 次足の安値-安値>=-1
    condition2 = (next_high - one_minute_data['安値'] >= 3)  # 次足の高値-安値>=3
    one_minute_data['次足で勝ち'] = np.where(condition1 & condition2, "◯", "✕")
    
    # 勝ちを計算（当足で勝ちまたは次足で勝ちのどちらかが◯なら◯、どちらも✕なら✕）
    one_minute_data['勝ち'] = np.where(
        (one_minute_data['当足で勝ち'] == "◯") | (one_minute_data['次足で勝ち'] == "◯"),
        "◯", "✕"
    )
    
    # 最低下落幅を計算（価格帯/100+1）
    one_minute_data['最低下落幅'] = one_minute_data['価格帯'] / 100 + 1
    
    # 列の順序を整理（時刻、高値、始値、終値、安値、出来高、VWAP、SMA5、出来高MA5、価格帯、下落幅、下ヒゲ、実体、抽出条件、当足で勝ち、次足で勝ち、勝ち、最低下落幅）
    result = one_minute_data[['時刻', '高値', '始値', '終値', '安値', '出来高', 'VWAP', 'SMA5', '出来高MA5', '価格帯', '下落幅', '下ヒゲ', '実体', '抽出条件', '当足で勝ち', '次足で勝ち', '勝ち', '最低下落幅']].copy()
    
    # 数値を整数または適切な小数に変換
    result['始値'] = result['始値'].astype(float)
    result['高値'] = result['高値'].astype(float)
    result['安値'] = result['安値'].astype(float)
    result['終値'] = result['終値'].astype(float)
    result['出来高'] = result['出来高'].astype(int)
    result['VWAP'] = result['VWAP'].astype(float).round(1)  # 小数点第1位まで
    result['SMA5'] = result['SMA5'].astype(float).round(1)  # 小数点第1位まで
    result['出来高MA5'] = result['出来高MA5'].astype(float).round(1)  # 小数点第1位まで
    result['価格帯'] = result['価格帯'].astype(int)  # 整数
    result['下落幅'] = result['下落幅'].astype(float).round(1)  # 小数点第1位まで
    result['下ヒゲ'] = result['下ヒゲ'].astype(float).round(1)  # 小数点第1位まで
    result['実体'] = result['実体'].astype(float).round(1)  # 小数点第1位まで
    result['最低下落幅'] = result['最低下落幅'].astype(float).round(1)  # 小数点第1位まで
    # 抽出条件、当足で勝ち、次足で勝ち、勝ちは文字列なので変換不要
    
    return result


def process_single_file(csv_path: Path, start_time: Optional[time] = None, end_time: Optional[time] = None) -> bool:
    """
    単一のCSVファイルを処理して1分足データを作成
    
    Args:
        csv_path: CSVファイルのパス
        start_time: 開始時刻（Noneの場合は全時間帯）
        end_time: 終了時刻（Noneの場合は全時間帯）
        
    Returns:
        bool: 処理が成功したかどうか
    """
    # ファイル名から出力ファイル名を生成
    filename = csv_path.stem  # 拡張子なし
    output_filename = f"{filename}_one.csv"
    output_path = csv_path.parent / output_filename
    
    print(f"処理中: {csv_path.name}")
    
    # データを読み込み
    df = load_tick_data(csv_path, start_time, end_time)
    
    if df is None or df.empty:
        print(f"  警告: データが読み込めませんでした")
        return False
    
    # 1分足データを作成
    one_minute_df = create_one_minute_chart(df)
    
    if one_minute_df.empty:
        print(f"  警告: 1分足データが作成できませんでした")
        return False
    
    # CSVファイルに保存（数値列のフォーマットを調整）
    try:
        # 数値列をフォーマット（小数点以下が0の場合は整数として表示）
        df_to_save = one_minute_df.copy()
        float_columns = ['始値', '高値', '安値', '終値', 'VWAP', 'SMA5', '出来高MA5', '下落幅', '下ヒゲ', '実体', '最低下落幅']
        for col in float_columns:
            if col in df_to_save.columns:
                df_to_save[col] = df_to_save[col].apply(
                    lambda x: f"{int(x)}" if pd.notna(x) and isinstance(x, (int, float)) and x == int(x) 
                    else f"{x:.1f}" if pd.notna(x) and isinstance(x, (int, float)) 
                    else x
                )
        
        df_to_save.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"  保存完了: {output_filename} ({len(one_minute_df)}行)")
        return True
    except Exception as e:
        print(f"  エラー: ファイル保存中にエラーが発生しました: {e}")
        return False


def parse_time_string(time_str: str) -> time:
    """
    時間文字列をtimeオブジェクトに変換
    
    Args:
        time_str: 時間文字列（"HH:MM" または "HH:MM:SS"形式）
        
    Returns:
        time: timeオブジェクト
        
    Raises:
        ValueError: パースに失敗した場合
    """
    try:
        # "HH:MM:SS"形式を試す
        return datetime.strptime(time_str, "%H:%M:%S").time()
    except ValueError:
        try:
            # "HH:MM"形式を試す
            return datetime.strptime(time_str, "%H:%M").time()
        except ValueError:
            raise ValueError(f"時間形式が正しくありません: {time_str} (形式: HH:MM または HH:MM:SS)")


def main():
    """メイン処理"""
    # スクリプトのディレクトリを基準にパスを解決
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    default_input_dir = project_root / 'data' / 'tick_chart'
    
    parser = argparse.ArgumentParser(description='tick_chartデータから1分足データを作成')
    parser.add_argument('--input-dir', type=str, default=str(default_input_dir),
                        help='入力ディレクトリ（tick_chartフォルダ）')
    parser.add_argument('--start-time', type=str, default=None,
                        help='開始時刻（形式: HH:MM または HH:MM:SS、例: 09:00）。指定しない場合は全時間帯を処理')
    parser.add_argument('--end-time', type=str, default=None,
                        help='終了時刻（形式: HH:MM または HH:MM:SS、例: 15:00）。指定しない場合は全時間帯を処理')
    
    args = parser.parse_args()
    
    # 時間範囲をパース
    start_time = None
    end_time = None
    
    if args.start_time:
        try:
            start_time = parse_time_string(args.start_time)
        except ValueError as e:
            print(f"エラー: {e}")
            return
    
    if args.end_time:
        try:
            end_time = parse_time_string(args.end_time)
        except ValueError as e:
            print(f"エラー: {e}")
            return
    
    # 時間範囲の表示
    if start_time or end_time:
        time_range_str = ""
        if start_time and end_time:
            time_range_str = f"{start_time.strftime('%H:%M:%S')} 〜 {end_time.strftime('%H:%M:%S')}"
        elif start_time:
            time_range_str = f"{start_time.strftime('%H:%M:%S')} 以降"
        elif end_time:
            time_range_str = f"{end_time.strftime('%H:%M:%S')} 以前"
        print(f"時間範囲: {time_range_str}")
    else:
        print("時間範囲: 全時間帯")
    
    # 相対パスの場合はスクリプトのディレクトリを基準に解決
    input_dir = Path(args.input_dir)
    if not input_dir.is_absolute():
        input_dir = (script_dir / input_dir).resolve()
    
    if not input_dir.exists():
        print(f"エラー: 入力ディレクトリが見つかりません: {input_dir}")
        return
    
    # CSVファイルを取得（_one.csvで終わるファイルは除外）
    all_csv_files = list(input_dir.glob("*.csv"))
    csv_files = [f for f in all_csv_files if not f.name.endswith("_one.csv")]
    
    if not csv_files:
        print(f"処理対象のCSVファイルが見つかりません: {input_dir}")
        return
    
    print(f"処理対象ファイル数: {len(csv_files)}")
    print("=" * 80)
    
    # 各ファイルを処理
    success_count = 0
    error_count = 0
    
    for csv_file in csv_files:
        try:
            if process_single_file(csv_file, start_time, end_time):
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

