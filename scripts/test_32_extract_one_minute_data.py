#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
1分足データから指定時間の情報を抽出してCSV形式で出力するスクリプト

処理内容:
1. 1分足データCSVファイルを読み込み
2. 引数で指定された時間の1分足データを抽出
3. 前の足と次の足のデータも含めて、指定された順序でCSV形式でターミナルに出力

使用方法:
    python test_32_extract_one_minute_data.py CSV_FILE TIME

例:
    python test_32_extract_one_minute_data.py data/tick_chart/2342_20251205_one.csv 09:30
    python test_32_extract_one_minute_data.py data/tick_chart/2342_20251205_one.csv 09:30:00
"""

import sys
import argparse
from pathlib import Path
import pandas as pd
import numpy as np


def parse_time_string(time_str: str) -> str:
    """
    時間文字列を正規化（HH:MM または HH:MM:SS形式をHH:MMに統一）
    
    Args:
        time_str: 時間文字列（"HH:MM" または "HH:MM:SS"形式）
        
    Returns:
        str: 正規化された時間文字列（"HH:MM"形式）
    """
    if ':' in time_str:
        parts = time_str.split(':')
        if len(parts) >= 2:
            return f"{parts[0]}:{parts[1]}"
    return time_str


def extract_one_minute_data(csv_path: Path, target_time: str) -> pd.Series:
    """
    1分足データから指定時間の情報を抽出
    
    Args:
        csv_path: 1分足データCSVファイルのパス
        target_time: 抽出する時間（"HH:MM"形式）
        
    Returns:
        pd.Series: 抽出されたデータ（前の足、現在の足、次の足を含む）
    """
    # CSVファイルを読み込み
    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    
    # 時刻列を確認
    if '時刻' not in df.columns:
        raise ValueError("CSVファイルに'時刻'列が見つかりません")
    
    # 時間を正規化
    normalized_time = parse_time_string(target_time)
    
    # 指定された時間の行を検索
    matching_rows = df[df['時刻'] == normalized_time]
    
    if matching_rows.empty:
        raise ValueError(f"指定された時間 '{target_time}' のデータが見つかりません")
    
    if len(matching_rows) > 1:
        raise ValueError(f"指定された時間 '{target_time}' のデータが複数見つかりました")
    
    # 現在の足のインデックスを取得
    current_idx = matching_rows.index[0]
    
    # 前の足と次の足のデータを取得
    prev_idx = current_idx - 1 if current_idx > 0 else None
    next_idx = current_idx + 1 if current_idx < len(df) - 1 else None
    
    # データを整理
    result_data = {}
    
    # 現在の足のデータ
    current_row = df.loc[current_idx]
    
    # 前の足のデータ
    if prev_idx is not None:
        prev_row = df.loc[prev_idx]
        result_data['前の足の高値'] = prev_row.get('高値', '')
        result_data['前の足の始値'] = prev_row.get('始値', '')
        result_data['前の足の終値'] = prev_row.get('終値', '')
        result_data['前の足の安値'] = prev_row.get('安値', '')
        result_data['前の足の出来高'] = prev_row.get('出来高', '')
    else:
        result_data['前の足の高値'] = ''
        result_data['前の足の始値'] = ''
        result_data['前の足の終値'] = ''
        result_data['前の足の安値'] = ''
        result_data['前の足の出来高'] = ''
    
    # 現在の足のデータ
    result_data['VWAP'] = current_row.get('VWAP', '')
    result_data['SMA5'] = current_row.get('SMA5', '')
    result_data['高値'] = current_row.get('高値', '')
    result_data['始値'] = current_row.get('始値', '')
    result_data['終値'] = current_row.get('終値', '')
    result_data['安値'] = current_row.get('安値', '')
    result_data['出来高'] = current_row.get('出来高', '')
    result_data['出来高MA5'] = current_row.get('出来高MA5', '')
    result_data['価格帯'] = current_row.get('価格帯', '')
    result_data['下落幅'] = current_row.get('下落幅', '')
    result_data['下ヒゲ'] = current_row.get('下ヒゲ', '')
    result_data['実体'] = current_row.get('実体', '')
    result_data['抽出条件'] = current_row.get('抽出条件', '')
    result_data['当足で勝ち'] = current_row.get('当足で勝ち', '')
    result_data['次足で勝ち'] = current_row.get('次足で勝ち', '')
    result_data['勝ち'] = current_row.get('勝ち', '')
    result_data['最低下落幅'] = current_row.get('最低下落幅', '')
    
    # 次の足のデータ
    if next_idx is not None:
        next_row = df.loc[next_idx]
        result_data['次の足の高値'] = next_row.get('高値', '')
        result_data['次の足の始値'] = next_row.get('始値', '')
        result_data['次の足の終値'] = next_row.get('終値', '')
        result_data['次の足の安値'] = next_row.get('安値', '')
        result_data['次の足の出来高'] = next_row.get('出来高', '')
    else:
        result_data['次の足の高値'] = ''
        result_data['次の足の始値'] = ''
        result_data['次の足の終値'] = ''
        result_data['次の足の安値'] = ''
        result_data['次の足の出来高'] = ''
    
    return pd.Series(result_data)


def format_output(data: pd.Series) -> str:
    """
    データを指定された順序でCSV形式の文字列に変換
    
    Args:
        data: 抽出されたデータ
        
    Returns:
        str: CSV形式の文字列
    """
    # 指定された順序
    column_order = [
        'VWAP',
        'SMA5',
        '前の足の高値',
        '前の足の始値',
        '前の足の終値',
        '前の足の安値',
        '高値',
        '始値',
        '終値',
        '安値',
        '次の足の高値',
        '次の足の始値',
        '次の足の終値',
        '次の足の安値',
        '出来高MA5',
        '前の足の出来高',
        '出来高',
        '次の足の出来高',
        '価格帯',
        '下落幅',
        '下ヒゲ',
        '実体',
        '抽出条件',
        '当足で勝ち',
        '次足で勝ち',
        '勝ち',
        '最低下落幅'
    ]
    
    # ヘッダー行
    header = ','.join(column_order)
    
    # データ行
    values = []
    for col in column_order:
        value = data.get(col, '')
        # 数値の場合はそのまま、文字列の場合はそのまま
        values.append(str(value))
    
    data_row = ','.join(values)
    
    return f"{header}\n{data_row}"


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(description='1分足データから指定時間の情報を抽出')
    parser.add_argument('csv_file', type=str, help='1分足データCSVファイルのパス')
    parser.add_argument('time', type=str, help='抽出する時間（HH:MM または HH:MM:SS形式）')
    
    args = parser.parse_args()
    
    # ファイルパスを解決
    csv_path = Path(args.csv_file)
    if not csv_path.is_absolute():
        # 相対パスの場合は、スクリプトのディレクトリを基準に解決
        script_dir = Path(__file__).parent
        csv_path = (script_dir.parent / csv_path).resolve()
    
    if not csv_path.exists():
        print(f"エラー: ファイルが見つかりません: {csv_path}", file=sys.stderr)
        return 1
    
    try:
        # データを抽出
        data = extract_one_minute_data(csv_path, args.time)
        
        # CSV形式で出力
        output = format_output(data)
        print(output)
        
        return 0
        
    except ValueError as e:
        print(f"エラー: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"エラー: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
