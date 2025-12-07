#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
1分足データを分析して、推奨下落幅>=検証値の足を抽出し、10秒以内に出来る最大利確を計算するスクリプト

処理内容:
1. 1分足データCSVファイルを読み込み
2. 推奨下落幅>=検証値の1分足を抽出
3. 抽出した足に対して、10秒以内に出来る最大利確を計算
   （計算式: 安値から10秒戻り幅 - (推奨下落幅 - 6 + 1)）
4. 時間と10秒以内に出来る最大利確をターミナルに出力

使用方法:
    python test_33_analysis_one_minute.py CSV_FILE VERIFICATION_VALUE

例:
    python test_33_analysis_one_minute.py data/tick_chart/6081_20251205_one.csv 10
"""

import sys
import argparse
from pathlib import Path
import pandas as pd
import numpy as np


def analyze_one_minute_data(csv_path: Path, verification_value: float) -> pd.DataFrame:
    """
    1分足データを分析して、推奨下落幅>=検証値の足を抽出し、10秒以内に出来る最大利確を計算
    
    Args:
        csv_path: 1分足データCSVファイルのパス
        verification_value: 検証値
        
    Returns:
        pd.DataFrame: 抽出したデータ（時刻、10秒以内に出来る最大利確）
    """
    # CSVファイルを読み込み
    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    
    # 必要な列が存在するか確認
    required_columns = ['時刻', '推奨下落幅', '安値から10秒戻り幅']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"CSVファイルに'{col}'列が見つかりません")
    
    # 推奨下落幅>=検証値の1分足を抽出
    filtered_df = df[df['推奨下落幅'] >= verification_value].copy()
    
    if filtered_df.empty:
        return pd.DataFrame(columns=['時刻', '10秒以内に出来る最大利確'])
    
    # 10秒以内に出来る最大利確を計算
    # 計算式: 安値から10秒戻り幅 - (推奨下落幅 - 6 + 1)
    filtered_df['10秒以内に出来る最大利確'] = (
        filtered_df['安値から10秒戻り幅'] - (filtered_df['推奨下落幅'] - 6 + 1)
    )
    
    # NaN値を0に置き換え（データがない場合）
    filtered_df['10秒以内に出来る最大利確'] = filtered_df['10秒以内に出来る最大利確'].fillna(0)
    
    # 時刻と10秒以内に出来る最大利確のみを抽出
    result = filtered_df[['時刻', '10秒以内に出来る最大利確']].copy()
    
    # 数値をfloat型に変換してから丸める
    result['10秒以内に出来る最大利確'] = result['10秒以内に出来る最大利確'].astype(float).round(1)
    
    return result


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(description='1分足データを分析して、推奨下落幅>=検証値の足を抽出し、10秒以内に出来る最大利確を計算')
    parser.add_argument('csv_file', type=str, help='1分足データCSVファイルのパス')
    parser.add_argument('verification_value', type=float, help='検証値')
    
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
        # データを分析
        result_df = analyze_one_minute_data(csv_path, args.verification_value)
        
        if result_df.empty:
            print(f"推奨下落幅>={args.verification_value}の1分足が見つかりませんでした。")
            return 0
        
        # CSV形式で出力（小数点以下が0の場合は整数として表示）
        df_to_output = result_df.copy()
        df_to_output['10秒以内に出来る最大利確'] = df_to_output['10秒以内に出来る最大利確'].apply(
            lambda x: f"{int(x)}" if pd.notna(x) and isinstance(x, (int, float)) and x == int(x) 
            else f"{x:.1f}" if pd.notna(x) and isinstance(x, (int, float)) 
            else x
        )
        
        # CSV形式で標準出力に出力
        df_to_output.to_csv(sys.stdout, index=False, encoding='utf-8-sig')
        
        return 0
        
    except ValueError as e:
        print(f"エラー: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"エラー: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
