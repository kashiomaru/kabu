"""
tick_chartフォルダ内のCSVファイルから9時〜10時のデータのみを抽出するスクリプト
"""

import os
import csv
from pathlib import Path
from datetime import datetime

def filter_tick_chart_by_time(input_dir, start_hour=9, end_hour=10):
    """
    tick_chartフォルダ内のCSVファイルから指定時間帯のデータのみを抽出
    
    Args:
        input_dir: tick_chartフォルダのパス
        start_hour: 開始時刻（時）
        end_hour: 終了時刻（時、この時刻は含まない）
    """
    input_path = Path(input_dir)
    
    if not input_path.exists():
        print(f"エラー: ディレクトリが見つかりません: {input_dir}")
        return
    
    # CSVファイルを取得
    csv_files = list(input_path.glob("*.csv"))
    
    if not csv_files:
        print(f"CSVファイルが見つかりません: {input_dir}")
        return
    
    print(f"処理対象ファイル数: {len(csv_files)}")
    
    for csv_file in csv_files:
        print(f"\n処理中: {csv_file.name}")
        
        # CSVファイルを読み込み
        rows_to_keep = []
        total_rows = 0
        filtered_rows = 0
        
        try:
            with open(csv_file, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                header = next(reader)  # ヘッダー行を取得
                rows_to_keep.append(header)
                
                # 時間列のインデックスを取得（"時間 "に注意）
                time_col_idx = None
                for idx, col in enumerate(header):
                    if '時間' in col:
                        time_col_idx = idx
                        break
                
                if time_col_idx is None:
                    print(f"  警告: 時間列が見つかりません。スキップします。")
                    continue
                
                # データ行を処理
                for row in reader:
                    total_rows += 1
                    
                    if len(row) <= time_col_idx:
                        continue
                    
                    time_str = row[time_col_idx].strip()
                    
                    # 時間をパース（HH:MM:SS形式）
                    try:
                        time_obj = datetime.strptime(time_str, "%H:%M:%S").time()
                        hour = time_obj.hour
                        
                        # 9時以上10時未満のデータを抽出
                        if start_hour <= hour < end_hour:
                            rows_to_keep.append(row)
                            filtered_rows += 1
                    except ValueError:
                        # 時間形式が不正な場合はスキップ
                        continue
            
            # フィルタリング後のデータを書き込み
            if filtered_rows > 0:
                with open(csv_file, 'w', encoding='utf-8-sig', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerows(rows_to_keep)
                print(f"  完了: {total_rows}行 → {filtered_rows}行（ヘッダー除く）")
            else:
                print(f"  警告: 該当するデータがありませんでした。ファイルは変更されません。")
                
        except Exception as e:
            print(f"  エラー: {csv_file.name} の処理中にエラーが発生しました: {e}")
    
    print(f"\n処理完了！")

if __name__ == "__main__":
    # tick_chartフォルダのパス
    base_dir = Path(__file__).parent.parent
    tick_chart_dir = base_dir / "data" / "tick_chart"
    
    print("=" * 60)
    print("tick_chartフォルダ内のCSVファイルから9時〜10時のデータを抽出")
    print("=" * 60)
    print(f"対象ディレクトリ: {tick_chart_dir}")
    print(f"抽出時間帯: 09:00:00 〜 09:59:59")
    print("\n注意: 元のファイルが上書きされます。必要に応じてバックアップを取ってください。")
    
    response = input("\n処理を続行しますか？ (y/n): ")
    if response.lower() == 'y':
        filter_tick_chart_by_time(tick_chart_dir, start_hour=9, end_hour=10)
    else:
        print("処理をキャンセルしました。")


