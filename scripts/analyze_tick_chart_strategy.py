#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tick_chartフォルダ内の歩み値データからエントリー・イグジット戦略を分析するスクリプト

処理内容:
1. tick_chartフォルダ内のCSVファイルを読み込み
2. 各銘柄の9時〜10時の歩み値データを分析
3. 基本統計、パターン分析、戦略提案を実行
4. 結果をCSVとグラフで出力

使用方法:
    python analyze_tick_chart_strategy.py [--input-dir INPUT_DIR] [--output-dir OUTPUT_DIR]

例:
    python analyze_tick_chart_strategy.py
    python analyze_tick_chart_strategy.py --input-dir ../data/tick_chart --output-dir ../data/analysis
"""

import os
import sys
import csv
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import pandas as pd
import numpy as np

# matplotlibはオプション（グラフ作成時のみ必要）
try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib import font_manager
    # 日本語フォントの設定
    plt.rcParams['font.family'] = 'DejaVu Sans'
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False


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


def calculate_basic_stats(df: pd.DataFrame) -> Dict:
    """
    基本統計を計算
    
    Args:
        df: 歩み値データのDataFrame
        
    Returns:
        dict: 基本統計の辞書
    """
    if df.empty:
        return {}
    
    stats = {}
    
    # 価格の基本統計
    stats['start_price'] = df['price'].iloc[0]  # 最初の約定値
    stats['end_price'] = df['price'].iloc[-1]  # 最後の約定値
    stats['high_price'] = df['price'].max()
    stats['low_price'] = df['price'].min()
    stats['avg_price'] = df['price'].mean()
    
    # 価格変動
    stats['price_change'] = stats['end_price'] - stats['start_price']
    stats['price_change_pct'] = (stats['price_change'] / stats['start_price']) * 100 if stats['start_price'] > 0 else 0
    stats['price_range'] = stats['high_price'] - stats['low_price']
    stats['price_range_pct'] = (stats['price_range'] / stats['start_price']) * 100 if stats['start_price'] > 0 else 0
    
    # 出来高の統計
    stats['total_volume'] = df['volume'].sum()
    stats['avg_volume'] = df['volume'].mean()
    stats['max_volume'] = df['volume'].max()
    stats['trade_count'] = len(df)
    
    # 時間関連
    stats['duration_minutes'] = (df['datetime'].iloc[-1] - df['datetime'].iloc[0]).total_seconds() / 60
    stats['start_time'] = df['time'].iloc[0]
    stats['end_time'] = df['time'].iloc[-1]
    
    return stats


def detect_patterns(df: pd.DataFrame) -> Dict:
    """
    パターンを検出
    
    Args:
        df: 歩み値データのDataFrame
        
    Returns:
        dict: 検出されたパターンの辞書
    """
    if df.empty or len(df) < 2:
        return {}
    
    patterns = {}
    
    # 価格トレンドの判定
    price_trend = "横ばい"
    price_change_pct = ((df['price'].iloc[-1] - df['price'].iloc[0]) / df['price'].iloc[0]) * 100
    
    if price_change_pct > 0.5:
        price_trend = "上昇"
    elif price_change_pct < -0.5:
        price_trend = "下降"
    
    patterns['price_trend'] = price_trend
    patterns['price_trend_strength'] = abs(price_change_pct)
    
    # 出来高の急増を検出（移動平均の2倍以上）
    if len(df) >= 10:
        df['volume_ma'] = df['volume'].rolling(window=10, min_periods=1).mean()
        df['volume_ratio'] = df['volume'] / df['volume_ma']
        
        # 出来高が平均の2倍以上になったポイント
        volume_surge_points = df[df['volume_ratio'] >= 2.0]
        patterns['volume_surge_count'] = len(volume_surge_points)
        patterns['max_volume_ratio'] = df['volume_ratio'].max() if 'volume_ratio' in df.columns else 0
    else:
        patterns['volume_surge_count'] = 0
        patterns['max_volume_ratio'] = 0
    
    # 価格の急変動を検出（前の価格からの変化率が1%以上）
    df['price_change_pct'] = df['price'].pct_change() * 100
    price_volatility_points = df[abs(df['price_change_pct']) >= 1.0]
    patterns['price_volatility_count'] = len(price_volatility_points)
    patterns['max_price_change_pct'] = df['price_change_pct'].abs().max() if 'price_change_pct' in df.columns else 0
    
    # 時間帯別の特徴（前半30分 vs 後半30分）
    mid_time = df['datetime'].iloc[0] + pd.Timedelta(minutes=30)
    first_half = df[df['datetime'] < mid_time]
    second_half = df[df['datetime'] >= mid_time]
    
    if not first_half.empty and not second_half.empty:
        first_half_change = ((first_half['price'].iloc[-1] - first_half['price'].iloc[0]) / first_half['price'].iloc[0]) * 100
        second_half_change = ((second_half['price'].iloc[-1] - second_half['price'].iloc[0]) / second_half['price'].iloc[0]) * 100
        
        patterns['first_half_change_pct'] = first_half_change
        patterns['second_half_change_pct'] = second_half_change
    else:
        patterns['first_half_change_pct'] = 0
        patterns['second_half_change_pct'] = 0
    
    return patterns


def suggest_strategy(stats: Dict, patterns: Dict) -> Dict:
    """
    エントリー・イグジット戦略を提案
    
    Args:
        stats: 基本統計の辞書
        patterns: パターンの辞書
        
    Returns:
        dict: 戦略提案の辞書
    """
    strategy = {}
    
    # エントリーシグナル
    entry_signals = []
    
    # 1. 出来高急増 + 価格上昇
    if patterns.get('volume_surge_count', 0) > 0 and patterns.get('price_trend') == '上昇':
        entry_signals.append("出来高急増 + 価格上昇")
    
    # 2. 前半の上昇トレンド
    if patterns.get('first_half_change_pct', 0) > 0.3:
        entry_signals.append("前半の上昇トレンド")
    
    # 3. 価格ブレイクアウト（高値更新）
    if stats.get('end_price', 0) > stats.get('start_price', 0) * 1.01:
        entry_signals.append("価格ブレイクアウト")
    
    strategy['entry_signals'] = "; ".join(entry_signals) if entry_signals else "シグナルなし"
    strategy['entry_score'] = len(entry_signals)
    
    # イグジットシグナル
    exit_signals = []
    
    # 1. 利益確定ポイント（価格上昇率）
    if stats.get('price_change_pct', 0) > 1.0:
        exit_signals.append(f"利益確定候補（+{stats.get('price_change_pct', 0):.2f}%）")
    
    # 2. 損切りポイント（価格下落率）
    if stats.get('price_change_pct', 0) < -0.5:
        exit_signals.append(f"損切り候補（{stats.get('price_change_pct', 0):.2f}%）")
    
    # 3. 出来高減少 + 価格停滞
    if patterns.get('volume_surge_count', 0) == 0 and abs(stats.get('price_change_pct', 0)) < 0.3:
        exit_signals.append("出来高減少 + 価格停滞")
    
    strategy['exit_signals'] = "; ".join(exit_signals) if exit_signals else "シグナルなし"
    
    # 推奨エントリーポイント
    if stats.get('start_price', 0) > 0:
        if patterns.get('first_half_change_pct', 0) > 0.2:
            strategy['recommended_entry'] = f"前半（9:00-9:30）の上昇開始時"
        elif patterns.get('price_trend') == '上昇':
            strategy['recommended_entry'] = "9:00開始時"
        else:
            strategy['recommended_entry'] = "様子見推奨"
    else:
        strategy['recommended_entry'] = "データ不足"
    
    # 推奨イグジットポイント
    if stats.get('price_change_pct', 0) > 1.0:
        strategy['recommended_exit'] = f"利益確定（+{stats.get('price_change_pct', 0):.2f}%達成時）"
    elif stats.get('price_change_pct', 0) < -0.5:
        strategy['recommended_exit'] = f"損切り（{stats.get('price_change_pct', 0):.2f}%下落時）"
    else:
        strategy['recommended_exit'] = "10:00終了時"
    
    return strategy


def visualize_data(df: pd.DataFrame, stock_code: str, date_str: str, output_dir: Path):
    """
    データを可視化してグラフを保存
    
    Args:
        df: 歩み値データのDataFrame
        stock_code: 銘柄コード
        date_str: 日付文字列
        output_dir: 出力ディレクトリ
    """
    if not MATPLOTLIB_AVAILABLE:
        print(f"  警告: matplotlibがインストールされていないため、グラフをスキップします")
        return
    
    if df.empty:
        return
    
    fig, axes = plt.subplots(2, 1, figsize=(12, 8))
    fig.suptitle(f'{stock_code} - {date_str} (9:00-10:00)', fontsize=14, fontweight='bold')
    
    # 価格推移
    ax1 = axes[0]
    ax1.plot(df['datetime'], df['price'], marker='o', markersize=2, linewidth=1, color='blue', label='約定値')
    ax1.set_ylabel('価格 (円)', fontsize=10)
    ax1.set_title('価格推移', fontsize=11)
    ax1.grid(True, alpha=0.3)
    ax1.legend()
    
    # X軸の時刻フォーマット
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax1.xaxis.set_major_locator(mdates.MinuteLocator(interval=10))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
    
    # 出来高推移
    ax2 = axes[1]
    ax2.bar(df['datetime'], df['volume'], width=pd.Timedelta(seconds=30), color='green', alpha=0.6, label='出来高')
    ax2.set_xlabel('時刻', fontsize=10)
    ax2.set_ylabel('出来高', fontsize=10)
    ax2.set_title('出来高推移', fontsize=11)
    ax2.grid(True, alpha=0.3)
    ax2.legend()
    
    # X軸の時刻フォーマット
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax2.xaxis.set_major_locator(mdates.MinuteLocator(interval=10))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
    
    plt.tight_layout()
    
    # グラフを保存
    graph_path = output_dir / f'{stock_code}_{date_str}_chart.png'
    plt.savefig(graph_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"  グラフを保存しました: {graph_path.name}")


def analyze_single_file(csv_path: Path, output_dir: Path, create_graphs: bool = True) -> Optional[Dict]:
    """
    単一のCSVファイルを分析
    
    Args:
        csv_path: CSVファイルのパス
        output_dir: 出力ディレクトリ
        create_graphs: グラフを作成するかどうか
        
    Returns:
        dict: 分析結果、失敗時はNone
    """
    # ファイル名から銘柄コードと日付を抽出
    filename = csv_path.stem
    parts = filename.split('_')
    
    if len(parts) < 2:
        print(f"  警告: ファイル名の形式が不正です: {filename}")
        return None
    
    stock_code = parts[0]
    date_str = parts[1]
    
    print(f"分析中: {stock_code} ({date_str})")
    
    # データを読み込み
    df = load_tick_data(csv_path)
    
    if df is None or df.empty:
        print(f"  警告: データが読み込めませんでした")
        return None
    
    # 基本統計を計算
    stats = calculate_basic_stats(df)
    
    # パターンを検出
    patterns = detect_patterns(df)
    
    # 戦略を提案
    strategy = suggest_strategy(stats, patterns)
    
    # 結果をまとめる
    result = {
        'stock_code': stock_code,
        'date': date_str,
        **stats,
        **patterns,
        **strategy
    }
    
    # グラフを作成
    if create_graphs:
        visualize_data(df, stock_code, date_str, output_dir)
    
    return result


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(description='tick_chartデータからエントリー・イグジット戦略を分析')
    parser.add_argument('--input-dir', type=str, default='../data/tick_chart',
                        help='入力ディレクトリ（tick_chartフォルダ）')
    parser.add_argument('--output-dir', type=str, default='../data/tick_chart_analysis',
                        help='出力ディレクトリ')
    parser.add_argument('--no-graphs', action='store_true',
                        help='グラフを作成しない')
    
    args = parser.parse_args()
    
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    
    # 出力ディレクトリを作成
    output_dir.mkdir(parents=True, exist_ok=True)
    
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
    
    # 各ファイルを分析
    results = []
    success_count = 0
    error_count = 0
    
    for csv_file in csv_files:
        try:
            result = analyze_single_file(csv_file, output_dir, create_graphs=not args.no_graphs)
            
            if result:
                results.append(result)
                success_count += 1
            else:
                error_count += 1
        except Exception as e:
            print(f"  エラー: {e}")
            error_count += 1
    
    # 結果をCSVに保存
    if results:
        df_results = pd.DataFrame(results)
        output_csv = output_dir / 'analysis_results.csv'
        df_results.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print("\n" + "=" * 80)
        print(f"分析完了")
        print(f"成功: {success_count}件, エラー: {error_count}件")
        print(f"結果を保存しました: {output_csv}")
    else:
        print("\n" + "=" * 80)
        print("分析結果がありません")


if __name__ == '__main__':
    main()

