#!/usr/bin/env python3
"""
J-Quants APIを使用して指定価格帯の銘柄コードリストを取得するスクリプト

現在の価格が3000〜5000円の銘柄を抽出します。

使用方法:
    python test_20_stock_price_range.py [--min-price MIN] [--max-price MAX]

例:
    python test_20_stock_price_range.py
    python test_20_stock_price_range.py --min-price 3000 --max-price 5000

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


def get_all_stocks_prices_for_period(client, days=7):
    """
    J-Quants APIから過去指定日数間の全銘柄の株価データを一括取得する
    
    Args:
        client: J-Quants APIクライアント
        days (int): 取得する日数（デフォルト7日）
        
    Returns:
        pandas.DataFrame: 全銘柄の株価データ（Code, Date, Close等の列を含む）
    """
    try:
        print(f"過去{days}日間の全銘柄株価データを一括取得中...")
        
        # 日付範囲を設定
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        all_data = []
        
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
            return pd.DataFrame()
        
        # 全データを結合
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"\n合計 {len(combined_df)} 件のデータを取得しました")
        
        return combined_df
        
    except Exception as e:
        print(f"エラー: 全銘柄データの取得中にエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def filter_stocks_by_price_range(client, stock_list_df, price_df, min_price: float, max_price: float):
    """
    株価データから指定価格帯の銘柄を抽出する
    
    Args:
        client: J-Quants APIクライアント（使用しないが互換性のため保持）
        stock_list_df (pandas.DataFrame): 銘柄一覧のデータフレーム
        price_df (pandas.DataFrame): 全銘柄の株価データ
        min_price (float): 最小価格
        max_price (float): 最大価格
        
    Returns:
        List[Dict]: 条件に合致する銘柄のリスト
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
        stock_list_df[['Code', 'CompanyName', 'MarketCodeName', 'Sector17CodeName']],
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
                'price': float(row['Close']),
                'market': row.get('MarketCodeName', 'N/A'),
                'sector': row.get('Sector17CodeName', 'N/A')
            })
    
    print(f"該当銘柄数: {len(results)} 件")
    return results


def display_results(results: List[Dict], min_price: float, max_price: float):
    """
    結果をコンソールに表示する
    
    Args:
        results (List[Dict]): 該当銘柄のリスト
        min_price (float): 最小価格
        max_price (float): 最大価格
    """
    print("\n" + "=" * 80)
    print(f"価格範囲 {min_price:,.0f}円 〜 {max_price:,.0f}円 の銘柄一覧")
    print("=" * 80)
    print(f"該当銘柄数: {len(results)}件\n")
    
    if not results:
        print("該当する銘柄が見つかりませんでした。")
        return
    
    # 価格順にソート
    results_sorted = sorted(results, key=lambda x: x['price'], reverse=True)
    
    # ヘッダー表示
    print(f"{'銘柄コード':<8} {'銘柄名':<30} {'価格':>10} {'市場':<15} {'業種':<20}")
    print("-" * 80)
    
    # 各銘柄を表示
    for stock in results_sorted:
        code = stock['code']
        name = stock['company_name'][:28] if len(stock['company_name']) > 28 else stock['company_name']
        price = f"{stock['price']:,.0f}円"
        market = stock['market'][:13] if len(stock['market']) > 13 else stock['market']
        sector = stock['sector'][:18] if len(stock['sector']) > 18 else stock['sector']
        
        print(f"{code:<8} {name:<30} {price:>10} {market:<15} {sector:<20}")
    
    print("\n" + "=" * 80)
    print(f"合計: {len(results)}件")


def save_to_csv(results: List[Dict], output_dir: Path) -> Optional[str]:
    """
    結果をCSVファイルに保存する
    
    Args:
        results (List[Dict]): 該当銘柄のリスト
        output_dir (Path): 出力ディレクトリのパス
        
    Returns:
        Optional[str]: 保存したファイルパス（エラー時はNone）
    """
    if not results:
        print("保存するデータがありません。")
        return None
    
    try:
        # 出力ディレクトリを作成
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # ファイル名を生成（<西暦>_<月>_<日付>.csv）
        today = datetime.now()
        filename = f"{today.year}_{today.month:02d}_{today.day:02d}.csv"
        filepath = output_dir / filename
        
        # 価格順にソート
        results_sorted = sorted(results, key=lambda x: x['price'], reverse=True)
        
        # CSVファイルに書き込み
        with open(filepath, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            
            # ヘッダー
            writer.writerow(['銘柄コード', '銘柄名', '価格'])
            
            # データ行
            for stock in results_sorted:
                writer.writerow([
                    stock['code'],
                    stock['company_name'],
                    int(stock['price'])  # 価格は整数で出力（円不要）
                ])
        
        print(f"\nCSVファイルを保存しました: {filepath}")
        print(f"保存件数: {len(results_sorted)}件")
        return str(filepath)
        
    except Exception as e:
        print(f"エラー: CSVファイルの保存中にエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(
        description='指定価格帯の銘柄コードリストを取得するスクリプト'
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
        '--max-stocks',
        type=int,
        default=None,
        help='最大処理銘柄数（テスト用、指定しない場合は全件処理）'
    )
    
    args = parser.parse_args()
    
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
        
        print("J-Quants API 価格帯別銘柄リスト取得スクリプト")
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
        
        # 過去1週間の全銘柄株価データを一括取得
        price_df = get_all_stocks_prices_for_period(client, days=7)
        
        if price_df.empty:
            print("エラー: 株価データが取得できませんでした")
            sys.exit(1)
        
        # 価格帯でフィルタリング
        results = filter_stocks_by_price_range(
            client,
            stock_list_df,
            price_df,
            args.min_price,
            args.max_price
        )
        
        # 結果の表示
        display_results(results, args.min_price, args.max_price)
        
        # CSVファイルに保存
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        output_dir = project_root / "data" / "one_shot_trade"
        csv_path = save_to_csv(results, output_dir)
        
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

