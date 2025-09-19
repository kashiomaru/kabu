#!/usr/bin/env python3
"""
J-Quants APIを使用して銘柄一覧を取得し、銘柄コードリストをファイルに保存するスクリプト

使用方法:
    python test_11_stock_list.py

前提条件:
    - token.txtファイルにAPIキー（リフレッシュトークン）が記述されていること
    - jquants-api-clientライブラリがインストールされていること
"""

import os
import sys
from pathlib import Path
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


def get_stock_list(refresh_token):
    """
    J-Quants APIから銘柄一覧を取得する
    
    Args:
        refresh_token (str): リフレッシュトークン
        
    Returns:
        pandas.DataFrame: 銘柄一覧のデータフレーム
        
    Raises:
        Exception: API接続エラー
    """
    try:
        # クライアントの初期化
        client = jquantsapi.Client(refresh_token=refresh_token)
        
        # 銘柄一覧の取得
        print("銘柄一覧を取得中...")
        stock_list = client.get_listed_info()
        
        # データフレームに変換
        df = pd.DataFrame(stock_list)
        
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
    
    print(f"市場フィルタリング結果:")
    print(f"  全銘柄数: {len(df)}件")
    print(f"  対象市場銘柄数: {len(filtered_df)}件")
    
    # 市場別の銘柄数を表示
    if len(filtered_df) > 0:
        market_counts = filtered_df['MarketCodeName'].value_counts()
        print(f"  市場別銘柄数:")
        for market, count in market_counts.items():
            print(f"    {market}: {count}件")
    
    return filtered_df


def save_stock_list_to_csv(df, output_file_path):
    """
    銘柄一覧をCSV形式でファイルに保存する
    
    Args:
        df (pandas.DataFrame): 銘柄一覧のデータフレーム
        output_file_path (str): 出力ファイルのパス
    """
    try:
        # 出力ディレクトリが存在しない場合は作成
        output_dir = Path(output_file_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 既存ファイルがある場合は削除
        if os.path.exists(output_file_path):
            os.remove(output_file_path)
            print(f"既存ファイルを削除しました: {output_file_path}")
        
        # 必要な列を確認
        required_columns = ['Code', 'CompanyName', 'Sector17CodeName', 'Sector33CodeName', 'MarketCodeName']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"警告: 以下の列が見つかりません: {missing_columns}")
            print(f"利用可能な列: {df.columns.tolist()}")
        
        # 利用可能な列のみでデータフレームを作成
        available_columns = [col for col in required_columns if col in df.columns]
        if not available_columns:
            raise ValueError("必要な列が一つも見つかりませんでした")
        
        # データフレームをフィルタリング
        filtered_df = df[available_columns].copy()
        
        # 欠損値を空文字で埋める
        filtered_df = filtered_df.fillna('')
        
        # 列名を日本語に変更
        column_mapping = {
            'Code': '銘柄コード',
            'CompanyName': '企業名（日本語）',
            'Sector17CodeName': '17業種名',
            'Sector33CodeName': '33業種名',
            'MarketCodeName': '市場名'
        }
        
        # 利用可能な列のみをマッピング
        available_mapping = {k: v for k, v in column_mapping.items() if k in available_columns}
        filtered_df = filtered_df.rename(columns=available_mapping)
        
        # CSVファイルに保存（ダブルクオートで囲む）
        filtered_df.to_csv(output_file_path, index=False, encoding='utf-8-sig', quoting=1)
        
        print(f"銘柄一覧をCSV形式で保存しました: {output_file_path}")
        print(f"保存した銘柄数: {len(filtered_df)}件")
        print(f"保存した列: {list(available_mapping.values())}")
        
        # 最初の5件を表示（確認用）
        print(f"\n保存したデータ（最初の5件）:")
        for i, (_, row) in enumerate(filtered_df.head(5).iterrows(), 1):
            print(f"  {i:2d}. {', '.join([f'{col}: {row[col]}' for col in available_mapping.values()])}")
        
        if len(filtered_df) > 5:
            print(f"  ... 他 {len(filtered_df) - 5} 件")
        
    except Exception as e:
        raise Exception(f"CSVファイル保存中にエラーが発生しました: {e}")


def display_stock_list_summary(df):
    """
    銘柄一覧のサマリー情報を表示する
    
    Args:
        df (pandas.DataFrame): 銘柄一覧のデータフレーム
    """
    if df.empty:
        print("銘柄データが見つかりませんでした。")
        return
        
    print(f"\n取得した銘柄数: {len(df)}件")
    
    # 市場別の銘柄数
    if 'MarketCodeName' in df.columns:
        market_counts = df['MarketCodeName'].value_counts()
        print(f"\n市場別銘柄数:")
        for market, count in market_counts.items():
            print(f"  {market}: {count}件")
    
    # セクター別の銘柄数（上位10位）
    if 'Sector17CodeName' in df.columns:
        sector_counts = df['Sector17CodeName'].value_counts()
        print(f"\nセクター別銘柄数（上位10位）:")
        for i, (sector, count) in enumerate(sector_counts.head(10).items(), 1):
            print(f"  {i:2d}. {sector}: {count}件")
    
    # 利用可能な列情報
    print(f"\n利用可能なデータ項目: {len(df.columns)}項目")
    print("主要項目:", [col for col in ['Code', 'CompanyName', 'MarketCodeName', 'Sector17CodeName'] if col in df.columns])


def main():
    """メイン処理"""
    try:
        # スクリプトのディレクトリを取得
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        token_file_path = project_root / "token.txt"
        output_file_path = project_root / "data" / "stock_list.csv"
        
        print("J-Quants API 銘柄一覧取得・保存スクリプト")
        print("=" * 60)
        
        # APIキーの読み込み
        print(f"APIキーを読み込み中: {token_file_path}")
        refresh_token = load_api_key(token_file_path)
        print("APIキーの読み込み完了")
        
        # 銘柄一覧の取得
        df = get_stock_list(refresh_token)
        
        if df.empty:
            print("エラー: 銘柄データが取得できませんでした")
            sys.exit(1)
        
        # 対象市場（プライム、スタンダード、グロース）の銘柄のみをフィルタリング
        print(f"\n対象市場の銘柄をフィルタリング中...")
        filtered_df = filter_target_markets(df)
        
        if filtered_df.empty:
            print("エラー: 対象市場の銘柄が見つかりませんでした")
            sys.exit(1)
        
        # サマリー情報の表示
        display_stock_list_summary(filtered_df)
        
        # 銘柄一覧のCSV保存
        print(f"\n銘柄一覧をCSV形式で保存中...")
        save_stock_list_to_csv(filtered_df, str(output_file_path))
        
        print(f"\n処理が完了しました。")
        print(f"保存先: {output_file_path}")
        print(f"出力形式: CSV（銘柄コード,企業名（日本語）,17業種名,33業種名,市場名）")
        print(f"対象市場: プライム、スタンダード、グロース")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
