#!/usr/bin/env python3
"""
J-Quants APIを使用して指定された銘柄コードの株価を取得するスクリプト

使用方法:
    python test_01_stock_price.py <銘柄コード>
    
例:
    python test_01_stock_price.py 7203  # トヨタ自動車
    python test_01_stock_price.py 6758  # ソニーグループ

前提条件:
    - token.txtファイルにAPIキー（リフレッシュトークン）が記述されていること
    - jquants-api-clientライブラリがインストールされていること
"""

import os
import sys
import json
from pathlib import Path
import jquantsapi
import pandas as pd
from datetime import datetime, timedelta


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


def get_stock_price(client, code, days=30):
    """
    J-Quants APIから指定された銘柄の株価を取得する
    
    Args:
        client: J-Quants APIクライアント
        code (str): 銘柄コード
        days (int): 取得する日数（デフォルト30日）
        
    Returns:
        pandas.DataFrame: 株価データのデータフレーム
        
    Raises:
        Exception: API接続エラー
    """
    try:
        # 日付範囲を設定（過去指定日数間）
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
        
        print(f"銘柄コード {code} の株価データを取得中...")
        print(f"期間: {start_date} ～ {end_date}")
        
        # 株価データの取得
        price_data = client.get_prices_daily_quotes(
            code=code,
            from_yyyymmdd=start_date,
            to_yyyymmdd=end_date
        )
        
        # データフレームに変換
        df = pd.DataFrame(price_data)
        
        if df.empty:
            raise ValueError(f"銘柄コード {code} のデータが見つかりませんでした")
        
        # 日付でソート（最新が最初に来るように降順）
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.sort_values('Date', ascending=False).reset_index(drop=True)
            
        return df
        
    except Exception as e:
        raise Exception(f"株価データの取得中にエラーが発生しました: {e}")


def display_stock_price(df, code):
    """
    株価データを表示する
    
    Args:
        df (pandas.DataFrame): 株価データのデータフレーム
        code (str): 銘柄コード
    """
    if df.empty:
        print(f"銘柄コード {code} のデータが見つかりませんでした。")
        return
        
    print(f"\n銘柄コード: {code}")
    print(f"取得データ数: {len(df)}件")
    print("=" * 80)
    
    # 最新のデータを表示
    latest_data = df.iloc[0] if not df.empty else None
    
    if latest_data is not None:
        print("最新の株価情報:")
        print(f"  日付: {latest_data.get('Date', 'N/A')}")
        print(f"  銘柄コード: {latest_data.get('Code', 'N/A')}")
        print(f"  始値: {latest_data.get('Open', 'N/A'):,}円" if latest_data.get('Open') else "  始値: N/A")
        print(f"  高値: {latest_data.get('High', 'N/A'):,}円" if latest_data.get('High') else "  高値: N/A")
        print(f"  安値: {latest_data.get('Low', 'N/A'):,}円" if latest_data.get('Low') else "  安値: N/A")
        print(f"  終値: {latest_data.get('Close', 'N/A'):,}円" if latest_data.get('Close') else "  終値: N/A")
        print(f"  出来高: {latest_data.get('Volume', 'N/A'):,}株" if latest_data.get('Volume') else "  出来高: N/A")
        print(f"  調整後終値: {latest_data.get('AdjustmentFactor', 'N/A'):,}円" if latest_data.get('AdjustmentFactor') else "  調整後終値: N/A")
    
    print("\n直近5日間の株価推移:")
    print("-" * 80)
    
    # 直近5日間のデータを表示
    recent_data = df.head(5)
    display_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    available_columns = [col for col in display_columns if col in df.columns]
    
    if available_columns:
        for _, row in recent_data.iterrows():
            print(f"日付: {row.get('Date', 'N/A')}")
            for col in available_columns:
                if col == 'Date':
                    continue
                value = row.get(col, 'N/A')
                if col in ['Open', 'High', 'Low', 'Close', 'AdjustmentFactor'] and value != 'N/A':
                    print(f"  {col}: {value:,}円")
                elif col == 'Volume' and value != 'N/A':
                    print(f"  {col}: {value:,}株")
                else:
                    print(f"  {col}: {value}")
            print("-" * 40)
    
    print(f"\n全列の情報:")
    print(df.columns.tolist())


def validate_stock_code(code):
    """
    銘柄コードの形式を検証する
    
    Args:
        code (str): 銘柄コード
        
    Returns:
        bool: 有効な形式かどうか
    """
    if not code:
        return False
    
    # 4桁の数字かチェック
    if not code.isdigit() or len(code) != 4:
        return False
        
    return True


def main():
    """メイン処理"""
    try:
        # 引数の確認
        if len(sys.argv) != 2:
            print("使用方法: python test_01_stock_price.py <銘柄コード>")
            print("例: python test_01_stock_price.py 7203")
            sys.exit(1)
        
        stock_code = sys.argv[1]
        
        # 銘柄コードの検証
        if not validate_stock_code(stock_code):
            print(f"エラー: 無効な銘柄コードです: {stock_code}")
            print("銘柄コードは4桁の数字で入力してください。")
            sys.exit(1)
        
        # スクリプトのディレクトリを取得
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        token_file_path = project_root / "token.txt"
        
        print("J-Quants API 株価取得スクリプト")
        print("=" * 50)
        
        # APIキーの読み込み
        print(f"APIキーを読み込み中: {token_file_path}")
        refresh_token = load_api_key(token_file_path)
        print("APIキーの読み込み完了")
        
        # クライアントの初期化
        client = jquantsapi.Client(refresh_token=refresh_token)
        
        # 株価データの取得
        df = get_stock_price(client, stock_code)
        
        # 結果の表示
        display_stock_price(df, stock_code)
        
        print("\n処理が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
