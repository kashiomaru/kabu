#!/usr/bin/env python3
"""
J-Quants APIを使用して銘柄一覧を取得するスクリプト

使用方法:
    python test_00_stock_list.py

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


def display_stock_list(df):
    """
    銘柄一覧を表示する
    
    Args:
        df (pandas.DataFrame): 銘柄一覧のデータフレーム
    """
    if df.empty:
        print("銘柄データが見つかりませんでした。")
        return
        
    print(f"\n取得した銘柄数: {len(df)}件")
    print("\n銘柄一覧:")
    print("=" * 80)
    
    # 主要な列のみ表示（全列だと長すぎるため）
    display_columns = ['Code', 'CompanyName', 'MarketCodeName', 'Sector17CodeName']
    available_columns = [col for col in display_columns if col in df.columns]
    
    if available_columns:
        print(df[available_columns].to_string(index=False))
    else:
        print(df.head(10).to_string(index=False))
    
    print("\n全列の情報:")
    print(df.columns.tolist())


def main():
    """メイン処理"""
    try:
        # スクリプトのディレクトリを取得
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        token_file_path = project_root / "token.txt"
        
        print("J-Quants API 銘柄一覧取得スクリプト")
        print("=" * 50)
        
        # APIキーの読み込み
        print(f"APIキーを読み込み中: {token_file_path}")
        refresh_token = load_api_key(token_file_path)
        print("APIキーの読み込み完了")
        
        # 銘柄一覧の取得
        df = get_stock_list(refresh_token)
        
        # 結果の表示
        display_stock_list(df)
        
        print("\n処理が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
