#!/usr/bin/env python3
"""
J-Quants APIを使用して指定された銘柄コードの最新財務データの基本情報を取得・表示するスクリプト

使用方法:
    python test_06_stock_statements.py <銘柄コード>
    
例:
    python test_06_stock_statements.py 7203  # トヨタ自動車
    python test_06_stock_statements.py 6758  # ソニーグループ

前提条件:
    - token.txtファイルにAPIキー（リフレッシュトークン）が記述されていること
    - jquants-api-clientライブラリがインストールされていること
"""

import os
import sys
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


def get_latest_financial_statements(client, code):
    """
    J-Quants APIから指定された銘柄の全財務データを取得する
    
    Args:
        client: J-Quants APIクライアント
        code (str): 銘柄コード
        
    Returns:
        pandas.DataFrame: 財務データのデータフレーム（取得できない場合は空のDataFrame）
        
    Raises:
        Exception: API接続エラー
    """
    try:
        print(f"銘柄コード {code} の財務データを取得中...")
        
        # 銘柄コードのみで財務データを取得（日付指定なし）
        financial_data = client.get_fins_statements(code=code)
        
        if not financial_data.empty:
            print(f"財務データを取得しました: {len(financial_data)} 件")
            # DisclosedDateでソート（最新が最初に来るように降順）
            if 'DisclosedDate' in financial_data.columns:
                financial_data['DisclosedDate'] = pd.to_datetime(financial_data['DisclosedDate'])
                financial_data = financial_data.sort_values('DisclosedDate', ascending=False).reset_index(drop=True)
            return financial_data
        else:
            print(f"銘柄コード {code} の財務データが見つかりませんでした")
            return pd.DataFrame()
        
    except Exception as e:
        raise Exception(f"財務データの取得中にエラーが発生しました: {e}")


def display_financial_statements_info(df, code):
    """
    財務データの基本情報を表示する
    
    Args:
        df (pandas.DataFrame): 財務データのデータフレーム
        code (str): 銘柄コード
    """
    if df.empty:
        print(f"銘柄コード {code} の財務データが見つかりませんでした。")
        return
    
    print(f"\n銘柄コード: {code}")
    print("=" * 80)
    print("【最新財務データの基本情報】")
    print("-" * 50)
    
    # 最新のデータを取得（DisclosedDateでソート済みのため最初の行）
    latest_data = df.iloc[0]
    
    # 指定された項目を表示
    print(f"DisclosedDate: {latest_data.get('DisclosedDate', 'N/A')}")
    print(f"DisclosedTime: {latest_data.get('DisclosedTime', 'N/A')}")
    print(f"LocalCode: {latest_data.get('LocalCode', 'N/A')}")
    print(f"DisclosureNumber: {latest_data.get('DisclosureNumber', 'N/A')}")
    print(f"TypeOfDocument: {latest_data.get('TypeOfDocument', 'N/A')}")
    print(f"TypeOfCurrentPeriod: {latest_data.get('TypeOfCurrentPeriod', 'N/A')}")
    
    print("\n【追加情報】")
    print("-" * 50)
    print(f"FiscalYear: {latest_data.get('FiscalYear', 'N/A')}")
    print(f"FiscalPeriod: {latest_data.get('FiscalPeriod', 'N/A')}")
    print(f"TypeOfCurrentPeriod: {latest_data.get('TypeOfCurrentPeriod', 'N/A')}")
    print(f"TypeOfPreviousPeriod: {latest_data.get('TypeOfPreviousPeriod', 'N/A')}")
    print(f"NetSales: {latest_data.get('NetSales', 'N/A')}")
    print(f"Profit: {latest_data.get('Profit', 'N/A')}")
    print(f"Equity: {latest_data.get('Equity', 'N/A')}")
    
    print(f"\n【全データ件数】")
    print(f"取得した財務データ件数: {len(df)} 件")
    
    # 複数のデータがある場合は、最新の数件を表示
    if len(df) > 1:
        print(f"\n【最新3件のデータ】")
        print("-" * 50)
        for i, (_, row) in enumerate(df.head(3).iterrows()):
            print(f"\nデータ {i+1}:")
            print(f"  DisclosedDate: {row.get('DisclosedDate', 'N/A')}")
            print(f"  TypeOfDocument: {row.get('TypeOfDocument', 'N/A')}")
            print(f"  TypeOfCurrentPeriod: {row.get('TypeOfCurrentPeriod', 'N/A')}")
            print(f"  FiscalYear: {row.get('FiscalYear', 'N/A')}")
            print(f"  FiscalPeriod: {row.get('FiscalPeriod', 'N/A')}")


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
            print("使用方法: python test_06_stock_statements.py <銘柄コード>")
            print("例: python test_06_stock_statements.py 7203")
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
        
        print("J-Quants API 財務データ基本情報取得スクリプト")
        print("=" * 60)
        
        # APIキーの読み込み
        print(f"APIキーを読み込み中: {token_file_path}")
        refresh_token = load_api_key(token_file_path)
        print("APIキーの読み込み完了")
        
        # クライアントの初期化
        client = jquantsapi.Client(refresh_token=refresh_token)
        
        # 財務データの取得
        df = get_latest_financial_statements(client, stock_code)
        
        # 結果の表示
        display_financial_statements_info(df, stock_code)
        
        print("\n処理が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
