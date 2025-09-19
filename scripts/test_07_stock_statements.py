#!/usr/bin/env python3
"""
J-Quants APIを使用して指定された銘柄コードの財務データから特定項目を取得・表示するスクリプト

使用方法:
    python test_07_stock_statements.py <銘柄コード>
    
例:
    python test_07_stock_statements.py 7203  # トヨタ自動車
    python test_07_stock_statements.py 6758  # ソニーグループ
    python test_07_stock_statements.py 228A  # アルファベット付き銘柄コード

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


def get_financial_statements(client, code):
    """
    J-Quants APIから指定された銘柄の財務データを取得する
    
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


def display_specific_financial_data(df, code):
    """
    指定された財務データ項目を表示する
    
    Args:
        df (pandas.DataFrame): 財務データのデータフレーム
        code (str): 銘柄コード
    """
    if df.empty:
        print(f"銘柄コード {code} の財務データが見つかりませんでした。")
        return
    
    print(f"\n銘柄コード: {code}")
    print("=" * 80)
    print("【指定財務データ項目】")
    print("-" * 50)


    # print(">>>>")
    # print(df.iloc[0])
    # print(">>>>")
    # print(df.iloc[1])
    # print(">>>>")
    # print(df.iloc[2])
    # print(">>>>")
    # print(df.iloc[3])
    # print(">>>>")


    # 最新のデータを取得（DisclosedDateでソート済みのため最初の行）
    latest_data = df.iloc[0]

    # 指定された項目を表示
    print(f"TypeOfDocument: {latest_data.get('TypeOfDocument', 'N/A')}")
    print(f"TypeOfCurrentPeriod: {latest_data.get('TypeOfCurrentPeriod', 'N/A')}")
    
    # NetSales（売上高）の表示
    net_sales = latest_data.get('NetSales', 'N/A')
    if net_sales != 'N/A' and net_sales is not None:
        try:
            net_sales_value = float(net_sales) if isinstance(net_sales, (int, float, str)) else 0
            print(f"NetSales: {net_sales_value:,.0f} 円")
        except (ValueError, TypeError):
            print(f"NetSales: {net_sales} 円")
    else:
        print("NetSales: データなし")

    # OrdinaryProfit
    ordinary_profit = latest_data.get('OrdinaryProfit', 'N/A')
    if ordinary_profit != 'N/A' and ordinary_profit is not None:
        try:
            ordinary_profit_value = float(ordinary_profit) if isinstance(ordinary_profit, (int, float, str)) else 0
            print(f"OrdinaryProfit: {ordinary_profit_value:,.0f} 円")
        except (ValueError, TypeError):
            print(f"OrdinaryProfit: {ordinary_profit} 円")
    else:
        print("OrdinaryProfit: データなし")


    # OrdinaryProfit（経常利益）の表示
    ordinary_profit = latest_data.get('OrdinaryProfit', 'N/A')
    if ordinary_profit != 'N/A' and ordinary_profit is not None:
        try:
            ordinary_profit_value = float(ordinary_profit) if isinstance(ordinary_profit, (int, float, str)) else 0
            print(f"OrdinaryProfit: {ordinary_profit_value:,.0f} 円")
        except (ValueError, TypeError):
            print(f"OrdinaryProfit: {ordinary_profit} 円")
    else:
        print("OrdinaryProfit: データなし")
    
    # Profit
    profit = latest_data.get('Profit', 'N/A')
    if profit != 'N/A' and profit is not None:
        try:
            profit_value = float(profit) if isinstance(profit, (int, float, str)) else 0
            print(f"Profit: {profit_value:,.0f} 円")
        except (ValueError, TypeError):
            print(f"Profit: {profit} 円")
    else:
        print("Profit: データなし")

    print(f"\n【基本情報】")
    print(f"DisclosedDate: {latest_data.get('DisclosedDate', 'N/A')}")
    print(f"FiscalYear: {latest_data.get('FiscalYear', 'N/A')}")
    print(f"FiscalPeriod: {latest_data.get('FiscalPeriod', 'N/A')}")
    
    print(f"\n【全データ件数】")
    print(f"取得した財務データ件数: {len(df)} 件")
    
    # 取得した全データを表示
    if len(df) > 1:
        print(f"\n【全{len(df)}件の指定項目データ】")
        print("-" * 50)
        for i, (_, row) in enumerate(df.iterrows()):
            print(f"\nデータ {i+1}:")
            print(f"  DisclosedDate: {row.get('DisclosedDate', 'N/A')}")
            print(f"  TypeOfDocument: {row.get('TypeOfDocument', 'N/A')}")
            print(f"  TypeOfCurrentPeriod: {row.get('TypeOfCurrentPeriod', 'N/A')}")
            
            # NetSales
            net_sales = row.get('NetSales', 'N/A')
            if net_sales != 'N/A' and net_sales is not None:
                try:
                    net_sales_value = float(net_sales) if isinstance(net_sales, (int, float, str)) else 0
                    print(f"  NetSales: {net_sales_value:,.0f} 円")
                except (ValueError, TypeError):
                    print(f"  NetSales: {net_sales} 円")
            else:
                print(f"  NetSales: データなし")

            # OperatingProfit
            operating_profit = row.get('OperatingProfit', 'N/A')
            if operating_profit != 'N/A' and operating_profit is not None:
                try:
                    operating_profit_value = float(operating_profit) if isinstance(operating_profit, (int, float, str)) else 0
                    print(f"  OperatingProfit: {operating_profit_value:,.0f} 円")
                except (ValueError, TypeError):
                    print(f"  OperatingProfit: {operating_profit} 円")
            else:
                print(f"  OperatingProfit: データなし")

            # OrdinaryProfit
            ordinary_profit = row.get('OrdinaryProfit', 'N/A')
            if ordinary_profit != 'N/A' and ordinary_profit is not None:
                try:
                    ordinary_profit_value = float(ordinary_profit) if isinstance(ordinary_profit, (int, float, str)) else 0
                    print(f"  OrdinaryProfit: {ordinary_profit_value:,.0f} 円")
                except (ValueError, TypeError):
                    print(f"  OrdinaryProfit: {ordinary_profit} 円")
            else:
                print(f"  OrdinaryProfit: データなし")

            # Profit
            profit = row.get('Profit', 'N/A')
            if profit != 'N/A' and profit is not None:
                try:
                    profit_value = float(profit) if isinstance(profit, (int, float, str)) else 0
                    print(f"  Profit: {profit_value:,.0f} 円")
                except (ValueError, TypeError):
                    print(f"  Profit: {profit} 円")
            else:
                print(f"  Profit: データなし")

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
    
    # 4文字の英数字かチェック（アルファベットも許可）
    if len(code) != 4:
        return False
    
    # 英数字のみかチェック
    if not code.isalnum():
        return False
        
    return True


def main():
    """メイン処理"""
    try:
        # 引数の確認
        if len(sys.argv) != 2:
            print("使用方法: python test_07_stock_statements.py <銘柄コード>")
            print("例: python test_07_stock_statements.py 7203")
            print("例: python test_07_stock_statements.py 228A")
            sys.exit(1)
        
        stock_code = sys.argv[1]
        
        # 銘柄コードの検証
        if not validate_stock_code(stock_code):
            print(f"エラー: 無効な銘柄コードです: {stock_code}")
            print("銘柄コードは4文字の英数字で入力してください。")
            sys.exit(1)
        
        # スクリプトのディレクトリを取得
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        token_file_path = project_root / "token.txt"
        
        print("J-Quants API 指定財務データ取得スクリプト")
        print("=" * 60)
        
        # APIキーの読み込み
        print(f"APIキーを読み込み中: {token_file_path}")
        refresh_token = load_api_key(token_file_path)
        print("APIキーの読み込み完了")
        
        # クライアントの初期化
        client = jquantsapi.Client(refresh_token=refresh_token)
        
        # 財務データの取得
        df = get_financial_statements(client, stock_code)
        
        # 結果の表示
        display_specific_financial_data(df, stock_code)
        
        print("\n処理が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
