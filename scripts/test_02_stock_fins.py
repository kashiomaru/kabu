#!/usr/bin/env python3
"""
J-Quants APIを使用して指定された銘柄コードの財務データを取得するスクリプト

使用方法:
    python test_02_stock_fins.py <銘柄コード>
    
例:
    python test_02_stock_fins.py 7203  # トヨタ自動車
    python test_02_stock_fins.py 6758  # ソニーグループ

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
        pandas.DataFrame: 財務データのデータフレーム
        
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


def extract_financial_metrics(df, code):
    """
    財務データから当期純利益と発行済み株式数を抽出する
    
    Args:
        df (pandas.DataFrame): 財務データのデータフレーム
        code (str): 銘柄コード
        
    Returns:
        dict: 抽出した財務指標
    """
    if df.empty:
        return None
    
    # 最新のデータを取得（DisclosedDateでソート済みのため最初の行）
    latest_data = df.iloc[0]
    
    # 財務指標の抽出
    metrics = {
        'code': code,
        'disclosed_date': latest_data.get('DisclosedDate', 'N/A'),
        'fiscal_year': latest_data.get('FiscalYear', 'N/A'),
        'fiscal_period': latest_data.get('FiscalPeriod', 'N/A'),
        'profit': latest_data.get('Profit', None),  # 当期純利益
        'issued_shares': latest_data.get('NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock', None),  # 発行済み株式数
        'earnings_per_share': latest_data.get('EarningsPerShare', None),  # 1株当たり純利益（APIから直接取得）
        'revenue': latest_data.get('NetSales', None),  # 売上高
        'total_assets': latest_data.get('TotalAssets', None),  # 総資産
        'total_liabilities': latest_data.get('TotalLiabilities', None),  # 総負債
        'net_assets': latest_data.get('NetAssets', None),  # 純資産
    }
    
    return metrics


def display_financial_metrics(metrics):
    """
    財務指標を表示する
    
    Args:
        metrics (dict): 財務指標の辞書
    """
    if not metrics:
        print("財務データが見つかりませんでした。")
        return
    
    print(f"\n銘柄コード: {metrics['code']}")
    print(f"開示日: {metrics['disclosed_date']}")
    print(f"会計年度: {metrics['fiscal_year']}")
    print(f"会計期間: {metrics['fiscal_period']}")
    print("=" * 80)
    
    # 主要な財務指標
    print("【主要財務指標】")
    if metrics['profit'] is not None:
        try:
            profit_value = float(metrics['profit']) if isinstance(metrics['profit'], (int, float, str)) else 0
            print(f"当期純利益: {profit_value:,.0f} 円")
        except (ValueError, TypeError):
            print(f"当期純利益: {metrics['profit']} 円")
    else:
        print("当期純利益: データなし")
    
    if metrics['issued_shares'] is not None:
        try:
            shares_value = float(metrics['issued_shares']) if isinstance(metrics['issued_shares'], (int, float, str)) else 0
            print(f"発行済み株式数: {shares_value:,.0f} 株")
        except (ValueError, TypeError):
            print(f"発行済み株式数: {metrics['issued_shares']} 株")
    else:
        print("発行済み株式数: データなし")
    
    print("\n【その他の財務指標】")
    if metrics['revenue'] is not None:
        try:
            revenue_value = float(metrics['revenue']) if isinstance(metrics['revenue'], (int, float, str)) else 0
            print(f"売上高: {revenue_value:,.0f} 円")
        except (ValueError, TypeError):
            print(f"売上高: {metrics['revenue']} 円")
    else:
        print("売上高: データなし")
    
    if metrics['total_assets'] is not None:
        try:
            assets_value = float(metrics['total_assets']) if isinstance(metrics['total_assets'], (int, float, str)) else 0
            print(f"総資産: {assets_value:,.0f} 円")
        except (ValueError, TypeError):
            print(f"総資産: {metrics['total_assets']} 円")
    else:
        print("総資産: データなし")
    
    if metrics['total_liabilities'] is not None:
        try:
            liabilities_value = float(metrics['total_liabilities']) if isinstance(metrics['total_liabilities'], (int, float, str)) else 0
            print(f"総負債: {liabilities_value:,.0f} 円")
        except (ValueError, TypeError):
            print(f"総負債: {metrics['total_liabilities']} 円")
    else:
        print("総負債: データなし")
    
    if metrics['net_assets'] is not None:
        try:
            net_assets_value = float(metrics['net_assets']) if isinstance(metrics['net_assets'], (int, float, str)) else 0
            print(f"純資産: {net_assets_value:,.0f} 円")
        except (ValueError, TypeError):
            print(f"純資産: {metrics['net_assets']} 円")
    else:
        print("純資産: データなし")
    
    # 1株当たりの指標を表示
    print(f"\n【1株当たり指標】")
    
    # APIから取得したEPSを優先的に表示
    if metrics['earnings_per_share'] is not None:
        try:
            eps_value = float(metrics['earnings_per_share']) if isinstance(metrics['earnings_per_share'], (int, float, str)) else 0
            print(f"1株当たり純利益(EPS): {eps_value:.2f} 円 (API)")
        except (ValueError, TypeError):
            print(f"1株当たり純利益(EPS): {metrics['earnings_per_share']} 円 (API)")
    else:
        # APIからEPSが取得できない場合は手動計算
        if metrics['profit'] is not None and metrics['issued_shares'] is not None:
            try:
                profit_value = float(metrics['profit']) if isinstance(metrics['profit'], (int, float, str)) else 0
                shares_value = float(metrics['issued_shares']) if isinstance(metrics['issued_shares'], (int, float, str)) else 0
                
                if shares_value > 0:
                    eps = profit_value / shares_value  # 円単位で直接計算
                    print(f"1株当たり純利益(EPS): {eps:.2f} 円 (手動計算)")
                else:
                    print("1株当たり純利益(EPS): 計算不可（発行済み株式数が0以下）")
            except (ValueError, TypeError):
                print("1株当たり純利益(EPS): 計算不可（データ形式エラー）")
        else:
            print("1株当たり純利益(EPS): データなし")
    
    print(f"\n全列の情報:")
    print("利用可能な財務データ項目:")
    print("- Profit: 当期純利益")
    print("- NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock: 発行済み株式数")
    print("- NetSales: 売上高")
    print("- TotalAssets: 総資産")
    print("- TotalLiabilities: 総負債")
    print("- NetAssets: 純資産")


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
            print("使用方法: python test_02_stock_fins.py <銘柄コード>")
            print("例: python test_02_stock_fins.py 7203")
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
        
        print("J-Quants API 財務データ取得スクリプト")
        print("=" * 50)
        
        # APIキーの読み込み
        print(f"APIキーを読み込み中: {token_file_path}")
        refresh_token = load_api_key(token_file_path)
        print("APIキーの読み込み完了")
        
        # クライアントの初期化
        client = jquantsapi.Client(refresh_token=refresh_token)
        
        # 財務データの取得
        df = get_financial_statements(client, stock_code)
        
        # 財務指標の抽出
        metrics = extract_financial_metrics(df, stock_code)
        
        # 結果の表示
        display_financial_metrics(metrics)
        
        print("\n処理が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
