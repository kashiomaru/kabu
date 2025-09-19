#!/usr/bin/env python3
"""
J-Quants APIを使用して指定された銘柄コードの年次財務データ（TypeOfCurrentPeriod = 'FY'）を取得・表示するスクリプト

使用方法:
    python test_08_stock_statements.py <銘柄コード>
    
例:
    python test_08_stock_statements.py 7203  # トヨタ自動車
    python test_08_stock_statements.py 6758  # ソニーグループ
    python test_08_stock_statements.py 228A  # アルファベット付き銘柄コード

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


def filter_annual_financial_data(df):
    """
    年次財務データ（TypeOfCurrentPeriod = 'FY' かつ TypeOfDocument に 'FinancialStatements' を含む）をフィルタリングする
    
    Args:
        df (pandas.DataFrame): 財務データのデータフレーム
        
    Returns:
        pandas.DataFrame: 年次財務データのみのデータフレーム
    """
    if df.empty:
        return df
    
    # 年次データかつ財務諸表のデータのみをフィルタリング
    annual_data = df[
        (df['TypeOfCurrentPeriod'] == 'FY') & 
        (df['TypeOfDocument'].str.contains('FinancialStatements', na=False))
    ].copy()
    
    print(f"年次財務データ（TypeOfCurrentPeriod = 'FY' かつ FinancialStatements）: {len(annual_data)} 件")
    
    return annual_data


def determine_profit_type(df):
    """
    経常利益または営業利益のどちらを使用するかを決定する
    
    Args:
        df (pandas.DataFrame): 年次財務データのデータフレーム
        
    Returns:
        str: 使用する利益タイプ（'ordinary' または 'operating'）
    """
    if df.empty:
        return 'ordinary'
    
    # 経常利益のデータが存在するかチェック
    ordinary_profit_available = 0
    operating_profit_available = 0
    
    for _, row in df.iterrows():
        ordinary_profit = row.get('OrdinaryProfit')
        operating_profit = row.get('OperatingProfit')
        
        if ordinary_profit is not None and ordinary_profit != '' and ordinary_profit != 'N/A':
            try:
                float(ordinary_profit)
                ordinary_profit_available += 1
            except (ValueError, TypeError):
                pass
        
        if operating_profit is not None and operating_profit != '' and operating_profit != 'N/A':
            try:
                float(operating_profit)
                operating_profit_available += 1
            except (ValueError, TypeError):
                pass
    
    # 経常利益が全年度で利用可能な場合は経常利益を使用
    if ordinary_profit_available == len(df):
        return 'ordinary'
    else:
        return 'operating'


def calculate_profit_growth_rates(df, profit_type):
    """
    利益の上昇率を計算する
    
    Args:
        df (pandas.DataFrame): 年次財務データのデータフレーム（最新順にソート済み）
        profit_type (str): 使用する利益タイプ（'ordinary' または 'operating'）
        
    Returns:
        list: 各年度の上昇率のリスト（最新年度から順）
    """
    if df.empty or len(df) < 2:
        return []
    
    growth_rates = []
    profit_column = 'OrdinaryProfit' if profit_type == 'ordinary' else 'OperatingProfit'
    
    for i in range(len(df)):
        # 一番古い年度（最後のインデックス）は前年度がないので計算不可
        if i == len(df) - 1:
            growth_rates.append(None)
            continue
        
        # 現在年度と前年度の比較
        current_row = df.iloc[i]      # 現在の年度（例：2025年）
        previous_row = df.iloc[i+1]   # 前年度（例：2024年）
        
        current_profit = current_row.get(profit_column)
        previous_profit = previous_row.get(profit_column)
        
        # 数値に変換
        try:
            current_value = float(current_profit) if current_profit is not None and current_profit != '' and current_profit != 'N/A' else None
            previous_value = float(previous_profit) if previous_profit is not None and previous_profit != '' and previous_profit != 'N/A' else None
        except (ValueError, TypeError):
            growth_rates.append(None)
            continue
        
        # 上昇率を計算
        if current_value is not None and previous_value is not None and previous_value != 0:
            growth_rate = ((current_value - previous_value) / abs(previous_value)) * 100
            growth_rates.append(round(growth_rate, 1))
        else:
            growth_rates.append(None)
    
    return growth_rates


def display_annual_financial_data(df, code):
    """
    年次財務データを表示する
    
    Args:
        df (pandas.DataFrame): 年次財務データのデータフレーム
        code (str): 銘柄コード
    """
    if df.empty:
        print(f"銘柄コード {code} の年次財務データが見つかりませんでした。")
        return
    
    print(f"\n銘柄コード: {code}")
    print("=" * 80)
    print("【年次財務データ一覧】")
    print("-" * 50)
    
    # 年次財務データの件数表示
    print(f"取得した年次財務データ件数: {len(df)} 件")
    
    # 11年分のデータに制限
    df_11years = df.head(11) if len(df) > 11 else df
    print(f"分析対象年数: {len(df_11years)} 年（最新11年分）")
    
    # 使用する利益タイプを決定
    profit_type = determine_profit_type(df_11years)
    profit_name = "経常利益" if profit_type == 'ordinary' else "営業利益"
    print(f"使用する利益指標: {profit_name}")
    
    # 上昇率を計算
    growth_rates = calculate_profit_growth_rates(df_11years, profit_type)
    
    # 各年度の基本情報を表示
    print(f"\n【各年度の基本情報】")
    print("-" * 50)
    
    for i, (_, row) in enumerate(df_11years.iterrows()):
        print(f"\n年度 {i+1}:")
        print(f"  開示日: {row.get('DisclosedDate', 'N/A')}")
        print(f"  会計年度: {row.get('FiscalYear', 'N/A')}")
        print(f"  会計期間: {row.get('FiscalPeriod', 'N/A')}")
        print(f"  文書タイプ: {row.get('TypeOfDocument', 'N/A')}")
        print(f"  現在期間タイプ: {row.get('TypeOfCurrentPeriod', 'N/A')}")
        print(f"  前期間タイプ: {row.get('TypeOfPreviousPeriod', 'N/A')}")
        
        # 主要財務指標
        print(f"  【主要財務指標】")
        
        # 売上高
        net_sales = row.get('NetSales', 'N/A')
        if net_sales != 'N/A' and net_sales is not None:
            try:
                net_sales_value = float(net_sales) if isinstance(net_sales, (int, float, str)) else 0
                print(f"    売上高: {net_sales_value:,.0f} 円")
            except (ValueError, TypeError):
                print(f"    売上高: {net_sales} 円")
        else:
            print(f"    売上高: データなし")
        
        # 営業利益
        operating_profit = row.get('OperatingProfit', 'N/A')
        if operating_profit != 'N/A' and operating_profit is not None:
            try:
                operating_profit_value = float(operating_profit) if isinstance(operating_profit, (int, float, str)) else 0
                print(f"    営業利益: {operating_profit_value:,.0f} 円")
            except (ValueError, TypeError):
                print(f"    営業利益: {operating_profit} 円")
        else:
            print(f"    営業利益: データなし")
        
        # 経常利益
        ordinary_profit = row.get('OrdinaryProfit', 'N/A')
        if ordinary_profit != 'N/A' and ordinary_profit is not None:
            try:
                ordinary_profit_value = float(ordinary_profit) if isinstance(ordinary_profit, (int, float, str)) else 0
                print(f"    経常利益: {ordinary_profit_value:,.0f} 円")
            except (ValueError, TypeError):
                print(f"    経常利益: {ordinary_profit} 円")
        else:
            print(f"    経常利益: データなし")
        
        # 当期純利益
        profit = row.get('Profit', 'N/A')
        if profit != 'N/A' and profit is not None:
            try:
                profit_value = float(profit) if isinstance(profit, (int, float, str)) else 0
                print(f"    当期純利益: {profit_value:,.0f} 円")
            except (ValueError, TypeError):
                print(f"    当期純利益: {profit} 円")
        else:
            print(f"    当期純利益: データなし")
        
        # 純資産
        equity = row.get('Equity', 'N/A')
        if equity != 'N/A' and equity is not None:
            try:
                equity_value = float(equity) if isinstance(equity, (int, float, str)) else 0
                print(f"    純資産: {equity_value:,.0f} 円")
            except (ValueError, TypeError):
                print(f"    純資産: {equity} 円")
        else:
            print(f"    純資産: データなし")
        
        # 上昇率の表示
        if i < len(growth_rates):
            growth_rate = growth_rates[i]
            if growth_rate is not None:
                print(f"    {profit_name}上昇率: {growth_rate:+.1f}%")
            else:
                print(f"    {profit_name}上昇率: 計算不可")
    
    # 営業利益の一覧表示
    print(f"\n【{profit_name}一覧】")
    print("-" * 50)
    print(f"{'年度':<8} {'営業利益':<20} {'上昇率':<10}")
    print("-" * 50)
    
    for i, (_, row) in enumerate(df_11years.iterrows()):
        # 年度の表示
        fiscal_year = row.get('FiscalYear', 'N/A')
        disclosed_date = row.get('DisclosedDate', 'N/A')
        
        if fiscal_year != 'N/A' and fiscal_year is not None:
            year_display = f"{fiscal_year}年"
        else:
            try:
                if isinstance(disclosed_date, str):
                    year_display = f"{disclosed_date[:4]}年"
                else:
                    year_display = f"{disclosed_date.year}年"
            except:
                year_display = "N/A年"
        
        # 営業利益の表示
        operating_profit = row.get('OperatingProfit', 'N/A')
        if operating_profit != 'N/A' and operating_profit is not None:
            try:
                operating_profit_value = float(operating_profit) if isinstance(operating_profit, (int, float, str)) else 0
                profit_display = f"{operating_profit_value:,.0f}円"
            except (ValueError, TypeError):
                profit_display = f"{operating_profit}円"
        else:
            profit_display = "データなし"
        
        # 上昇率の表示
        if i < len(growth_rates):
            growth_rate = growth_rates[i]
            if growth_rate is not None:
                rate_display = f"{growth_rate:+.1f}%"
            else:
                rate_display = "計算不可"
        else:
            rate_display = "計算不可"
        
        print(f"{year_display:<8} {profit_display:<20} {rate_display:<10}")
    
    # 上昇率の統計情報
    print(f"\n【{profit_name}上昇率の統計】")
    print("-" * 50)
    
    valid_growth_rates = [rate for rate in growth_rates if rate is not None]  # 全年度を含める
    
    if valid_growth_rates:
        print(f"計算可能な上昇率: {len(valid_growth_rates)} 年分")
        print(f"平均上昇率: {sum(valid_growth_rates) / len(valid_growth_rates):.1f}%")
        print(f"最高上昇率: {max(valid_growth_rates):.1f}%")
        print(f"最低上昇率: {min(valid_growth_rates):.1f}%")
        
        # 上昇率の年次推移
        print(f"\n{profit_name}上昇率の年次推移:")
        for i, rate in enumerate(growth_rates):  # 全年度を含める
            if rate is not None:
                # 会計年度が利用可能な場合は会計年度を、そうでなければ開示日を使用
                fiscal_year = df_11years.iloc[i].get('FiscalYear', 'N/A')
                disclosed_date = df_11years.iloc[i].get('DisclosedDate', 'N/A')
                
                if fiscal_year != 'N/A' and fiscal_year is not None:
                    year_display = f"{fiscal_year}年"
                else:
                    # 開示日から年を抽出
                    try:
                        if isinstance(disclosed_date, str):
                            year_display = f"{disclosed_date[:4]}年"
                        else:
                            year_display = f"{disclosed_date.year}年"
                    except:
                        year_display = "N/A年"
                
                print(f"  {year_display}: {rate:+.1f}%")
            else:
                # 会計年度が利用可能な場合は会計年度を、そうでなければ開示日を使用
                fiscal_year = df_11years.iloc[i].get('FiscalYear', 'N/A')
                disclosed_date = df_11years.iloc[i].get('DisclosedDate', 'N/A')
                
                if fiscal_year != 'N/A' and fiscal_year is not None:
                    year_display = f"{fiscal_year}年"
                else:
                    # 開示日から年を抽出
                    try:
                        if isinstance(disclosed_date, str):
                            year_display = f"{disclosed_date[:4]}年"
                        else:
                            year_display = f"{disclosed_date.year}年"
                    except:
                        year_display = "N/A年"
                
                print(f"  {year_display}: 計算不可")
    else:
        print(f"{profit_name}上昇率の計算に必要なデータが不足しています。")
    
    # 統計情報
    print(f"\n【統計情報】")
    print("-" * 50)
    
    # 会計年度の範囲
    if 'FiscalYear' in df_11years.columns:
        fiscal_years = df_11years['FiscalYear'].dropna().unique()
        if len(fiscal_years) > 0:
            fiscal_years_sorted = sorted(fiscal_years)
            print(f"会計年度範囲: {min(fiscal_years_sorted)} ～ {max(fiscal_years_sorted)}")
            print(f"取得年度数: {len(fiscal_years_sorted)} 年度")
    
    # 文書タイプの分布
    if 'TypeOfDocument' in df_11years.columns:
        doc_types = df_11years['TypeOfDocument'].value_counts()
        print(f"\n文書タイプ分布:")
        for doc_type, count in doc_types.items():
            print(f"  {doc_type}: {count} 件")


def validate_stock_code(code):
    """
    銘柄コードの形式を検証する（アルファベット含む4文字の英数字に対応）
    
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
            print("使用方法: python test_08_stock_statements.py <銘柄コード>")
            print("例: python test_08_stock_statements.py 7203")
            print("例: python test_08_stock_statements.py 228A")
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
        
        print("J-Quants API 年次財務データ取得スクリプト")
        print("=" * 60)
        
        # APIキーの読み込み
        print(f"APIキーを読み込み中: {token_file_path}")
        refresh_token = load_api_key(token_file_path)
        print("APIキーの読み込み完了")
        
        # クライアントの初期化
        client = jquantsapi.Client(refresh_token=refresh_token)
        
        # 財務データの取得
        df = get_financial_statements(client, stock_code)
        
        if df.empty:
            print("エラー: 財務データが取得できませんでした")
            sys.exit(1)
        
        # 年次財務データのフィルタリング
        annual_df = filter_annual_financial_data(df)
        
        # 結果の表示
        display_annual_financial_data(annual_df, stock_code)
        
        print("\n処理が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
