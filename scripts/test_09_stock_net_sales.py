#!/usr/bin/env python3
"""
J-Quants APIを使用して指定された銘柄コードの過去2年分の財務データから売上高・利益を取得・表示するスクリプト

使用方法:
    python test_09_stock_net_sales.py <銘柄コード>
    
例:
    python test_09_stock_net_sales.py 7203  # トヨタ自動車
    python test_09_stock_net_sales.py 6758  # ソニーグループ
    python test_09_stock_net_sales.py 228A  # アルファベット付き銘柄コード

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
            
            # 財務諸表のデータのみをフィルタリング
            financial_data = filter_financial_statements_data(financial_data)
            
            if not financial_data.empty:
                # DisclosedDateでソート（最新が最初に来るように降順）
                if 'DisclosedDate' in financial_data.columns:
                    financial_data['DisclosedDate'] = pd.to_datetime(financial_data['DisclosedDate'])
                    financial_data = financial_data.sort_values('DisclosedDate', ascending=False).reset_index(drop=True)
                return financial_data
            else:
                print(f"銘柄コード {code} の財務諸表データが見つかりませんでした")
                return pd.DataFrame()
        else:
            print(f"銘柄コード {code} の財務データが見つかりませんでした")
            return pd.DataFrame()
        
    except Exception as e:
        raise Exception(f"財務データの取得中にエラーが発生しました: {e}")


def filter_financial_statements_data(df):
    """
    財務諸表のデータのみをフィルタリングする（TypeOfDocument に 'FinancialStatements' を含む）
    
    Args:
        df (pandas.DataFrame): 財務データのデータフレーム
        
    Returns:
        pandas.DataFrame: 財務諸表データのみのデータフレーム
    """
    if df.empty:
        return df
    
    # 財務諸表のデータのみをフィルタリング
    financial_statements_data = df[
        df['TypeOfDocument'].str.contains('FinancialStatements', na=False)
    ].copy()
    
    print(f"財務諸表データ（FinancialStatements）: {len(financial_statements_data)} 件")
    
    return financial_statements_data


def filter_past_3years_data(df):
    """
    過去3年分の財務データをフィルタリングする（期間中値計算のため）
    
    Args:
        df (pandas.DataFrame): 財務データのデータフレーム
        
    Returns:
        pandas.DataFrame: 過去3年分の財務データのデータフレーム
    """
    if df.empty:
        return df
    
    # 現在の日付から3年前の日付を計算
    current_date = datetime.now()
    three_years_ago = current_date - timedelta(days=1095)  # 3年 = 1095日
    
    # DisclosedDateでフィルタリング（過去3年分）
    past_3years_data = df[df['DisclosedDate'] >= three_years_ago].copy()
    
    print(f"過去3年分の財務データ: {len(past_3years_data)} 件")
    
    return past_3years_data


def determine_profit_type(df):
    """
    経常利益または営業利益のどちらを使用するかを決定する
    
    Args:
        df (pandas.DataFrame): 財務データのデータフレーム
        
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
    
    # 経常利益が1つでも取得できる場合は経常利益を使用
    if ordinary_profit_available > 0:
        return 'ordinary'
    else:
        return 'operating'


def calculate_period_values(df, profit_type):
    """
    累積値から期間中値を計算する
    
    Args:
        df (pandas.DataFrame): 過去3年分の財務データのデータフレーム（最新順にソート済み）
        profit_type (str): 使用する利益タイプ（'ordinary' または 'operating'）
        
    Returns:
        pandas.DataFrame: 期間中値を計算したデータフレーム（最新2年分）
    """
    if df.empty:
        return df
    
    # データフレームをコピー
    result_df = df.copy()
    
    # 利益カラム名を決定
    profit_column = 'OrdinaryProfit' if profit_type == 'ordinary' else 'OperatingProfit'
    
    # 期間中値を計算するための新しいカラムを追加
    result_df['PeriodNetSales'] = None
    result_df['PeriodProfit'] = None
    
    # 各データに対して期間中値を計算
    for i, (_, row) in enumerate(result_df.iterrows()):
        period_type = row.get('TypeOfCurrentPeriod', '')
        
        # 売上高の期間中値計算
        current_sales = row.get('NetSales')
        if current_sales is not None and current_sales != '' and current_sales != 'N/A':
            try:
                current_sales_value = float(current_sales)
                
                if period_type == '1Q':
                    # 1Qはそのまま（累積値）
                    result_df.at[i, 'PeriodNetSales'] = current_sales_value
                else:
                    # 2Q, 3Q, FYは前の期間の値を引く
                    previous_sales = find_previous_period_sales(result_df, i, period_type)
                    if previous_sales is not None:
                        period_sales = current_sales_value - previous_sales
                        result_df.at[i, 'PeriodNetSales'] = period_sales
                    else:
                        result_df.at[i, 'PeriodNetSales'] = current_sales_value  # 前の期間が見つからない場合は累積値
            except (ValueError, TypeError):
                result_df.at[i, 'PeriodNetSales'] = None
        
        # 利益の期間中値計算
        current_profit = row.get(profit_column)
        if current_profit is not None and current_profit != '' and current_profit != 'N/A':
            try:
                current_profit_value = float(current_profit)
                
                if period_type == '1Q':
                    # 1Qはそのまま（累積値）
                    result_df.at[i, 'PeriodProfit'] = current_profit_value
                else:
                    # 2Q, 3Q, FYは前の期間の値を引く
                    previous_profit = find_previous_period_profit(result_df, i, period_type, profit_column)
                    if previous_profit is not None:
                        period_profit = current_profit_value - previous_profit
                        result_df.at[i, 'PeriodProfit'] = period_profit
                    else:
                        result_df.at[i, 'PeriodProfit'] = current_profit_value  # 前の期間が見つからない場合は累積値
            except (ValueError, TypeError):
                result_df.at[i, 'PeriodProfit'] = None
    
    # 最新2年分のデータのみを返す
    current_date = datetime.now()
    two_years_ago = current_date - timedelta(days=730)  # 2年 = 730日
    latest_2years_df = result_df[result_df['DisclosedDate'] >= two_years_ago].copy()
    
    print(f"期間中値計算完了: 最新2年分 {len(latest_2years_df)} 件")
    
    return latest_2years_df


def find_previous_period_sales(df, current_index, period_type):
    """
    指定された期間の前の期間の売上高を取得する
    
    Args:
        df (pandas.DataFrame): 財務データのデータフレーム
        current_index (int): 現在のデータのインデックス
        period_type (str): 現在の期間タイプ
        
    Returns:
        float: 前の期間の売上高（見つからない場合はNone）
    """
    if period_type == '2Q':
        # 2Qの前は1Q
        target_period = '1Q'
    elif period_type == '3Q':
        # 3Qの前は2Q
        target_period = '2Q'
    elif period_type == 'FY':
        # FYの前は3Q
        target_period = '3Q'
    else:
        return None
    
    # 現在のデータより後のデータ（古いデータ）から前の期間を探す
    for i in range(current_index + 1, len(df)):
        row = df.iloc[i]
        if row.get('TypeOfCurrentPeriod') == target_period:
            sales = row.get('NetSales')
            if sales is not None and sales != '' and sales != 'N/A':
                try:
                    return float(sales)
                except (ValueError, TypeError):
                    continue
    
    return None


def find_previous_period_profit(df, current_index, period_type, profit_column):
    """
    指定された期間の前の期間の利益を取得する
    
    Args:
        df (pandas.DataFrame): 財務データのデータフレーム
        current_index (int): 現在のデータのインデックス
        period_type (str): 現在の期間タイプ
        profit_column (str): 利益カラム名
        
    Returns:
        float: 前の期間の利益（見つからない場合はNone）
    """
    if period_type == '2Q':
        # 2Qの前は1Q
        target_period = '1Q'
    elif period_type == '3Q':
        # 3Qの前は2Q
        target_period = '2Q'
    elif period_type == 'FY':
        # FYの前は3Q
        target_period = '3Q'
    else:
        return None
    
    # 現在のデータより後のデータ（古いデータ）から前の期間を探す
    for i in range(current_index + 1, len(df)):
        row = df.iloc[i]
        if row.get('TypeOfCurrentPeriod') == target_period:
            profit = row.get(profit_column)
            if profit is not None and profit != '' and profit != 'N/A':
                try:
                    return float(profit)
                except (ValueError, TypeError):
                    continue
    
    return None


def find_previous_year_same_period(df, current_index, period_type):
    """
    前年度の同じ期間タイプのデータを取得する
    
    Args:
        df (pandas.DataFrame): 財務データのデータフレーム
        current_index (int): 現在のデータのインデックス
        period_type (str): 現在の期間タイプ
        
    Returns:
        pandas.Series: 前年度の同じ期間タイプのデータ（見つからない場合はNone）
    """
    if period_type not in ['1Q', '2Q', '3Q', 'FY']:
        return None
    
    # 現在のデータの開示日を取得
    current_date = df.iloc[current_index].get('DisclosedDate')
    if current_date is None:
        return None
    
    # 現在の年を取得
    try:
        if isinstance(current_date, str):
            current_year = int(current_date[:4])
        else:
            current_year = current_date.year
    except (ValueError, TypeError):
        return None
    
    # 前年度の同じ期間タイプを探す
    target_year = current_year - 1
    
    for i in range(current_index + 1, len(df)):
        row = df.iloc[i]
        row_period_type = row.get('TypeOfCurrentPeriod')
        row_date = row.get('DisclosedDate')
        
        if row_period_type == period_type and row_date is not None:
            try:
                if isinstance(row_date, str):
                    row_year = int(row_date[:4])
                else:
                    row_year = row_date.year
                
                if row_year == target_year:
                    return row
            except (ValueError, TypeError):
                continue
    
    return None


def calculate_growth_rates(df):
    """
    前年同期比上昇率を計算する
    
    Args:
        df (pandas.DataFrame): 期間中値を計算したデータフレーム
        
    Returns:
        pandas.DataFrame: 上昇率を追加したデータフレーム
    """
    if df.empty:
        return df
    
    # データフレームをコピー
    result_df = df.copy()
    
    # 上昇率を計算するための新しいカラムを追加
    result_df['SalesGrowthRate'] = None
    result_df['ProfitGrowthRate'] = None
    
    # 各データに対して上昇率を計算
    for i, (_, row) in enumerate(result_df.iterrows()):
        period_type = row.get('TypeOfCurrentPeriod', '')
        
        # 前年度の同じ期間タイプを検索
        previous_year_data = find_previous_year_same_period(result_df, i, period_type)
        
        if previous_year_data is not None:
            # 売上高の上昇率計算
            current_sales = row.get('PeriodNetSales')
            previous_sales = previous_year_data.get('PeriodNetSales')
            
            if (current_sales is not None and previous_sales is not None and 
                current_sales != 0 and previous_sales != 0):
                try:
                    sales_growth_rate = ((current_sales - previous_sales) / abs(previous_sales)) * 100
                    result_df.at[i, 'SalesGrowthRate'] = round(sales_growth_rate, 1)
                except (ValueError, TypeError, ZeroDivisionError):
                    result_df.at[i, 'SalesGrowthRate'] = None
            
            # 利益の上昇率計算
            current_profit = row.get('PeriodProfit')
            previous_profit = previous_year_data.get('PeriodProfit')
            
            if (current_profit is not None and previous_profit is not None and 
                current_profit != 0 and previous_profit != 0):
                try:
                    profit_growth_rate = ((current_profit - previous_profit) / abs(previous_profit)) * 100
                    result_df.at[i, 'ProfitGrowthRate'] = round(profit_growth_rate, 1)
                except (ValueError, TypeError, ZeroDivisionError):
                    result_df.at[i, 'ProfitGrowthRate'] = None
    
    print(f"前年同期比上昇率計算完了")
    
    return result_df


def display_net_sales_and_profit_data(df, code, profit_name):
    """
    売上高・利益データを表示する（期間中値）
    
    Args:
        df (pandas.DataFrame): 期間中値を計算したデータフレーム（最新2年分）
        code (str): 銘柄コード
        profit_name (str): 利益指標名
    """
    if df.empty:
        print(f"銘柄コード {code} の過去2年分の財務データが見つかりませんでした。")
        return
    
    print(f"\n銘柄コード: {code}")
    print("=" * 80)
    print("【過去2年分の期間中売上高・利益データ】")
    print("-" * 50)
    
    # 過去2年分のデータ件数表示
    print(f"取得した財務データ件数: {len(df)} 件")
    print(f"使用する利益指標: {profit_name}")
    
    # 各データの基本情報を表示
    print(f"\n【各データの詳細】")
    print("-" * 50)
    
    for i, (_, row) in enumerate(df.iterrows()):
        print(f"\nデータ {i+1}:")
        print(f"  開示日: {row.get('DisclosedDate', 'N/A')}")
        print(f"  文書タイプ: {row.get('TypeOfDocument', 'N/A')}")
        print(f"  現在期間タイプ: {row.get('TypeOfCurrentPeriod', 'N/A')}")
        
        # 累積売上高の表示
        net_sales = row.get('NetSales', 'N/A')
        if net_sales != 'N/A' and net_sales is not None:
            try:
                net_sales_value = float(net_sales) if isinstance(net_sales, (int, float, str)) else 0
                print(f"  累積売上高: {net_sales_value:,.0f} 円")
            except (ValueError, TypeError):
                print(f"  累積売上高: {net_sales} 円")
        else:
            print(f"  累積売上高: データなし")
        
        # 期間中売上高の表示
        period_sales = row.get('PeriodNetSales')
        if period_sales is not None:
            try:
                period_sales_value = float(period_sales) if isinstance(period_sales, (int, float, str)) else 0
                print(f"  期間中売上高: {period_sales_value:,.0f} 円")
            except (ValueError, TypeError):
                print(f"  期間中売上高: {period_sales} 円")
        else:
            print(f"  期間中売上高: データなし")
        
        # 累積利益の表示
        profit_column = 'OrdinaryProfit' if profit_name == "経常利益" else 'OperatingProfit'
        profit_value = row.get(profit_column, 'N/A')
        
        if profit_value != 'N/A' and profit_value is not None:
            try:
                profit_float = float(profit_value) if isinstance(profit_value, (int, float, str)) else 0
                print(f"  累積{profit_name}: {profit_float:,.0f} 円")
            except (ValueError, TypeError):
                print(f"  累積{profit_name}: {profit_value} 円")
        else:
            print(f"  累積{profit_name}: データなし")
        
        # 期間中利益の表示
        period_profit = row.get('PeriodProfit')
        if period_profit is not None:
            try:
                period_profit_value = float(period_profit) if isinstance(period_profit, (int, float, str)) else 0
                print(f"  期間中{profit_name}: {period_profit_value:,.0f} 円")
            except (ValueError, TypeError):
                print(f"  期間中{profit_name}: {period_profit} 円")
        else:
            print(f"  期間中{profit_name}: データなし")
        
        # 前年同期比上昇率の表示
        sales_growth = row.get('SalesGrowthRate')
        if sales_growth is not None:
            print(f"  売上高前年同期比: {sales_growth:+.1f}%")
        else:
            print(f"  売上高前年同期比: 計算不可")
        
        profit_growth = row.get('ProfitGrowthRate')
        if profit_growth is not None:
            print(f"  {profit_name}前年同期比: {profit_growth:+.1f}%")
        else:
            print(f"  {profit_name}前年同期比: 計算不可")
    
    # 期間中売上高・利益の一覧表示
    print(f"\n【期間中{profit_name}一覧】")
    print("-" * 120)
    print(f"{'開示日':<12} {'期間タイプ':<8} {'累積売上高':<20} {'期間中売上高':<20} {'累積' + profit_name:<20} {'期間中' + profit_name:<20} {'売上高前年同期比':<15} {profit_name + '前年同期比':<15}")
    print("-" * 120)
    
    for i, (_, row) in enumerate(df.iterrows()):
        # 開示日の表示
        disclosed_date = row.get('DisclosedDate', 'N/A')
        if disclosed_date != 'N/A':
            try:
                if isinstance(disclosed_date, str):
                    date_display = disclosed_date[:10]  # YYYY-MM-DD形式
                else:
                    date_display = disclosed_date.strftime('%Y-%m-%d')
            except:
                date_display = str(disclosed_date)
        else:
            date_display = "N/A"
        
        # 期間タイプの表示
        period_type = row.get('TypeOfCurrentPeriod', 'N/A')
        period_display = str(period_type) if period_type != 'N/A' else "N/A"
        
        # 累積売上高の表示
        net_sales = row.get('NetSales', 'N/A')
        if net_sales != 'N/A' and net_sales is not None:
            try:
                net_sales_value = float(net_sales) if isinstance(net_sales, (int, float, str)) else 0
                cumulative_sales_display = f"{net_sales_value:,.0f}円"
            except (ValueError, TypeError):
                cumulative_sales_display = f"{net_sales}円"
        else:
            cumulative_sales_display = "データなし"
        
        # 期間中売上高の表示
        period_sales = row.get('PeriodNetSales')
        if period_sales is not None:
            try:
                period_sales_value = float(period_sales) if isinstance(period_sales, (int, float, str)) else 0
                period_sales_display = f"{period_sales_value:,.0f}円"
            except (ValueError, TypeError):
                period_sales_display = f"{period_sales}円"
        else:
            period_sales_display = "データなし"
        
        # 累積利益の表示
        profit_column = 'OrdinaryProfit' if profit_name == "経常利益" else 'OperatingProfit'
        profit_value = row.get(profit_column, 'N/A')
        
        if profit_value != 'N/A' and profit_value is not None:
            try:
                profit_float = float(profit_value) if isinstance(profit_value, (int, float, str)) else 0
                cumulative_profit_display = f"{profit_float:,.0f}円"
            except (ValueError, TypeError):
                cumulative_profit_display = f"{profit_value}円"
        else:
            cumulative_profit_display = "データなし"
        
        # 期間中利益の表示
        period_profit = row.get('PeriodProfit')
        if period_profit is not None:
            try:
                period_profit_value = float(period_profit) if isinstance(period_profit, (int, float, str)) else 0
                period_profit_display = f"{period_profit_value:,.0f}円"
            except (ValueError, TypeError):
                period_profit_display = f"{period_profit}円"
        else:
            period_profit_display = "データなし"
        
        # 売上高前年同期比の表示
        sales_growth = row.get('SalesGrowthRate')
        if sales_growth is not None:
            sales_growth_display = f"{sales_growth:+.1f}%"
        else:
            sales_growth_display = "計算不可"
        
        # 利益前年同期比の表示
        profit_growth = row.get('ProfitGrowthRate')
        if profit_growth is not None:
            profit_growth_display = f"{profit_growth:+.1f}%"
        else:
            profit_growth_display = "計算不可"
        
        print(f"{date_display:<12} {period_display:<8} {cumulative_sales_display:<20} {period_sales_display:<20} {cumulative_profit_display:<20} {period_profit_display:<20} {sales_growth_display:<15} {profit_growth_display:<15}")
    
    # 統計情報
    print(f"\n【統計情報】")
    print("-" * 50)
    
    # 期間タイプの分布
    if 'TypeOfCurrentPeriod' in df.columns:
        period_types = df['TypeOfCurrentPeriod'].value_counts()
        print(f"\n期間タイプ分布:")
        for period_type, count in period_types.items():
            print(f"  {period_type}: {count} 件")
    
    # 文書タイプの分布
    if 'TypeOfDocument' in df.columns:
        doc_types = df['TypeOfDocument'].value_counts()
        print(f"\n文書タイプ分布:")
        for doc_type, count in doc_types.items():
            print(f"  {doc_type}: {count} 件")
    
    # 上昇率の統計情報
    print(f"\n【前年同期比上昇率の統計】")
    print("-" * 50)
    
    # 売上高上昇率の統計
    valid_sales_growth_rates = [rate for rate in df['SalesGrowthRate'] if rate is not None]
    if valid_sales_growth_rates:
        print(f"売上高前年同期比計算可能: {len(valid_sales_growth_rates)} 件")
        print(f"売上高平均上昇率: {sum(valid_sales_growth_rates) / len(valid_sales_growth_rates):.1f}%")
        print(f"売上高最高上昇率: {max(valid_sales_growth_rates):.1f}%")
        print(f"売上高最低上昇率: {min(valid_sales_growth_rates):.1f}%")
    else:
        print("売上高前年同期比の計算に必要なデータが不足しています。")
    
    # 利益上昇率の統計
    valid_profit_growth_rates = [rate for rate in df['ProfitGrowthRate'] if rate is not None]
    if valid_profit_growth_rates:
        print(f"\n{profit_name}前年同期比計算可能: {len(valid_profit_growth_rates)} 件")
        print(f"{profit_name}平均上昇率: {sum(valid_profit_growth_rates) / len(valid_profit_growth_rates):.1f}%")
        print(f"{profit_name}最高上昇率: {max(valid_profit_growth_rates):.1f}%")
        print(f"{profit_name}最低上昇率: {min(valid_profit_growth_rates):.1f}%")
    else:
        print(f"{profit_name}前年同期比の計算に必要なデータが不足しています。")


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
            print("使用方法: python test_09_stock_net_sales.py <銘柄コード>")
            print("例: python test_09_stock_net_sales.py 7203")
            print("例: python test_09_stock_net_sales.py 228A")
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
        
        print("J-Quants API 過去2年分売上高・利益データ取得スクリプト")
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
        
        # 過去3年分の財務データのフィルタリング（期間中値計算のため）
        past_3years_df = filter_past_3years_data(df)
        
        if past_3years_df.empty:
            print("エラー: 過去3年分の財務データが見つかりませんでした")
            sys.exit(1)
        
        # 使用する利益タイプを決定
        profit_type = determine_profit_type(past_3years_df)
        profit_name = "経常利益" if profit_type == 'ordinary' else "営業利益"
        
        # 期間中値を計算（最新2年分を返す）
        period_values_df = calculate_period_values(past_3years_df, profit_type)
        
        if period_values_df.empty:
            print("エラー: 期間中値の計算に失敗しました")
            sys.exit(1)
        
        # 前年同期比上昇率を計算
        growth_rates_df = calculate_growth_rates(period_values_df)
        
        # 結果の表示
        display_net_sales_and_profit_data(growth_rates_df, stock_code, profit_name)
        
        print("\n処理が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
