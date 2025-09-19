#!/usr/bin/env python3
"""
CSVファイルから銘柄コードを読み取り、J-Quants APIを使用して財務指標を計算し、CSVに追加するスクリプト

計算する指標:
1. 過去10年の経常利益前年比上昇率の平均（経常利益が取得できない場合は営業利益）
2. 過去1年の売上高前年同期比上昇率（1Q, 2Q, 3Q, FY）
3. 過去1年の経常利益前年同期比上昇率（1Q, 2Q, 3Q, FY）

使用方法:
    python old_04_statements.py <CSVファイルパス>
    
例:
    python old_04_statements.py ../data/2025_09_18.csv

前提条件:
    - token.txtファイルにAPIキー（リフレッシュトークン）が記述されていること
    - jquants-api-clientライブラリがインストールされていること
    - pandasライブラリがインストールされていること
"""

import os
import sys
import pandas as pd
from pathlib import Path
import jquantsapi
from datetime import datetime, timedelta
import time
import csv


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
        print(f"    財務データを取得中...")
        
        # 銘柄コードのみで財務データを取得（日付指定なし）
        financial_data = client.get_fins_statements(code=code)
        
        if not financial_data.empty:
            print(f"    財務データを取得しました: {len(financial_data)} 件")
            
            # 財務諸表のデータのみをフィルタリング
            financial_data = filter_financial_statements_data(financial_data)
            
            if not financial_data.empty:
                # DisclosedDateでソート（最新が最初に来るように降順）
                if 'DisclosedDate' in financial_data.columns:
                    financial_data['DisclosedDate'] = pd.to_datetime(financial_data['DisclosedDate'])
                    financial_data = financial_data.sort_values('DisclosedDate', ascending=False).reset_index(drop=True)
                return financial_data
            else:
                print(f"    財務諸表データが見つかりませんでした")
                return pd.DataFrame()
        else:
            print(f"    財務データが見つかりませんでした")
            return pd.DataFrame()
        
    except Exception as e:
        print(f"    エラー: 財務データの取得中にエラー: {e}")
        return pd.DataFrame()


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
    
    print(f"    財務諸表データ（FinancialStatements）: {len(financial_statements_data)} 件")
    
    return financial_statements_data


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
    
    print(f"    年次財務データ（TypeOfCurrentPeriod = 'FY' かつ FinancialStatements）: {len(annual_data)} 件")
    
    return annual_data


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
    
    print(f"    過去3年分の財務データ: {len(past_3years_data)} 件")
    
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


def calculate_annual_profit_growth_rates(df, profit_type):
    """
    年次利益の上昇率を計算する
    
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


def calculate_period_values(df, profit_type):
    """
    累積値から期間中値を計算する
    
    Args:
        df (pandas.DataFrame): 財務データのデータフレーム（最新順にソート済み）
        profit_type (str): 使用する利益タイプ（'ordinary' または 'operating'）
        
    Returns:
        pandas.DataFrame: 期間中値を計算したデータフレーム
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
    
    return result_df


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
    
    return result_df


def process_stock_statements(client, code, company_name):
    """
    単一銘柄の財務指標を計算する
    
    Args:
        client: J-Quants APIクライアント
        code (str): 銘柄コード
        company_name (str): 会社名（ログ用）
        
    Returns:
        dict: 計算した財務指標の辞書
    """
    print(f"  処理中: {code} ({company_name})")
    
    # 財務データの取得
    financial_data = get_financial_statements(client, code)
    
    if financial_data.empty:
        print(f"    スキップ: 財務データが取得できませんでした")
        return None
    
    # 年次データのフィルタリング
    annual_data = filter_annual_financial_data(financial_data)
    
    # 過去3年分のデータのフィルタリング（期間中値計算のため）
    past_3years_data = filter_past_3years_data(financial_data)
    
    # デバッグ情報を表示
    print(f"    デバッグ: 年次データ {len(annual_data)} 件, 過去3年データ {len(past_3years_data)} 件")
    if not past_3years_data.empty:
        print(f"    デバッグ: 過去3年データの期間タイプ: {past_3years_data['TypeOfCurrentPeriod'].unique()}")
    
    if annual_data.empty and past_3years_data.empty:
        print(f"    スキップ: 必要な財務データが取得できませんでした")
        return None
    
    # 使用する利益タイプを決定
    profit_type = determine_profit_type(financial_data)
    profit_name = "経常利益" if profit_type == 'ordinary' else "営業利益"
    print(f"    使用する利益指標: {profit_name}")
    
    result = {}
    
    # 1. 過去10年の経常利益前年比上昇率の平均を計算
    if not annual_data.empty:
        # 過去10年分に制限
        annual_10years = annual_data.head(10)
        growth_rates = calculate_annual_profit_growth_rates(annual_10years, profit_type)
        
        # 有効な上昇率のみを抽出
        valid_growth_rates = [rate for rate in growth_rates if rate is not None]
        
        if valid_growth_rates:
            average_growth_rate = sum(valid_growth_rates) / len(valid_growth_rates)
            result['過去10年利益上昇率平均'] = round(average_growth_rate, 1)
            print(f"    過去10年利益上昇率平均: {average_growth_rate:.1f}% ({len(valid_growth_rates)}年分)")
        else:
            result['過去10年利益上昇率平均'] = None
            print(f"    過去10年利益上昇率平均: 計算不可")
    else:
        result['過去10年利益上昇率平均'] = None
        print(f"    過去10年利益上昇率平均: 年次データなし")
    
    # 2. 過去1年の売上高・利益前年同期比上昇率を計算（過去3年データから最新2年分を抽出）
    if not past_3years_data.empty:
        print(f"    デバッグ: 期間中値計算開始 - 入力データ {len(past_3years_data)} 件")
        # 期間中値を計算
        period_values_df = calculate_period_values(past_3years_data, profit_type)
        print(f"    デバッグ: 期間中値計算完了 - 出力データ {len(period_values_df)} 件")
        
        # 前年同期比上昇率を計算
        growth_rates_df = calculate_growth_rates(period_values_df)
        print(f"    デバッグ: 上昇率計算完了 - 出力データ {len(growth_rates_df)} 件")
        
        # 最新2年分のデータのみを抽出（前年同期比計算のため）
        current_date = datetime.now()
        two_years_ago = current_date - timedelta(days=730)  # 2年 = 730日
        latest_2years_df = growth_rates_df[growth_rates_df['DisclosedDate'] >= two_years_ago].copy()
        print(f"    デバッグ: 最新2年分データ抽出 - {len(latest_2years_df)} 件")
        
        # 直近4つの期間データを時系列順に取得（開示日順）
        latest_4periods_df = latest_2years_df.head(4).copy()
        
        # 直近4つの期間の上昇率を取得
        for i, (_, row) in enumerate(latest_4periods_df.iterrows(), 1):
            period_type = row.get('TypeOfCurrentPeriod', 'N/A')
            disclosed_date = row.get('DisclosedDate', 'N/A')
            
            # 売上高上昇率
            sales_growth = row.get('SalesGrowthRate')
            result[f'過去1年売上高上昇率_直近{i}'] = sales_growth
            
            # 利益上昇率
            profit_growth = row.get('ProfitGrowthRate')
            result[f'過去1年利益上昇率_直近{i}'] = profit_growth
            
            # デバッグ表示
            date_str = disclosed_date.strftime('%Y-%m-%d') if hasattr(disclosed_date, 'strftime') else str(disclosed_date)
            if sales_growth is not None:
                print(f"    過去1年売上高上昇率_直近{i} ({period_type}, {date_str}): {sales_growth:+.1f}%")
            else:
                print(f"    過去1年売上高上昇率_直近{i} ({period_type}, {date_str}): 計算不可")
            
            if profit_growth is not None:
                print(f"    過去1年利益上昇率_直近{i} ({period_type}, {date_str}): {profit_growth:+.1f}%")
            else:
                print(f"    過去1年利益上昇率_直近{i} ({period_type}, {date_str}): 計算不可")
        
        # 4つに満たない場合は残りをNoneで埋める
        for i in range(len(latest_4periods_df) + 1, 5):
            result[f'過去1年売上高上昇率_直近{i}'] = None
            result[f'過去1年利益上昇率_直近{i}'] = None
            print(f"    過去1年売上高上昇率_直近{i}: データなし")
            print(f"    過去1年利益上昇率_直近{i}: データなし")
    else:
        # 過去3年のデータがない場合、すべてNoneで初期化
        for i in range(1, 5):
            result[f'過去1年売上高上昇率_直近{i}'] = None
            result[f'過去1年利益上昇率_直近{i}'] = None
        print(f"    過去3年のデータなし")
    
    return result


def main():
    """メイン処理"""
    try:
        # 引数の確認
        if len(sys.argv) != 2:
            print("使用方法: python old_04_statements.py <CSVファイルパス>")
            print("例: python old_04_statements.py ../data/2025_09_18.csv")
            sys.exit(1)
        
        csv_file_path = sys.argv[1]
        
        # CSVファイルの存在確認
        if not os.path.exists(csv_file_path):
            print(f"エラー: CSVファイルが見つかりません: {csv_file_path}")
            sys.exit(1)
        
        # スクリプトのディレクトリを取得
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        token_file_path = project_root / "token.txt"
        
        print("CSV銘柄財務指標計算スクリプト")
        print("=" * 50)
        
        # APIキーの読み込み
        print(f"APIキーを読み込み中: {token_file_path}")
        refresh_token = load_api_key(token_file_path)
        print("APIキーの読み込み完了")
        
        # クライアントの初期化
        client = jquantsapi.Client(refresh_token=refresh_token)
        
        # CSVファイルの読み込み
        print(f"CSVファイルを読み込み中: {csv_file_path}")
        try:
            df = pd.read_csv(csv_file_path, encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(csv_file_path, encoding='shift_jis')
        
        print(f"読み込み完了: {len(df)}件の銘柄")
        
        # 新規列の確認・作成
        new_columns = [
            '過去10年利益上昇率平均',
            '過去1年売上高上昇率_直近1', '過去1年売上高上昇率_直近2', '過去1年売上高上昇率_直近3', '過去1年売上高上昇率_直近4',
            '過去1年利益上昇率_直近1', '過去1年利益上昇率_直近2', '過去1年利益上昇率_直近3', '過去1年利益上昇率_直近4'
        ]
        
        for col in new_columns:
            if col not in df.columns:
                df[col] = None
                print(f"新規列を作成: {col}")
            else:
                print(f"既存列を更新: {col}")
        
        # 銘柄コード列の確認
        if 'コード' not in df.columns:
            print("エラー: 'コード'列が見つかりません。")
            sys.exit(1)
        
        # 銘柄名列の確認（ログ用）
        company_name_col = '銘柄名' if '銘柄名' in df.columns else None
        
        # 各銘柄の財務指標を計算
        print(f"\n財務指標計算を開始します...")
        processed_count = 0
        success_count = 0
        
        for index, row in df.iterrows():
            code = str(row['コード']).zfill(4)  # 4桁にゼロパディング
            company_name = row[company_name_col] if company_name_col else f"銘柄{code}"
            
            # 既に計算済みの場合はスキップ（最初の列でチェック）
            if pd.notna(row['過去10年利益上昇率平均']) and row['過去10年利益上昇率平均'] != '':
                print(f"  スキップ: {code} ({company_name}) - 既に計算済み")
                continue
            
            # 財務指標計算
            result = process_stock_statements(client, code, company_name)
            
            if result is not None:
                # 結果をDataFrameに反映
                for col, value in result.items():
                    df.at[index, col] = value
                success_count += 1
                print(f"    完了: {code} ({company_name})")
            else:
                print(f"    スキップ: {code} ({company_name}) - 計算失敗")
            
            processed_count += 1
            
            # 進捗表示
            if processed_count % 10 == 0:
                print(f"  進捗: {processed_count}/{len(df)} 件処理完了")
            
            # API制限を考慮して少し待機
            time.sleep(0.1)
        
        # 結果の保存
        print(f"\n結果を保存中: {csv_file_path}")
        df.to_csv(csv_file_path, index=False, encoding='utf-8', quoting=csv.QUOTE_ALL)
        
        print(f"\n処理完了:")
        print(f"  処理銘柄数: {processed_count} 件")
        print(f"  成功銘柄数: {success_count} 件")
        print(f"  失敗銘柄数: {processed_count - success_count} 件")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
