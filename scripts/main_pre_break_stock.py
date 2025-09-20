#!/usr/bin/env python3
"""
Pre-Break Stock Analyzer

J-Quants APIを使用して銘柄の財務分析を行い、CSV出力するスクリプト

処理内容:
- 銘柄リスト取得（プライム・スタンダード・グロース）
- 各銘柄の財務指標計算
- CSV出力（pb_<日付>.csv）

計算項目:
- 時価総額
- PER
- ROE
- 過去10年利益上昇率平均
- 過去1年売上高上昇率
- 過去1年利益上昇率
- 前回報告日
- 次回報告日（予想）

使用方法:
    python main_pre_break_stock.py

前提条件:
    - token.txtファイルにAPIキー（リフレッシュトークン）が記述されていること
    - jquants-api-clientライブラリがインストールされていること
    - stock_database.pyが同じディレクトリに存在すること
"""

import os
import sys
import csv
import time
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import jquantsapi
from stock_database import StockFinancialDatabase


class PreBreakStockAnalyzer:
    """
    Pre-Break Stock Analyzer クラス
    
    銘柄の財務分析を行い、CSV出力する機能を提供
    """
    
    def __init__(self, database_dir="database", token_file_path="token.txt"):
        """
        初期化
        
        Args:
            database_dir (str): データベースディレクトリのパス
            token_file_path (str): APIキーファイルのパス
        """
        self.db = StockFinancialDatabase(database_dir=database_dir, token_file_path=token_file_path)
        self.client = None
        self.results = []
        self.processed_count = 0
        self.error_count = 0
        
    def _get_client(self):
        """J-Quants APIクライアントを取得する（遅延初期化）"""
        if self.client is None:
            self.client = self.db._get_client()
        return self.client
    
    def get_market_stocks(self, markets=['プライム', 'スタンダード', 'グロース']):
        """
        指定された市場の銘柄リストを取得する
        
        Args:
            markets (list): 対象市場のリスト
            
        Returns:
            list: 銘柄コードのリスト
        """
        try:
            print(f"銘柄リストを取得中... (対象市場: {', '.join(markets)})")
            stock_codes = self.db.get_market_stock_list(markets)
            print(f"取得した銘柄数: {len(stock_codes)} 件")
            return stock_codes
        except Exception as e:
            print(f"エラー: 銘柄リストの取得に失敗しました: {e}")
            return []
    
    def get_stock_price(self, code, days=7):
        """
        指定された銘柄の最新株価を取得する
        
        Args:
            code (str): 銘柄コード
            days (int): 取得する日数（デフォルト7日）
            
        Returns:
            dict: 株価データ（取得失敗時はNone）
        """
        try:
            client = self._get_client()
            
            # 銘柄コードを正規化
            normalized_code = self.db._normalize_stock_code(code)
            print(f"  正規化後コード: {normalized_code}")
            
            # 最新の株価データを取得（過去指定日数間）
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
            
            price_data = client.get_prices_daily_quotes(
                code=normalized_code,
                from_yyyymmdd=start_date,
                to_yyyymmdd=end_date
            )
            
            # データフレームに変換
            if isinstance(price_data, pd.DataFrame):
                df = price_data
            else:
                df = pd.DataFrame(price_data)
            
            if df.empty:
                return None
            
            # 日付でソート（最新が最初に来るように降順）
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])
                df = df.sort_values('Date', ascending=False).reset_index(drop=True)
            
            # 最新のデータを取得
            latest_data = df.iloc[0]
            
            return {
                'code': code,
                'date': latest_data.get('Date'),
                'close': latest_data.get('Close'),
                'open': latest_data.get('Open'),
                'high': latest_data.get('High'),
                'low': latest_data.get('Low'),
                'volume': latest_data.get('Volume')
            }
            
        except Exception as e:
            print(f"エラー: 銘柄コード {code} の株価取得に失敗しました: {e}")
            print(f"  エラータイプ: {type(e).__name__}")
            return None
    
    def get_financial_data(self, code):
        """
        指定された銘柄の財務データを取得する
        
        Args:
            code (str): 銘柄コード
            
        Returns:
            dict: 財務データ（取得失敗時はNone）
        """
        try:
            # 既存データをチェック
            data = self.db.load_stock_data(code)
            if data is not None:
                return data
            
            # データを取得・保存
            financial_data = self.db.get_financial_statements(code)
            if not financial_data.empty:
                self.db.save_stock_data(code, financial_data)
                return self.db.load_stock_data(code)
            
            return None
            
        except Exception as e:
            print(f"エラー: 銘柄コード {code} の財務データ取得に失敗しました: {e}")
            return None
    
    def calculate_market_cap(self, price_data, financial_data):
        """
        時価総額を計算する（億円単位）
        
        Args:
            price_data (dict): 株価データ
            financial_data (dict): 財務データ
            
        Returns:
            float: 時価総額（億円単位、計算できない場合はNone）
        """
        try:
            if not price_data or not financial_data:
                return None
            
            close_price = price_data.get('close')
            if close_price is None:
                return None
            
            # 財務データから発行済み株式数を取得
            raw_data = financial_data.get('raw_data', [])
            if not raw_data:
                return None
            
            # 最新の財務データを取得
            latest_financial = raw_data[0]
            issued_shares = latest_financial.get('NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock')
            
            if issued_shares is None:
                return None
            
            # 時価総額 = 株価 × 発行済み株式数（円単位）
            market_cap_yen = float(close_price) * float(issued_shares)
            
            # 億円単位に変換（1億 = 100,000,000）
            market_cap_billion = market_cap_yen / 100000000
            return market_cap_billion
            
        except Exception as e:
            print(f"エラー: 時価総額の計算に失敗しました: {e}")
            return None
    
    def calculate_per(self, price_data, financial_data):
        """
        PERを計算する（EPSがマイナスでも計算）
        
        Args:
            price_data (dict): 株価データ
            financial_data (dict): 財務データ
            
        Returns:
            float: PER（計算できない場合はNone）
        """
        try:
            if not price_data or not financial_data:
                return None
            
            close_price = price_data.get('close')
            if close_price is None:
                return None
            
            # 財務データからEPSを取得
            raw_data = financial_data.get('raw_data', [])
            if not raw_data:
                return None
            
            latest_financial = raw_data[0]
            api_eps = latest_financial.get('EarningsPerShare')
            
            # APIから提供されるEPSを優先的に使用
            if api_eps is not None:
                try:
                    eps_value = float(api_eps) if isinstance(api_eps, (int, float, str)) else 0
                    
                    # EPSが0の場合は計算不可
                    if eps_value == 0:
                        return None
                    
                    # PERを計算（EPSがマイナスでも計算する）
                    per = float(close_price) / eps_value
                    return per
                    
                except (ValueError, TypeError):
                    pass
            
            # APIからEPSが取得できない場合は手動計算
            profit = latest_financial.get('Profit')
            issued_shares = latest_financial.get('NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock')
            
            if profit is None or issued_shares is None:
                return None
            
            # 数値に変換
            try:
                profit_value = float(profit) if isinstance(profit, (int, float, str)) else 0
                shares_value = float(issued_shares) if isinstance(issued_shares, (int, float, str)) else 0
            except (ValueError, TypeError):
                return None
            
            if shares_value <= 0:
                return None
            
            # EPS（1株当たり純利益）を計算
            eps = profit_value / shares_value
            
            # EPSが0の場合は計算不可
            if eps == 0:
                return None
            
            # PERを計算（EPSがマイナスでも計算する）
            per = float(close_price) / eps
            return per
            
        except Exception as e:
            print(f"エラー: PERの計算に失敗しました: {e}")
            return None
    
    def calculate_roe(self, financial_data):
        """
        ROEを計算する
        
        Args:
            financial_data (dict): 財務データ
            
        Returns:
            float: ROE（%）（計算できない場合はNone）
        """
        try:
            if not financial_data:
                return None
            
            raw_data = financial_data.get('raw_data', [])
            if not raw_data:
                return None
            
            # 年次財務諸表のデータを検索
            for record in raw_data:
                period_type = record.get('TypeOfCurrentPeriod', '')
                doc_type = record.get('TypeOfDocument', '')
                
                # 年次財務諸表かチェック
                if period_type == 'FY' and 'FinancialStatements' in doc_type:
                    profit = record.get('Profit')
                    equity = record.get('Equity')
                    
                    if profit is not None and equity is not None:
                        try:
                            profit_value = float(profit)
                            equity_value = float(equity)
                            
                            if equity_value > 0:
                                roe = (profit_value / equity_value) * 100
                                return roe
                        except (ValueError, TypeError):
                            continue
            
            return None
            
        except Exception as e:
            print(f"エラー: ROEの計算に失敗しました: {e}")
            return None
    
    def calculate_profit_growth_10years(self, financial_data):
        """
        過去10年利益上昇率平均を計算する
        
        Args:
            financial_data (dict): 財務データ
            
        Returns:
            float: 過去10年利益上昇率平均（%）（計算できない場合はNone）
        """
        try:
            if not financial_data:
                return None
            
            raw_data = financial_data.get('raw_data', [])
            if not raw_data:
                return None
            
            # データフレームに変換
            df = pd.DataFrame(raw_data)
            if df.empty:
                return None
            
            # 年次財務諸表のデータをフィルタリング
            annual_data = df[
                (df['TypeOfCurrentPeriod'] == 'FY') & 
                (df['TypeOfDocument'].str.contains('FinancialStatements', na=False))
            ].copy()
            
            if annual_data.empty or len(annual_data) < 2:
                return None
            
            # 開示日順でソート（最新が最初に来るように降順）
            if 'DisclosedDate' in annual_data.columns:
                annual_data['DisclosedDate'] = pd.to_datetime(annual_data['DisclosedDate'])
                annual_data = annual_data.sort_values('DisclosedDate', ascending=False).reset_index(drop=True)
            
            # 過去11年分のデータを取得（10年分の上昇率計算のため）
            df_11years = annual_data.head(11) if len(annual_data) >= 11 else annual_data
            
            if len(df_11years) < 2:
                return None
            
            # 使用する利益タイプを決定
            profit_type = self._determine_profit_type(df_11years)
            profit_column = 'OrdinaryProfit' if profit_type == 'ordinary' else 'OperatingProfit'
            
            # 成長率を計算（test_08_stock_statements.pyと同じロジック）
            growth_rates = []
            for i in range(len(df_11years)):
                # 一番古い年度（最後のインデックス）は前年度がないので計算不可
                if i == len(df_11years) - 1:
                    growth_rates.append(None)
                    continue
                
                # 現在年度と前年度の比較
                current_row = df_11years.iloc[i]      # 現在の年度（例：2025年）
                previous_row = df_11years.iloc[i+1]   # 前年度（例：2024年）
                
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
            
            # 有効な上昇率のみを抽出（test_08_stock_statements.pyと同じ）
            valid_growth_rates = [rate for rate in growth_rates if rate is not None]
            
            if not valid_growth_rates:
                return None
            
            # 平均成長率を計算
            average_growth = sum(valid_growth_rates) / len(valid_growth_rates)
            return average_growth
            
        except Exception as e:
            print(f"エラー: 過去10年利益上昇率平均の計算に失敗しました: {e}")
            return None
    
    def _determine_profit_type(self, df):
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
    
    def _calculate_period_values(self, df, data_type, profit_type='ordinary'):
        """
        累積値から期間中値を計算する
        
        Args:
            df (pandas.DataFrame): 過去3年分の財務データのデータフレーム（最新順にソート済み）
            data_type (str): データタイプ（'sales' または 'profit'）
            profit_type (str): 使用する利益タイプ（'ordinary' または 'operating'）
            
        Returns:
            pandas.DataFrame: 期間中値を計算したデータフレーム
        """
        if df.empty:
            return df
        
        # データフレームをコピー
        result_df = df.copy()
        
        if data_type == 'sales':
            # 売上高の期間中値計算
            result_df['PeriodNetSales'] = None
            
            for i, (_, row) in enumerate(result_df.iterrows()):
                period_type = row.get('TypeOfCurrentPeriod', '')
                current_sales = row.get('NetSales')
                
                if current_sales is not None and current_sales != '' and current_sales != 'N/A':
                    try:
                        current_sales_value = float(current_sales)
                        
                        if period_type == '1Q':
                            # 1Qはそのまま（累積値）
                            result_df.at[i, 'PeriodNetSales'] = current_sales_value
                        else:
                            # 2Q, 3Q, FYは前の期間の値を引く
                            previous_sales = self._find_previous_period_sales(result_df, i, period_type)
                            if previous_sales is not None:
                                period_sales = current_sales_value - previous_sales
                                result_df.at[i, 'PeriodNetSales'] = period_sales
                            else:
                                result_df.at[i, 'PeriodNetSales'] = current_sales_value
                    except (ValueError, TypeError):
                        result_df.at[i, 'PeriodNetSales'] = None
        
        elif data_type == 'profit':
            # 利益の期間中値計算
            profit_column = 'OrdinaryProfit' if profit_type == 'ordinary' else 'OperatingProfit'
            result_df['PeriodProfit'] = None
            
            for i, (_, row) in enumerate(result_df.iterrows()):
                period_type = row.get('TypeOfCurrentPeriod', '')
                current_profit = row.get(profit_column)
                
                if current_profit is not None and current_profit != '' and current_profit != 'N/A':
                    try:
                        current_profit_value = float(current_profit)
                        
                        if period_type == '1Q':
                            # 1Qはそのまま（累積値）
                            result_df.at[i, 'PeriodProfit'] = current_profit_value
                        else:
                            # 2Q, 3Q, FYは前の期間の値を引く
                            previous_profit = self._find_previous_period_profit(result_df, i, period_type, profit_column)
                            if previous_profit is not None:
                                period_profit = current_profit_value - previous_profit
                                result_df.at[i, 'PeriodProfit'] = period_profit
                            else:
                                result_df.at[i, 'PeriodProfit'] = current_profit_value
                    except (ValueError, TypeError):
                        result_df.at[i, 'PeriodProfit'] = None
        
        return result_df
    
    def _find_previous_period_sales(self, df, current_index, period_type):
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
            target_period = '1Q'
        elif period_type == '3Q':
            target_period = '2Q'
        elif period_type == 'FY':
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
    
    def _find_previous_period_profit(self, df, current_index, period_type, profit_column):
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
            target_period = '1Q'
        elif period_type == '3Q':
            target_period = '2Q'
        elif period_type == 'FY':
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
    
    def _find_previous_year_same_period(self, df, current_index, period_type):
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
    
    def _calculate_growth_rates(self, df, data_type):
        """
        前年同期比上昇率を計算する
        
        Args:
            df (pandas.DataFrame): 期間中値を計算したデータフレーム
            data_type (str): データタイプ（'sales' または 'profit'）
            
        Returns:
            pandas.DataFrame: 上昇率を追加したデータフレーム
        """
        if df.empty:
            return df
        
        # データフレームをコピー
        result_df = df.copy()
        
        if data_type == 'sales':
            result_df['SalesGrowthRate'] = None
        elif data_type == 'profit':
            result_df['ProfitGrowthRate'] = None
        
        # 各データに対して上昇率を計算
        for i, (_, row) in enumerate(result_df.iterrows()):
            period_type = row.get('TypeOfCurrentPeriod', '')
            
            # 前年度の同じ期間タイプを検索
            previous_year_data = self._find_previous_year_same_period(result_df, i, period_type)
            
            if previous_year_data is not None:
                if data_type == 'sales':
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
                
                elif data_type == 'profit':
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
    
    def calculate_sales_growth_1year(self, financial_data):
        """
        過去1年売上高上昇率を計算する（期間中値を使用、直近4期間分）
        
        Args:
            financial_data (dict): 財務データ
            
        Returns:
            dict: 直近4期間の売上高上昇率の辞書
        """
        try:
            if not financial_data:
                return {f'過去1年売上高上昇率_直近{i}': None for i in range(1, 5)}
            
            raw_data = financial_data.get('raw_data', [])
            if not raw_data:
                return {f'過去1年売上高上昇率_直近{i}': None for i in range(1, 5)}
            
            # データフレームに変換
            df = pd.DataFrame(raw_data)
            if df.empty:
                return {f'過去1年売上高上昇率_直近{i}': None for i in range(1, 5)}
            
            # 財務諸表のデータをフィルタリング
            financial_statements = df[
                df['TypeOfDocument'].str.contains('FinancialStatements', na=False)
            ].copy()
            
            if financial_statements.empty:
                return {f'過去1年売上高上昇率_直近{i}': None for i in range(1, 5)}
            
            # 過去3年分のデータをフィルタリング（期間中値計算のため）
            current_date = datetime.now()
            three_years_ago = current_date - timedelta(days=1095)  # 3年 = 1095日
            
            if 'DisclosedDate' in financial_statements.columns:
                financial_statements['DisclosedDate'] = pd.to_datetime(financial_statements['DisclosedDate'])
                past_3years_data = financial_statements[financial_statements['DisclosedDate'] >= three_years_ago].copy()
            else:
                past_3years_data = financial_statements
            
            if past_3years_data.empty:
                return {f'過去1年売上高上昇率_直近{i}': None for i in range(1, 5)}
            
            # 開示日順でソート（最新が最初に来るように降順）
            past_3years_data = past_3years_data.sort_values('DisclosedDate', ascending=False).reset_index(drop=True)
            
            # 期間中値を計算
            period_values_df = self._calculate_period_values(past_3years_data, 'sales')
            
            if period_values_df.empty:
                return {f'過去1年売上高上昇率_直近{i}': None for i in range(1, 5)}
            
            # 最新2年分のデータを取得
            two_years_ago = current_date - timedelta(days=730)  # 2年 = 730日
            latest_2years_df = period_values_df[period_values_df['DisclosedDate'] >= two_years_ago].copy()
            
            if latest_2years_df.empty:
                return {f'過去1年売上高上昇率_直近{i}': None for i in range(1, 5)}
            
            # 前年同期比上昇率を計算
            growth_rates_df = self._calculate_growth_rates(latest_2years_df, 'sales')
            
            if growth_rates_df.empty:
                return {f'過去1年売上高上昇率_直近{i}': None for i in range(1, 5)}
            
            # 直近4期間分のデータを取得
            latest_4periods_df = growth_rates_df.head(4).copy()
            
            # 結果辞書を初期化
            result = {}
            
            # 直近4期間の上昇率を取得
            for i, (_, row) in enumerate(latest_4periods_df.iterrows(), 1):
                sales_growth = row.get('SalesGrowthRate')
                result[f'過去1年売上高上昇率_直近{i}'] = sales_growth
            
            # 4つに満たない場合は残りをNoneで埋める
            for i in range(len(latest_4periods_df) + 1, 5):
                result[f'過去1年売上高上昇率_直近{i}'] = None
            
            return result
            
        except Exception as e:
            print(f"エラー: 過去1年売上高上昇率の計算に失敗しました: {e}")
            return {f'過去1年売上高上昇率_直近{i}': None for i in range(1, 5)}
    
    def calculate_profit_growth_1year(self, financial_data):
        """
        過去1年利益上昇率を計算する（期間中値を使用、直近4期間分）
        
        Args:
            financial_data (dict): 財務データ
            
        Returns:
            dict: 直近4期間の利益上昇率の辞書
        """
        try:
            if not financial_data:
                return {f'過去1年利益上昇率_直近{i}': None for i in range(1, 5)}
            
            raw_data = financial_data.get('raw_data', [])
            if not raw_data:
                return {f'過去1年利益上昇率_直近{i}': None for i in range(1, 5)}
            
            # データフレームに変換
            df = pd.DataFrame(raw_data)
            if df.empty:
                return {f'過去1年利益上昇率_直近{i}': None for i in range(1, 5)}
            
            # 財務諸表のデータをフィルタリング
            financial_statements = df[
                df['TypeOfDocument'].str.contains('FinancialStatements', na=False)
            ].copy()
            
            if financial_statements.empty:
                return {f'過去1年利益上昇率_直近{i}': None for i in range(1, 5)}
            
            # 過去3年分のデータをフィルタリング（期間中値計算のため）
            current_date = datetime.now()
            three_years_ago = current_date - timedelta(days=1095)  # 3年 = 1095日
            
            if 'DisclosedDate' in financial_statements.columns:
                financial_statements['DisclosedDate'] = pd.to_datetime(financial_statements['DisclosedDate'])
                past_3years_data = financial_statements[financial_statements['DisclosedDate'] >= three_years_ago].copy()
            else:
                past_3years_data = financial_statements
            
            if past_3years_data.empty:
                return {f'過去1年利益上昇率_直近{i}': None for i in range(1, 5)}
            
            # 開示日順でソート（最新が最初に来るように降順）
            past_3years_data = past_3years_data.sort_values('DisclosedDate', ascending=False).reset_index(drop=True)
            
            # 使用する利益タイプを決定
            profit_type = self._determine_profit_type(past_3years_data)
            
            # 期間中値を計算
            period_values_df = self._calculate_period_values(past_3years_data, 'profit', profit_type)
            
            if period_values_df.empty:
                return {f'過去1年利益上昇率_直近{i}': None for i in range(1, 5)}
            
            # 最新2年分のデータを取得
            two_years_ago = current_date - timedelta(days=730)  # 2年 = 730日
            latest_2years_df = period_values_df[period_values_df['DisclosedDate'] >= two_years_ago].copy()
            
            if latest_2years_df.empty:
                return {f'過去1年利益上昇率_直近{i}': None for i in range(1, 5)}
            
            # 前年同期比上昇率を計算
            growth_rates_df = self._calculate_growth_rates(latest_2years_df, 'profit')
            
            if growth_rates_df.empty:
                return {f'過去1年利益上昇率_直近{i}': None for i in range(1, 5)}
            
            # 直近4期間分のデータを取得
            latest_4periods_df = growth_rates_df.head(4).copy()
            
            # 結果辞書を初期化
            result = {}
            
            # 直近4期間の上昇率を取得
            for i, (_, row) in enumerate(latest_4periods_df.iterrows(), 1):
                profit_growth = row.get('ProfitGrowthRate')
                result[f'過去1年利益上昇率_直近{i}'] = profit_growth
            
            # 4つに満たない場合は残りをNoneで埋める
            for i in range(len(latest_4periods_df) + 1, 5):
                result[f'過去1年利益上昇率_直近{i}'] = None
            
            return result
            
        except Exception as e:
            print(f"エラー: 過去1年利益上昇率の計算に失敗しました: {e}")
            return {f'過去1年利益上昇率_直近{i}': None for i in range(1, 5)}
    
    def get_report_dates(self, financial_data):
        """
        前回報告日と次回報告日（予想）を取得する
        
        Args:
            financial_data (dict): 財務データ
            
        Returns:
            tuple: (前回報告日, 次回報告日予想)
        """
        try:
            if not financial_data:
                return None, None
            
            raw_data = financial_data.get('raw_data', [])
            if not raw_data:
                return None, None
            
            # 財務諸表のデータを抽出
            financial_statements = []
            for record in raw_data:
                doc_type = record.get('TypeOfDocument', '')
                if 'FinancialStatements' in doc_type:
                    disclosed_date = record.get('DisclosedDate')
                    period_type = record.get('TypeOfCurrentPeriod', '')
                    
                    if disclosed_date is not None and period_type in ['1Q', '2Q', '3Q', 'FY']:
                        financial_statements.append({
                            'disclosed_date': disclosed_date,
                            'period_type': period_type
                        })
            
            if not financial_statements:
                return None, None
            
            # 開示日順でソート
            financial_statements.sort(key=lambda x: x['disclosed_date'])
            
            # 最新の報告日を取得
            latest_report = financial_statements[-1]
            last_report_date = latest_report['disclosed_date']
            
            # 前回報告日をYYYY/MM/DD形式に変換
            try:
                last_date = pd.to_datetime(last_report_date)
                last_report_date_str = last_date.strftime('%Y/%m/%d')
            except:
                last_report_date_str = None
            
            # 次回報告日を予想（3ヶ月後）
            try:
                last_date = pd.to_datetime(last_report_date)
                next_report_date = last_date + timedelta(days=90)
                next_report_date_str = next_report_date.strftime('%Y/%m/%d')
            except:
                next_report_date_str = None
            
            return last_report_date_str, next_report_date_str
            
        except Exception as e:
            print(f"エラー: 報告日の取得に失敗しました: {e}")
            return None, None
    
    def get_company_info(self, code):
        """
        銘柄の基本情報（会社名、市場、業種）を取得する
        
        Args:
            code (str): 銘柄コード
            
        Returns:
            tuple: (会社名, 市場, 17業種名, 33業種名)
        """
        try:
            client = self._get_client()
            
            # 銘柄一覧から基本情報を取得
            stock_list = client.get_listed_info()
            df = pd.DataFrame(stock_list)
            
            # 該当する銘柄を検索
            target_stock = df[df['Code'] == code]
            if not target_stock.empty:
                company_name = target_stock.iloc[0].get('CompanyName', 'N/A')
                market = target_stock.iloc[0].get('MarketCodeName', 'N/A')
                sector_17 = target_stock.iloc[0].get('Sector17CodeName', '')
                sector_33 = target_stock.iloc[0].get('Sector33CodeName', '')
                return company_name, market, sector_17, sector_33
            
            return "N/A", "N/A", "", ""
            
        except Exception as e:
            print(f"警告: 銘柄コード {code} の基本情報取得に失敗しました: {e}")
            return "N/A", "N/A", "", ""
    
    def analyze_single_stock(self, code):
        """
        単一銘柄の分析を行う
        
        Args:
            code (str): 銘柄コード
            
        Returns:
            dict: 分析結果
        """
        try:
            print(f"分析中: {code}")
            
            # 株価データを取得
            price_data = self.get_stock_price(code)
            if not price_data:
                print(f"  警告: 株価データが取得できませんでした")
                return None
            
            # 財務データを取得
            financial_data = self.get_financial_data(code)
            if not financial_data:
                print(f"  警告: 財務データが取得できませんでした")
                return None
            
            # 基本情報を取得
            company_name, market, sector_17, sector_33 = self.get_company_info(code)
            
            # 各指標を計算
            market_cap = self.calculate_market_cap(price_data, financial_data)
            per = self.calculate_per(price_data, financial_data)
            roe = self.calculate_roe(financial_data)
            profit_growth_10y = self.calculate_profit_growth_10years(financial_data)
            sales_growth_1y_data = self.calculate_sales_growth_1year(financial_data)
            profit_growth_1y_data = self.calculate_profit_growth_1year(financial_data)
            last_report_date, next_report_date = self.get_report_dates(financial_data)
            
            # 結果をまとめる
            result = {
                'code': code,
                'company_name': company_name,
                'sector_17': sector_17,
                'sector_33': sector_33,
                'market': market,
                'stock_price': price_data.get('close'),
                'volume': price_data.get('volume'),  # 出来高を追加
                'market_cap': market_cap,
                'per': per,
                'roe': roe,
                'profit_growth_10y': profit_growth_10y,
                'last_report_date': last_report_date,
                'next_report_date': next_report_date
            }
            
            # 直近4期間の売上高・利益上昇率を追加
            result.update(sales_growth_1y_data)
            result.update(profit_growth_1y_data)
            
            print(f"  完了: {code} ({company_name})")
            return result
            
        except Exception as e:
            print(f"エラー: 銘柄コード {code} の分析に失敗しました: {e}")
            return None
    
    def export_to_csv(self, results, output_dir="data"):
        """
        結果をCSVファイルに出力する
        
        Args:
            results (list): 分析結果のリスト
            output_dir (str): 出力ディレクトリ（デフォルト: data）
        """
        try:
            # 出力ディレクトリを作成（存在しない場合）
            os.makedirs(output_dir, exist_ok=True)
            
            # ファイル名を生成
            current_date = datetime.now().strftime('%Y%m%d')
            filename = f"pb_{current_date}.csv"
            filepath = os.path.join(output_dir, filename)
            
            # CSVファイルに出力
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    '銘柄コード', '銘柄名', '17業種名', '33業種名', '市場', '株価', '出来高', '時価総額（億円）', 'PER', 'ROE',
                    '過去10年利益上昇率平均',
                    '過去1年売上高上昇率_直近1', '過去1年売上高上昇率_直近2', '過去1年売上高上昇率_直近3', '過去1年売上高上昇率_直近4',
                    '過去1年利益上昇率_直近1', '過去1年利益上昇率_直近2', '過去1年利益上昇率_直近3', '過去1年利益上昇率_直近4',
                    '前回報告日', '次回報告日予想'
                ]
                
                # ヘッダー行を出力
                csvfile.write(','.join(fieldnames) + '\n')
                
                for result in results:
                    if result:
                        # 数値を小数点第1位までに丸める関数
                        def format_number(value):
                            if value is None or value == '':
                                return ''
                            try:
                                return round(float(value), 1)
                            except (ValueError, TypeError):
                                return value
                        
                        # 各列の値を取得
                        values = [
                            result.get('code', ''),
                            f'"{result.get("company_name", "")}"',  # 銘柄名のみダブルクオートで囲む
                            result.get('sector_17', ''),
                            result.get('sector_33', ''),
                            result.get('market', ''),
                            format_number(result.get('stock_price', '')),
                            result.get('volume', ''),  # 出来高を追加
                            format_number(result.get('market_cap', '')),
                            format_number(result.get('per', '')),
                            format_number(result.get('roe', '')),
                            format_number(result.get('profit_growth_10y', '')),
                            format_number(result.get('過去1年売上高上昇率_直近1', '')),
                            format_number(result.get('過去1年売上高上昇率_直近2', '')),
                            format_number(result.get('過去1年売上高上昇率_直近3', '')),
                            format_number(result.get('過去1年売上高上昇率_直近4', '')),
                            format_number(result.get('過去1年利益上昇率_直近1', '')),
                            format_number(result.get('過去1年利益上昇率_直近2', '')),
                            format_number(result.get('過去1年利益上昇率_直近3', '')),
                            format_number(result.get('過去1年利益上昇率_直近4', '')),
                            result.get('last_report_date', ''),
                            result.get('next_report_date', '')
                        ]
                        
                        # CSV行を出力
                        csvfile.write(','.join(str(v) for v in values) + '\n')
            
            print(f"CSVファイルを出力しました: {filepath}")
            
        except Exception as e:
            print(f"エラー: CSVファイルの出力に失敗しました: {e}")
    
    def run_analysis(self, markets=['プライム', 'スタンダード', 'グロース'], max_stocks=None):
        """
        分析を実行する
        
        Args:
            markets (list): 対象市場のリスト
            max_stocks (int): 最大処理銘柄数（テスト用、Noneの場合は全銘柄）
        """
        try:
            print("=" * 80)
            print("Pre-Break Stock Analyzer 開始")
            print("=" * 80)
            
            # 銘柄リストを取得
            stock_codes = self.get_market_stocks(markets)
            if not stock_codes:
                print("エラー: 銘柄リストが取得できませんでした")
                return
            
            # テスト用に銘柄数を制限
            if max_stocks and max_stocks < len(stock_codes):
                stock_codes = stock_codes[:max_stocks]
                print(f"テストモード: 処理銘柄数を {max_stocks} 件に制限しました")
            
            print(f"分析対象銘柄数: {len(stock_codes)} 件")
            print("-" * 80)
            
            # 各銘柄を分析
            results = []
            start_time = datetime.now()
            
            for i, code in enumerate(stock_codes, 1):
                print(f"[{i}/{len(stock_codes)}] 処理中...")
                
                result = self.analyze_single_stock(code)
                if result:
                    results.append(result)
                    self.processed_count += 1
                else:
                    self.error_count += 1
                
                # 進捗表示（10件ごと）
                if i % 10 == 0:
                    elapsed_time = datetime.now() - start_time
                    print(f"  進捗: {i}/{len(stock_codes)} 件完了 (経過時間: {elapsed_time})")
                
                # APIレート制限対策
                if i < len(stock_codes):
                    time.sleep(0.5)
            
            # 結果をCSVに出力
            if results:
                # プロジェクトルートのdataフォルダに出力
                script_dir = Path(__file__).parent
                project_root = script_dir.parent
                data_dir = project_root / "data"
                self.export_to_csv(results, str(data_dir))
                
                # サマリーを表示
                end_time = datetime.now()
                total_time = end_time - start_time
                
                print("\n" + "=" * 80)
                print("分析完了")
                print("=" * 80)
                print(f"処理成功: {self.processed_count} 件")
                print(f"処理エラー: {self.error_count} 件")
                print(f"総処理数: {len(stock_codes)} 件")
                print(f"処理時間: {total_time}")
                print(f"平均処理時間: {total_time / len(stock_codes) if stock_codes else 0}")
            else:
                print("エラー: 分析結果がありませんでした")
            
        except Exception as e:
            print(f"エラー: 分析の実行に失敗しました: {e}")
            import traceback
            traceback.print_exc()


def main():
    """メイン処理"""
    try:
        # コマンドライン引数の処理
        max_stocks = None
        if len(sys.argv) > 1:
            try:
                max_stocks = int(sys.argv[1])
                print(f"テストモード: 最大処理銘柄数を {max_stocks} 件に設定")
            except ValueError:
                print("警告: 無効な引数です。全銘柄を処理します。")
        
        # スクリプトのディレクトリを取得
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        database_dir = project_root / "database"
        token_file_path = project_root / "token.txt"
        
        # 分析器を初期化
        analyzer = PreBreakStockAnalyzer(
            database_dir=str(database_dir),
            token_file_path=str(token_file_path)
        )
        
        # 分析を実行
        analyzer.run_analysis(max_stocks=max_stocks)
        
    except Exception as e:
        print(f"エラー: メイン処理でエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
