#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
統合株価分析スクリプト
4つの処理を順番に実行して包括的な株価分析を行う

処理内容:
1. 株探から52週高値銘柄を取得
2. 時価総額・業種・概要を取得
3. J-Quants APIでROEを計算
4. 財務指標（成長率等）を計算

使用方法:
    python main_new_break_stock.py [--output-dir OUTPUT_DIR] [--max-stocks MAX_STOCKS]

例:
    python main_new_break_stock.py --output-dir ../data --max-stocks 100
"""

import os
import sys
import csv
import glob
import time
import random
import argparse
import requests
from datetime import datetime, timedelta
from pathlib import Path
from bs4 import BeautifulSoup
import pandas as pd
import jquantsapi
from typing import List, Optional, Dict, Any


class IntegratedStockAnalyzer:
    """統合株価分析クラス"""
    
    def __init__(self, output_dir: str = "data", max_stocks: Optional[int] = None):
        """
        初期化
        
        Args:
            output_dir (str): 出力ディレクトリ
            max_stocks (Optional[int]): 最大処理銘柄数（テスト用）
        """
        self.output_dir = Path(output_dir)
        self.max_stocks = max_stocks
        self.jquants_client = None
        self.session = requests.Session()
        
        # 出力ディレクトリを作成
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 結果データ
        self.stock_data = []
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
    
    def _load_jquants_api_key(self) -> str:
        """J-Quants APIキーを読み込む"""
        try:
            script_dir = Path(__file__).parent
            project_root = script_dir.parent
            token_file_path = project_root / "token.txt"
            
            with open(token_file_path, 'r', encoding='utf-8') as file:
                token = file.read().strip()
            
            if not token:
                raise ValueError("APIキーファイルが空です")
            
            return token
        except FileNotFoundError:
            raise FileNotFoundError(f"APIキーファイルが見つかりません: {token_file_path}")
        except Exception as e:
            raise Exception(f"APIキーの読み込み中にエラーが発生しました: {e}")
    
    def _get_jquants_client(self):
        """J-Quants APIクライアントを取得する（遅延初期化）"""
        if self.jquants_client is None:
            refresh_token = self._load_jquants_api_key()
            self.jquants_client = jquantsapi.Client(refresh_token=refresh_token)
        return self.jquants_client
    
    def _fetch_html_from_url(self, url: str, max_retries: int = 3, delay: int = 1) -> Optional[str]:
        """URLからHTMLコンテンツを取得する"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        for attempt in range(max_retries):
            try:
                print(f"URLからHTMLを取得中: {url} (試行 {attempt + 1}/{max_retries})")
                response = self.session.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                response.encoding = response.apparent_encoding
                print(f"HTML取得成功: {len(response.text)}文字")
                return response.text
            except requests.exceptions.RequestException as e:
                print(f"エラー (試行 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    print(f"{delay}秒後にリトライします...")
                    time.sleep(delay)
                else:
                    print(f"最大リトライ回数に達しました。URL取得に失敗しました: {url}")
                    return None
        return None
    
    def _generate_kabutan_urls(self) -> List[str]:
        """株探の52週高値ページのURLを生成する"""
        base_url = "https://kabutan.jp/warning/record_w52_high_price"
        urls = []
        
        for market in range(1, 4):  # market=1,2,3
            for page in range(1, 4):  # page=1,2,3
                url = f"{base_url}?market={market}&page={page}"
                urls.append(url)
        
        return urls
    
    def _extract_stock_data_from_url(self, url: str) -> List[Dict[str, Any]]:
        """URLから株価データを抽出する"""
        try:
            # URLからHTMLを取得
            html_content = self._fetch_html_from_url(url)
            if not html_content:
                return []
            
            # テーブルを抽出
            soup = BeautifulSoup(html_content, 'html.parser')
            table = soup.find('table', class_='stock_table st_market')
            
            if not table:
                print(f"  スキップ: テーブルが見つかりません")
                return []
            
            # tbody内の行を取得
            tbody = table.find('tbody')
            if not tbody:
                print(f"  スキップ: tbodyが見つかりません")
                return []
            
            rows = tbody.find_all('tr')
            if not rows:
                print(f"  スキップ: データ行が見つかりません")
                return []
            
            # データを抽出
            page_data = []
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 13:  # 必要な列数があるかチェック
                    try:
                        # 各列からデータを抽出
                        code = cells[0].get_text(strip=True)
                        name = cells[1].get_text(strip=True)
                        market = cells[2].get_text(strip=True)
                        price = cells[5].get_text(strip=True)
                        change_amount = cells[7].get_text(strip=True)
                        change_percent = cells[8].get_text(strip=True)
                        per = cells[10].get_text(strip=True)
                        pbr = cells[11].get_text(strip=True)
                        yield_val = cells[12].get_text(strip=True)
                        
                        # データを辞書として保存
                        page_data.append({
                            'コード': code,
                            '銘柄名': name,
                            '市場': market,
                            '株価': price,
                            '前日比': change_amount,
                            '前日比（％）': change_percent,
                            'PER': per,
                            'PBR': pbr,
                            '利回り': yield_val
                        })
                    except (IndexError, AttributeError) as e:
                        print(f"  警告: 行でデータ抽出エラー: {e}")
                        continue
            
            if page_data:
                print(f"  成功: {len(page_data)}件のデータを抽出")
            else:
                print(f"  スキップ: 抽出可能なデータがありません")
            
            return page_data
            
        except Exception as e:
            print(f"  エラー: {e}")
            return []
    
    def step1_fetch_stock_list(self) -> List[Dict[str, Any]]:
        """ステップ1: 株探から52週高値銘柄を取得"""
        print("=" * 80)
        print("ステップ1: 株探から52週高値銘柄を取得")
        print("=" * 80)
        
        urls = self._generate_kabutan_urls()
        all_data = []
        successful_urls = 0
        skipped_urls = 0
        
        print(f"処理対象URL: {len(urls)}件")
        
        for i, url in enumerate(urls, 1):
            print(f"\n[{i}/{len(urls)}] 処理中: {url}")
            
            page_data = self._extract_stock_data_from_url(url)
            
            if page_data:
                all_data.extend(page_data)
                successful_urls += 1
            else:
                skipped_urls += 1
            
            # インターバル
            time.sleep(random.uniform(0.1, 0.3))
        
        print(f"\nステップ1完了:")
        print(f"  成功: {successful_urls}件")
        print(f"  スキップ: {skipped_urls}件")
        print(f"  総抽出件数: {len(all_data)}件")
        
        return all_data
    
    def step2_fetch_market_cap_and_industry(self, stock_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """ステップ2: 時価総額・業種・概要を取得"""
        print("\n" + "=" * 80)
        print("ステップ2: 時価総額・業種・概要を取得")
        print("=" * 80)
        
        # 最大処理銘柄数の制限
        if self.max_stocks and len(stock_data) > self.max_stocks:
            stock_data = stock_data[:self.max_stocks]
            print(f"処理銘柄数を{self.max_stocks}件に制限しました")
        
        print(f"処理対象銘柄数: {len(stock_data)}件")
        
        for i, stock in enumerate(stock_data, 1):
            code = stock['コード']
            company_name = stock['銘柄名']
            
            print(f"[{i}/{len(stock_data)}] 処理中: {code} ({company_name})")
            
            try:
                # 株探の個別ページからデータを取得
                url = f"https://kabutan.jp/stock/?code={code}"
                headers = {
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
                    )
                }
                
                resp = self.session.get(url, headers=headers, timeout=10)
                resp.raise_for_status()
                
                soup = BeautifulSoup(resp.text, "html.parser")
                
                # 時価総額を取得
                market_cap_text = None
                th_nodes = soup.find_all("th", class_="v_zika1")
                for th in th_nodes:
                    if th.get_text(strip=True) == "時価総額":
                        td = th.find_next_sibling("td", class_="v_zika2")
                        if td:
                            text = td.get_text(strip=True)
                            market_cap_text = text.replace(" ", "")
                        break
                
                # 業種を取得
                industry = None
                industry_link = soup.find("a", href=lambda x: x and "/themes/?industry=" in x)
                if industry_link:
                    industry = industry_link.get_text(strip=True)
                
                # 概要を取得
                summary = None
                summary_th = soup.find("th", string="概要")
                if summary_th:
                    summary_td = summary_th.find_next_sibling("td")
                    if summary_td:
                        summary = summary_td.get_text(strip=True)
                
                # 時価総額を正規化（億円単位）
                normalized_market_cap = self._normalize_market_cap_to_oku_number(market_cap_text)
                
                # 結果を追加
                stock['時価総額'] = normalized_market_cap
                stock['業種'] = industry
                stock['概要'] = summary
                
                print(f"  完了: 時価総額={normalized_market_cap}, 業種={industry}")
                
            except Exception as e:
                print(f"  エラー: {e}")
                stock['時価総額'] = None
                stock['業種'] = None
                stock['概要'] = None
            
            # インターバル
            if i < len(stock_data):
                time.sleep(random.uniform(0.3, 0.5))
        
        print(f"\nステップ2完了: {len(stock_data)}件処理")
        return stock_data
    
    def _normalize_market_cap_to_oku_number(self, text: Optional[str]) -> Optional[str]:
        """時価総額を億円単位の数値に正規化する"""
        if not text:
            return None
        
        import re
        s = text.strip().replace(",", "")
        # 兆円と億円を抽出
        m = re.search(r"(?:(?P<cho>[0-9]+(?:\.[0-9]+)?)兆)?(?:(?P<oku>[0-9]+(?:\.[0-9]+)?)億)?円?", s)
        if not m:
            return None
        
        cho_part = m.group("cho")
        oku_part = m.group("oku")
        
        try:
            total_oku = 0.0
            if cho_part is not None:
                total_oku += float(cho_part) * 10000.0
            if oku_part is not None:
                total_oku += float(oku_part)
            
            # 科学記法を避けてフォーマット
            formatted = ("%f" % total_oku).rstrip("0").rstrip(".")
            return formatted
        except Exception:
            return None
    
    def step3_calculate_roe(self, stock_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """ステップ3: J-Quants APIでROEを計算"""
        print("\n" + "=" * 80)
        print("ステップ3: J-Quants APIでROEを計算")
        print("=" * 80)
        
        client = self._get_jquants_client()
        
        for i, stock in enumerate(stock_data, 1):
            code = stock['コード']
            company_name = stock['銘柄名']
            
            print(f"[{i}/{len(stock_data)}] 処理中: {code} ({company_name})")
            
            try:
                # 財務データを取得
                financial_data = client.get_fins_statements(code=code)
                
                if not financial_data.empty:
                    print(f"  財務データを取得しました: {len(financial_data)} 件")
                    
                    # DisclosedDateでソート
                    if 'DisclosedDate' in financial_data.columns:
                        financial_data['DisclosedDate'] = pd.to_datetime(financial_data['DisclosedDate'])
                        financial_data = financial_data.sort_values('DisclosedDate', ascending=False).reset_index(drop=True)
                    
                    # 年次財務諸表を検索
                    roe = self._calculate_roe_from_data(financial_data)
                    
                    if roe is not None:
                        print(f"  完了: ROE = {roe:.2f}%")
                    else:
                        print(f"  スキップ: ROE計算に失敗")
                    
                    stock['ROE'] = roe
                else:
                    print(f"  スキップ: 財務データが取得できませんでした")
                    stock['ROE'] = None
                
            except Exception as e:
                print(f"  エラー: {e}")
                stock['ROE'] = None
            
            # インターバル
            if i < len(stock_data):
                time.sleep(0.1)
        
        print(f"\nステップ3完了: {len(stock_data)}件処理")
        return stock_data
    
    def _calculate_roe_from_data(self, df: pd.DataFrame) -> Optional[float]:
        """財務データからROEを計算する"""
        if df.empty:
            return None
        
        # 年次財務諸表を検索
        for _, row in df.iterrows():
            period_type = row.get('TypeOfCurrentPeriod', '')
            doc_type = row.get('TypeOfDocument', '')
            
            # 年次財務諸表かチェック
            if period_type == 'FY' and 'FinancialStatements' in doc_type:
                profit = row.get('Profit')
                equity = row.get('Equity')
                
                if profit is not None and equity is not None:
                    try:
                        profit_value = float(profit)
                        equity_value = float(equity)
                        
                        if equity_value > 0:
                            roe = (profit_value / equity_value) * 100
                            return round(roe, 1)
                    except (ValueError, TypeError):
                        continue
        
        return None
    
    def step4_calculate_financial_metrics(self, stock_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """ステップ4: 財務指標（成長率等）を計算"""
        print("\n" + "=" * 80)
        print("ステップ4: 財務指標（成長率等）を計算")
        print("=" * 80)
        
        client = self._get_jquants_client()
        
        # 新規列を追加
        new_columns = [
            '過去10年利益上昇率平均',
            '過去1年売上高上昇率_直近1', '過去1年売上高上昇率_直近2', '過去1年売上高上昇率_直近3', '過去1年売上高上昇率_直近4',
            '過去1年利益上昇率_直近1', '過去1年利益上昇率_直近2', '過去1年利益上昇率_直近3', '過去1年利益上昇率_直近4',
            'スコア'
        ]
        
        for stock in stock_data:
            for col in new_columns:
                if col not in stock:
                    stock[col] = None
        
        for i, stock in enumerate(stock_data, 1):
            code = stock['コード']
            company_name = stock['銘柄名']
            
            print(f"[{i}/{len(stock_data)}] 処理中: {code} ({company_name})")
            
            try:
                # 財務データを取得
                financial_data = client.get_fins_statements(code=code)
                
                if not financial_data.empty:
                    print(f"  財務データを取得しました: {len(financial_data)} 件")
                    
                    # 財務諸表のデータのみをフィルタリング
                    financial_data = financial_data[
                        financial_data['TypeOfDocument'].str.contains('FinancialStatements', na=False)
                    ].copy()
                    
                    if not financial_data.empty:
                        # DisclosedDateでソート
                        if 'DisclosedDate' in financial_data.columns:
                            financial_data['DisclosedDate'] = pd.to_datetime(financial_data['DisclosedDate'])
                            financial_data = financial_data.sort_values('DisclosedDate', ascending=False).reset_index(drop=True)
                        
                        # 財務指標を計算
                        metrics = self._calculate_financial_metrics(financial_data)
                        
                        # 結果を追加
                        for key, value in metrics.items():
                            stock[key] = value
                        
                        print(f"  完了: 財務指標計算完了")
                    else:
                        print(f"  スキップ: 財務諸表データが見つかりませんでした")
                else:
                    print(f"  スキップ: 財務データが取得できませんでした")
                
            except Exception as e:
                print(f"  エラー: {e}")
            
            # インターバル
            if i < len(stock_data):
                time.sleep(0.1)
        
        print(f"\nステップ4完了: {len(stock_data)}件処理")
        return stock_data
    
    def _calculate_financial_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """財務指標を計算する"""
        result = {}
        
        # 年次データのフィルタリング
        annual_data = df[
            (df['TypeOfCurrentPeriod'] == 'FY') & 
            (df['TypeOfDocument'].str.contains('FinancialStatements', na=False))
        ].copy()
        
        # 過去3年分のデータのフィルタリング
        current_date = datetime.now()
        three_years_ago = current_date - timedelta(days=1095)
        past_3years_data = df[df['DisclosedDate'] >= three_years_ago].copy()
        
        # 1. 過去10年の利益上昇率平均を計算
        if not annual_data.empty:
            annual_10years = annual_data.head(10)
            growth_rates = self._calculate_annual_profit_growth_rates(annual_10years)
            
            valid_growth_rates = [rate for rate in growth_rates if rate is not None]
            
            if valid_growth_rates:
                average_growth_rate = sum(valid_growth_rates) / len(valid_growth_rates)
                result['過去10年利益上昇率平均'] = round(average_growth_rate, 1)
            else:
                result['過去10年利益上昇率平均'] = None
        else:
            result['過去10年利益上昇率平均'] = None
        
        # 2. 過去1年の売上高・利益前年同期比上昇率を計算
        if not past_3years_data.empty:
            # 期間中値を計算
            period_values_df = self._calculate_period_values(past_3years_data)
            
            # 前年同期比上昇率を計算
            growth_rates_df = self._calculate_growth_rates(period_values_df)
            
            # 最新2年分のデータのみを抽出
            two_years_ago = current_date - timedelta(days=730)
            latest_2years_df = growth_rates_df[growth_rates_df['DisclosedDate'] >= two_years_ago].copy()
            
            # 直近4つの期間データを取得
            latest_4periods_df = latest_2years_df.head(4).copy()
            
            # 直近4つの期間の上昇率を取得
            for i, (_, row) in enumerate(latest_4periods_df.iterrows(), 1):
                sales_growth = row.get('SalesGrowthRate')
                profit_growth = row.get('ProfitGrowthRate')
                
                result[f'過去1年売上高上昇率_直近{i}'] = sales_growth
                result[f'過去1年利益上昇率_直近{i}'] = profit_growth
            
            # 4つに満たない場合は残りをNoneで埋める
            for i in range(len(latest_4periods_df) + 1, 5):
                result[f'過去1年売上高上昇率_直近{i}'] = None
                result[f'過去1年利益上昇率_直近{i}'] = None
        else:
            # 過去3年のデータがない場合、すべてNoneで初期化
            for i in range(1, 5):
                result[f'過去1年売上高上昇率_直近{i}'] = None
                result[f'過去1年利益上昇率_直近{i}'] = None
        
        # スコアを計算
        score = self._calculate_score(result)
        result['スコア'] = score
        
        return result
    
    def _calculate_annual_profit_growth_rates(self, df: pd.DataFrame) -> List[Optional[float]]:
        """年次利益の上昇率を計算する"""
        if df.empty or len(df) < 2:
            return []
        
        growth_rates = []
        
        for i in range(len(df)):
            if i == len(df) - 1:
                growth_rates.append(None)
                continue
            
            current_row = df.iloc[i]
            previous_row = df.iloc[i+1]
            
            current_profit = current_row.get('OrdinaryProfit')
            previous_profit = previous_row.get('OrdinaryProfit')
            
            try:
                current_value = float(current_profit) if current_profit is not None and current_profit != '' and current_profit != 'N/A' else None
                previous_value = float(previous_profit) if previous_profit is not None and previous_profit != '' and previous_profit != 'N/A' else None
            except (ValueError, TypeError):
                growth_rates.append(None)
                continue
            
            if current_value is not None and previous_value is not None and previous_value != 0:
                growth_rate = ((current_value - previous_value) / abs(previous_value)) * 100
                growth_rates.append(round(growth_rate, 1))
            else:
                growth_rates.append(None)
        
        return growth_rates
    
    def _calculate_period_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """累積値から期間中値を計算する"""
        if df.empty:
            return df
        
        result_df = df.copy()
        result_df['PeriodNetSales'] = None
        result_df['PeriodProfit'] = None
        
        for i, (_, row) in enumerate(result_df.iterrows()):
            period_type = row.get('TypeOfCurrentPeriod', '')
            
            # 売上高の期間中値計算
            current_sales = row.get('NetSales')
            if current_sales is not None and current_sales != '' and current_sales != 'N/A':
                try:
                    current_sales_value = float(current_sales)
                    
                    if period_type == '1Q':
                        result_df.at[i, 'PeriodNetSales'] = current_sales_value
                    else:
                        previous_sales = self._find_previous_period_sales(result_df, i, period_type)
                        if previous_sales is not None:
                            period_sales = current_sales_value - previous_sales
                            result_df.at[i, 'PeriodNetSales'] = period_sales
                        else:
                            result_df.at[i, 'PeriodNetSales'] = current_sales_value
                except (ValueError, TypeError):
                    result_df.at[i, 'PeriodNetSales'] = None
            
            # 利益の期間中値計算
            current_profit = row.get('OrdinaryProfit')
            if current_profit is not None and current_profit != '' and current_profit != 'N/A':
                try:
                    current_profit_value = float(current_profit)
                    
                    if period_type == '1Q':
                        result_df.at[i, 'PeriodProfit'] = current_profit_value
                    else:
                        previous_profit = self._find_previous_period_profit(result_df, i, period_type)
                        if previous_profit is not None:
                            period_profit = current_profit_value - previous_profit
                            result_df.at[i, 'PeriodProfit'] = period_profit
                        else:
                            result_df.at[i, 'PeriodProfit'] = current_profit_value
                except (ValueError, TypeError):
                    result_df.at[i, 'PeriodProfit'] = None
        
        return result_df
    
    def _find_previous_period_sales(self, df: pd.DataFrame, current_index: int, period_type: str) -> Optional[float]:
        """前の期間の売上高を取得する"""
        if period_type == '2Q':
            target_period = '1Q'
        elif period_type == '3Q':
            target_period = '2Q'
        elif period_type == 'FY':
            target_period = '3Q'
        else:
            return None
        
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
    
    def _find_previous_period_profit(self, df: pd.DataFrame, current_index: int, period_type: str) -> Optional[float]:
        """前の期間の利益を取得する"""
        if period_type == '2Q':
            target_period = '1Q'
        elif period_type == '3Q':
            target_period = '2Q'
        elif period_type == 'FY':
            target_period = '3Q'
        else:
            return None
        
        for i in range(current_index + 1, len(df)):
            row = df.iloc[i]
            if row.get('TypeOfCurrentPeriod') == target_period:
                profit = row.get('OrdinaryProfit')
                if profit is not None and profit != '' and profit != 'N/A':
                    try:
                        return float(profit)
                    except (ValueError, TypeError):
                        continue
        
        return None
    
    def _calculate_growth_rates(self, df: pd.DataFrame) -> pd.DataFrame:
        """前年同期比上昇率を計算する"""
        if df.empty:
            return df
        
        result_df = df.copy()
        result_df['SalesGrowthRate'] = None
        result_df['ProfitGrowthRate'] = None
        
        for i, (_, row) in enumerate(result_df.iterrows()):
            period_type = row.get('TypeOfCurrentPeriod', '')
            
            # 前年度の同じ期間タイプを検索
            previous_year_data = self._find_previous_year_same_period(result_df, i, period_type)
            
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
    
    def _find_previous_year_same_period(self, df: pd.DataFrame, current_index: int, period_type: str) -> Optional[pd.Series]:
        """前年度の同じ期間タイプのデータを取得する"""
        if period_type not in ['1Q', '2Q', '3Q', 'FY']:
            return None
        
        current_date = df.iloc[current_index].get('DisclosedDate')
        if current_date is None:
            return None
        
        try:
            if isinstance(current_date, str):
                current_year = int(current_date[:4])
            else:
                current_year = current_date.year
        except (ValueError, TypeError):
            return None
        
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
    
    def _calculate_score(self, result: Dict[str, Any]) -> Optional[float]:
        """スコアを計算する"""
        try:
            score = 0.0
            
            # profit_growth_10y: 0~10にクランプし、10で割った値の少数第1位まで
            profit_growth_10y = result.get('過去10年利益上昇率平均')
            if profit_growth_10y is not None:
                try:
                    growth_value = float(profit_growth_10y)
                    clamped_value = max(0, min(10, growth_value))
                    score += round(clamped_value / 10, 1)
                except (ValueError, TypeError):
                    pass
            
            # 過去1年売上高上昇率（直近1~4）
            sales_weights = [0.4, 0.3, 0.2, 0.1]
            for i in range(1, 5):
                sales_key = f'過去1年売上高上昇率_直近{i}'
                sales_growth = result.get(sales_key)
                if sales_growth is not None:
                    try:
                        growth_value = float(sales_growth)
                        if growth_value >= 10:
                            score += sales_weights[i-1]
                    except (ValueError, TypeError):
                        pass
            
            # 過去1年利益上昇率（直近1~4）
            profit_weights = [0.4, 0.3, 0.2, 0.1]
            for i in range(1, 5):
                profit_key = f'過去1年利益上昇率_直近{i}'
                profit_growth = result.get(profit_key)
                if profit_growth is not None:
                    try:
                        growth_value = float(profit_growth)
                        if growth_value >= 20:
                            score += profit_weights[i-1]
                    except (ValueError, TypeError):
                        pass
            
            return round(score, 1)
            
        except Exception:
            return None
    
    def save_to_csv(self, data: List[Dict[str, Any]]) -> str:
        """データをCSVファイルに保存する"""
        if not data:
            print("警告: 保存するデータがありません")
            return ""
        
        # 出力ファイル名を生成
        current_date = datetime.now()
        filename = f"{current_date.year}_{current_date.month:02d}_{current_date.day:02d}.csv"
        output_path = self.output_dir / filename
        
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'コード', '銘柄名', '市場', '株価', '前日比', '前日比（％）', 'PER', 'PBR', '利回り',
                    '時価総額', '業種', '概要', 'ROE',
                    '過去10年利益上昇率平均',
                    '過去1年売上高上昇率_直近1', '過去1年売上高上昇率_直近2', '過去1年売上高上昇率_直近3', '過去1年売上高上昇率_直近4',
                    '過去1年利益上昇率_直近1', '過去1年利益上昇率_直近2', '過去1年利益上昇率_直近3', '過去1年利益上昇率_直近4',
                    'スコア'
                ]
                
                # ヘッダー行を出力
                csvfile.write(','.join(fieldnames) + '\n')
                
                # 各行を処理して概要フィールドのみをダブルクオートで囲む
                for row in data:
                    # 数値を小数点第1位までに丸める関数
                    def format_number(value):
                        if value is None or value == '':
                            return ''
                        try:
                            return round(float(value), 1)
                        except (ValueError, TypeError):
                            return value
                    
                    # 各列の値を取得（株価と概要のみダブルクオートで囲む）
                    values = [
                        row.get('コード', ''),
                        row.get('銘柄名', ''),
                        row.get('市場', ''),
                        f'"{row.get("株価", "")}"',  # 株価をダブルクオートで囲む
                        f'"{row.get("前日比", "")}"',
                        row.get('前日比（％）', ''),
                        format_number(row.get('PER', '')),
                        format_number(row.get('PBR', '')),
                        row.get('利回り', ''),
                        format_number(row.get('時価総額', '')),
                        row.get('業種', ''),
                        f'"{row.get("概要", "")}"',  # 概要をダブルクオートで囲む
                        format_number(row.get('ROE', '')),
                        format_number(row.get('過去10年利益上昇率平均', '')),
                        format_number(row.get('過去1年売上高上昇率_直近1', '')),
                        format_number(row.get('過去1年売上高上昇率_直近2', '')),
                        format_number(row.get('過去1年売上高上昇率_直近3', '')),
                        format_number(row.get('過去1年売上高上昇率_直近4', '')),
                        format_number(row.get('過去1年利益上昇率_直近1', '')),
                        format_number(row.get('過去1年利益上昇率_直近2', '')),
                        format_number(row.get('過去1年利益上昇率_直近3', '')),
                        format_number(row.get('過去1年利益上昇率_直近4', '')),
                        format_number(row.get('スコア', ''))
                    ]
                    
                    # CSV行を出力
                    csvfile.write(','.join(str(v) for v in values) + '\n')
            
            print(f"CSVファイルを保存しました: {output_path}")
            print(f"抽出件数: {len(data)}件")
            return str(output_path)
            
        except Exception as e:
            print(f"エラー: CSVファイルの保存に失敗しました: {e}")
            return ""
    
    def run_analysis(self) -> str:
        """統合分析を実行する"""
        print("統合株価分析スクリプト開始")
        print("=" * 80)
        
        start_time = datetime.now()
        
        try:
            # ステップ1: 株探から52週高値銘柄を取得
            stock_data = self.step1_fetch_stock_list()
            
            if not stock_data:
                print("エラー: 銘柄データが取得できませんでした")
                return ""
            
            # ステップ2: 時価総額・業種・概要を取得
            stock_data = self.step2_fetch_market_cap_and_industry(stock_data)
            
            # ステップ3: J-Quants APIでROEを計算
            stock_data = self.step3_calculate_roe(stock_data)
            
            # ステップ4: 財務指標（成長率等）を計算
            stock_data = self.step4_calculate_financial_metrics(stock_data)
            
            # 結果をCSVに保存
            output_path = self.save_to_csv(stock_data)
            
            # 処理時間を計算
            end_time = datetime.now()
            processing_time = end_time - start_time
            
            print("\n" + "=" * 80)
            print("統合分析完了")
            print("=" * 80)
            print(f"処理時間: {processing_time}")
            print(f"出力ファイル: {output_path}")
            
            return output_path
            
        except Exception as e:
            print(f"エラー: 統合分析中にエラーが発生しました: {e}")
            import traceback
            traceback.print_exc()
            return ""


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(description='統合株価分析スクリプト')
    parser.add_argument('--output-dir', '-o', default='data', help='出力ディレクトリ（デフォルト: data）')
    parser.add_argument('--max-stocks', '-m', type=int, help='最大処理銘柄数（テスト用）')
    
    args = parser.parse_args()
    
    try:
        analyzer = IntegratedStockAnalyzer(
            output_dir=args.output_dir,
            max_stocks=args.max_stocks
        )
        
        output_path = analyzer.run_analysis()
        
        if output_path:
            print(f"\n処理が完了しました: {output_path}")
        else:
            print("\n処理が失敗しました")
            sys.exit(1)
            
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
