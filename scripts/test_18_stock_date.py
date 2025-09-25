#!/usr/bin/env python3
"""
トレーダーズ・ウェブから決算発表スケジュールを取得するスクリプト

使用方法:
    python test_18_stock_date.py [--output-csv] [--max-pages MAX_PAGES]
    
例:
    python test_18_stock_date.py
    python test_18_stock_date.py --output-csv
    python test_18_stock_date.py --max-pages 5

前提条件:
    - requestsライブラリがインストールされていること
    - beautifulsoup4ライブラリがインストールされていること

特徴:
    - トレーダーズ・ウェブの決算発表スケジュールから情報を取得
    - 発表日、銘柄名、銘柄コード、決算種別を抽出
    - CSV出力オプション対応
    - 複数ページの取得に対応
"""

import os
import sys
import csv
import time
import random
import argparse
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
import requests
from bs4 import BeautifulSoup


class TradersWebScraper:
    """トレーダーズ・ウェブ決算発表スケジュール取得クラス"""
    
    def __init__(self, max_pages: int = 10):
        """
        初期化
        
        Args:
            max_pages (int): 最大取得ページ数
        """
        self.max_pages = max_pages
        self.session = requests.Session()
        self.base_url = "https://www.traders.co.jp/market_jp/earnings_calendar/all/all_ex_etf"
        self.earnings_data = []
        
    def _get_headers(self) -> Dict[str, str]:
        """HTTPリクエストヘッダーを取得する"""
        return {
            'User-Agent': (
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
            ),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    
    def _fetch_html_from_url(self, url: str, max_retries: int = 3, delay: int = 1) -> Optional[str]:
        """URLからHTMLコンテンツを取得する"""
        headers = self._get_headers()
        
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
    
    def _extract_earnings_data_from_html(self, html_content: str) -> List[Dict[str, Any]]:
        """HTMLコンテンツから決算発表情報を抽出する"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 決算発表テーブルを検索
            table = soup.find('table', class_='data_table table inner_elm')
            if not table:
                print("警告: 決算発表テーブルが見つかりません")
                return []
            
            # tbody内の行を取得
            tbody = table.find('tbody')
            if not tbody:
                print("警告: tbodyが見つかりません")
                return []
            
            rows = tbody.find_all('tr')
            if not rows:
                print("警告: データ行が見つかりません")
                return []
            
            # ヘッダー行をスキップ（最初の行）
            data_rows = rows[1:] if len(rows) > 1 else []
            
            page_data = []
            for row in data_rows:
                try:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 6:  # 必要な列数があるかチェック
                        # 発表日を取得
                        announcement_date = cells[0].get_text(strip=True)
                        
                        # 時刻を取得（通常は"-"）
                        time_str = cells[1].get_text(strip=True)
                        
                        # 銘柄名とコードを取得
                        company_cell = cells[2]
                        company_link = company_cell.find('a')
                        company_name = company_link.get_text(strip=True) if company_link else ""
                        
                        # 銘柄コードと市場を取得
                        code_market_text = company_cell.get_text(strip=True)
                        code_match = re.search(r'\((\d+)/([^)]+)\)', code_market_text)
                        stock_code = code_match.group(1) if code_match else ""
                        market = code_match.group(2) if code_match else ""
                        
                        # 決算種別を取得
                        earnings_type = cells[3].get_text(strip=True)
                        
                        # 業種を取得
                        industry = cells[4].get_text(strip=True)
                        
                        # 時価総額を取得
                        market_cap = cells[5].get_text(strip=True)
                        
                        # データを辞書として保存
                        page_data.append({
                            '発表日': announcement_date,
                            '時刻': time_str,
                            '銘柄名': company_name,
                            '銘柄コード': stock_code,
                            '市場': market,
                            '決算種別': earnings_type,
                            '業種': industry,
                            '時価総額（億円）': market_cap
                        })
                        
                except (IndexError, AttributeError) as e:
                    print(f"警告: 行でデータ抽出エラー: {e}")
                    continue
            
            if page_data:
                print(f"成功: {len(page_data)}件の決算発表情報を抽出")
            else:
                print("スキップ: 抽出可能なデータがありません")
            
            return page_data
            
        except Exception as e:
            print(f"エラー: HTML解析中にエラーが発生しました: {e}")
            return []
    
    def _generate_page_urls(self) -> List[str]:
        """複数ページのURLを生成する"""
        urls = []
        for page in range(1, self.max_pages + 1):
            url = f"{self.base_url}/{page}?term=future"
            urls.append(url)
        return urls
    
    def fetch_earnings_schedule(self) -> List[Dict[str, Any]]:
        """決算発表スケジュールを取得する"""
        print("=" * 80)
        print("トレーダーズ・ウェブ決算発表スケジュール取得")
        print("=" * 80)
        
        urls = self._generate_page_urls()
        all_data = []
        successful_pages = 0
        skipped_pages = 0
        
        print(f"処理対象URL: {len(urls)}件")
        
        for i, url in enumerate(urls, 1):
            print(f"\n[{i}/{len(urls)}] 処理中: {url}")
            
            # URLからHTMLを取得
            html_content = self._fetch_html_from_url(url)
            if not html_content:
                skipped_pages += 1
                continue
            
            # データを抽出
            page_data = self._extract_earnings_data_from_html(html_content)
            
            if page_data:
                all_data.extend(page_data)
                successful_pages += 1
            else:
                skipped_pages += 1
            
            # インターバル（レート制限対策）
            if i < len(urls):
                time.sleep(random.uniform(0.5, 1.0))
        
        print(f"\n取得完了:")
        print(f"  成功ページ: {successful_pages}件")
        print(f"  スキップページ: {skipped_pages}件")
        print(f"  総抽出件数: {len(all_data)}件")
        
        self.earnings_data = all_data
        return all_data
    
    def save_to_csv(self, output_file: str = None) -> str:
        """データをCSVファイルに保存する"""
        if not self.earnings_data:
            print("警告: 保存するデータがありません")
            return ""
        
        if output_file is None:
            # デフォルトのファイル名を生成
            current_date = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"earnings_schedule_{current_date}.csv"
        
        try:
            # プロジェクトルートのdataフォルダに出力
            script_dir = Path(__file__).parent
            project_root = script_dir.parent
            data_dir = project_root / "data"
            data_dir.mkdir(exist_ok=True)
            
            output_path = data_dir / output_file
            
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    '発表日', '時刻', '銘柄名', '銘柄コード', '市場', 
                    '決算種別', '業種', '時価総額（億円）'
                ]
                
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for row in self.earnings_data:
                    writer.writerow(row)
            
            print(f"CSVファイルを保存しました: {output_path}")
            return str(output_path)
            
        except Exception as e:
            print(f"エラー: CSVファイルの保存に失敗しました: {e}")
            return ""
    
    def display_summary(self):
        """取得結果のサマリーを表示する"""
        if not self.earnings_data:
            print("表示するデータがありません")
            return
        
        print("\n" + "=" * 80)
        print("取得結果サマリー")
        print("=" * 80)
        print(f"総件数: {len(self.earnings_data)} 件")
        
        # 発表日別の件数
        date_counts = {}
        for item in self.earnings_data:
            date = item.get('発表日', '')
            date_counts[date] = date_counts.get(date, 0) + 1
        
        print(f"\n発表日別件数:")
        for date, count in sorted(date_counts.items()):
            print(f"  {date}: {count} 件")
        
        # 決算種別別の件数
        type_counts = {}
        for item in self.earnings_data:
            earnings_type = item.get('決算種別', '')
            type_counts[earnings_type] = type_counts.get(earnings_type, 0) + 1
        
        print(f"\n決算種別別件数:")
        for earnings_type, count in sorted(type_counts.items()):
            print(f"  {earnings_type}: {count} 件")
        
        # 市場別の件数
        market_counts = {}
        for item in self.earnings_data:
            market = item.get('市場', '')
            market_counts[market] = market_counts.get(market, 0) + 1
        
        print(f"\n市場別件数:")
        for market, count in sorted(market_counts.items()):
            print(f"  {market}: {count} 件")
    
    def display_recent_data(self, limit: int = 10):
        """最近のデータを表示する"""
        if not self.earnings_data:
            print("表示するデータがありません")
            return
        
        print(f"\n最近の決算発表情報（最新{limit}件）:")
        print("-" * 100)
        print(f"{'発表日':<8} {'銘柄名':<20} {'コード':<6} {'市場':<4} {'決算種別':<8} {'業種':<15}")
        print("-" * 100)
        
        for i, item in enumerate(self.earnings_data[:limit]):
            print(f"{item.get('発表日', ''):<8} "
                  f"{item.get('銘柄名', '')[:18]:<20} "
                  f"{item.get('銘柄コード', ''):<6} "
                  f"{item.get('市場', ''):<4} "
                  f"{item.get('決算種別', ''):<8} "
                  f"{item.get('業種', '')[:13]:<15}")


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(description='トレーダーズ・ウェブ決算発表スケジュール取得スクリプト')
    parser.add_argument('--output-csv', '-o', action='store_true', help='CSVファイルに出力する')
    parser.add_argument('--max-pages', '-p', type=int, default=10, help='最大取得ページ数（デフォルト: 10）')
    parser.add_argument('--display-limit', '-l', type=int, default=20, help='表示する件数（デフォルト: 20）')
    
    args = parser.parse_args()
    
    try:
        print("トレーダーズ・ウェブ決算発表スケジュール取得スクリプト")
        print("=" * 60)
        
        # スクレイパーを初期化
        scraper = TradersWebScraper(max_pages=args.max_pages)
        
        # 決算発表スケジュールを取得
        earnings_data = scraper.fetch_earnings_schedule()
        
        if not earnings_data:
            print("エラー: 決算発表情報が取得できませんでした")
            sys.exit(1)
        
        # サマリーを表示
        scraper.display_summary()
        
        # 最近のデータを表示
        scraper.display_recent_data(limit=args.display_limit)
        
        # CSV出力
        if args.output_csv:
            output_file = scraper.save_to_csv()
            if output_file:
                print(f"\nCSVファイルが保存されました: {output_file}")
        
        print(f"\n処理が完了しました。")
        print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
