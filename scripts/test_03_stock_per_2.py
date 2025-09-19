#!/usr/bin/env python3
"""
株探サイトから指定された銘柄コードのPER（実績PERと予想PER）を取得するスクリプト

使用方法:
    python test_03_stock_per_2.py <銘柄コード>
    
例:
    python test_03_stock_per_2.py 7203  # トヨタ自動車
    python test_03_stock_per_2.py 6758  # ソニーグループ

前提条件:
    - requestsライブラリがインストールされていること
    - beautifulsoup4ライブラリがインストールされていること
"""

import sys
import re
import time
import random
from pathlib import Path
from typing import Optional, Dict, Any
import requests
from bs4 import BeautifulSoup


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


def fetch_per_data_from_kabutan(code: str, session: requests.Session) -> Dict[str, Any]:
    """
    株探サイトから指定された銘柄のPERデータを取得する
    
    Args:
        code (str): 銘柄コード
        session: requests.Session
        
    Returns:
        dict: PERデータの辞書
    """
    url = f"https://kabutan.jp/stock/?code={code}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        )
    }
    
    try:
        resp = session.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
    except Exception as exc:
        return {
            'code': code,
            'error': f"HTTP error: {exc}",
            'actual_per': None,
            'forecast_per': None,
            'pbr': None,
            'yield': None,
            'market_cap': None,
            'company_name': None,
            'current_price': None
        }

    try:
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # 会社名を取得
        company_name = None
        title_tag = soup.find("title")
        if title_tag:
            title_text = title_tag.get_text(strip=True)
            # タイトルから会社名を抽出（例: "トヨタ自動車(7203) - 株価・株式投資情報 - 株探"）
            match = re.search(r"^(.+?)\(\d{4}\)", title_text)
            if match:
                company_name = match.group(1)
        
        # PERテーブルから実績PERを取得
        actual_per = None
        forecast_per = None
        pbr = None
        yield_value = None
        
        # すべてのテーブルを検索してPERテーブルを見つける
        tables = soup.find_all("table")
        per_table = None
        
        for table in tables:
            thead = table.find("thead")
            if thead:
                th_elements = thead.find_all("th")
                th_texts = [th.get_text(strip=True) for th in th_elements]
                if "PER" in th_texts and "PBR" in th_texts:
                    per_table = table
                    break
        
        if per_table:
            # テーブルヘッダーを確認
            thead = per_table.find("thead")
            if thead:
                th_elements = thead.find_all("th")
                per_col_index = None
                pbr_col_index = None
                yield_col_index = None
                
                for i, th in enumerate(th_elements):
                    th_text = th.get_text(strip=True)
                    if "PER" in th_text:
                        per_col_index = i
                    elif "PBR" in th_text:
                        pbr_col_index = i
                    elif "利回り" in th_text:
                        yield_col_index = i
                
                # テーブルボディから値を取得
                tbody = per_table.find("tbody")
                if tbody:
                    rows = tbody.find_all("tr")
                    
                    # 最初の行（実績値）を取得
                    if rows and len(rows) > 0:
                        first_row = rows[0]
                        cells = first_row.find_all("td")
                        
                        if per_col_index is not None and per_col_index < len(cells):
                            per_cell = cells[per_col_index]
                            per_text = per_cell.get_text(strip=True)
                            # "14.4倍" から "14.4" を抽出
                            per_match = re.search(r"([0-9]+(?:\.[0-9]+)?)", per_text)
                            if per_match:
                                actual_per = float(per_match.group(1))
                        
                        if pbr_col_index is not None and pbr_col_index < len(cells):
                            pbr_cell = cells[pbr_col_index]
                            pbr_text = pbr_cell.get_text(strip=True)
                            pbr_match = re.search(r"([0-9]+(?:\.[0-9]+)?)", pbr_text)
                            if pbr_match:
                                pbr = float(pbr_match.group(1))
                        
                        if yield_col_index is not None and yield_col_index < len(cells):
                            yield_cell = cells[yield_col_index]
                            yield_text = yield_cell.get_text(strip=True)
                            yield_match = re.search(r"([0-9]+(?:\.[0-9]+)?)", yield_text)
                            if yield_match:
                                yield_value = float(yield_match.group(1))
        
        # 現在の株価を取得
        current_price = None
        price_elements = soup.find_all("span", class_="kabuka")
        if price_elements:
            price_text = price_elements[0].get_text(strip=True)
            price_match = re.search(r"([0-9,]+(?:\.[0-9]+)?)", price_text.replace(",", ""))
            if price_match:
                current_price = float(price_match.group(1))
        
        # 予想PERを別のテーブルから取得（決算情報テーブル）
        if forecast_per is None and current_price is not None:
            for table in tables:
                tbody = table.find("tbody")
                if tbody:
                    rows = tbody.find_all("tr")
                    for row in rows:
                        cells = row.find_all(["td", "th"])
                        if len(cells) >= 2:
                            first_cell = cells[0].get_text(strip=True)
                            if "予" in first_cell and "2026" in first_cell:
                                # 予想決算の行を見つけた場合、1株益から予想PERを計算
                                if len(cells) >= 5:  # 1株益の列がある場合
                                    eps_text = cells[4].get_text(strip=True)  # 1株益の列
                                    eps_match = re.search(r"([0-9]+(?:\.[0-9]+)?)", eps_text)
                                    if eps_match:
                                        forecast_eps = float(eps_match.group(1))
                                        # 現在の株価から予想PERを計算
                                        if forecast_eps > 0:
                                            forecast_per = current_price / forecast_eps
                                break
        
        # 時価総額を取得
        market_cap = None
        market_cap_th = soup.find("th", string="時価総額")
        if market_cap_th:
            market_cap_td = market_cap_th.find_next_sibling("td")
            if market_cap_td:
                market_cap_text = market_cap_td.get_text(strip=True)
                market_cap = re.sub(r"\s+", "", market_cap_text)
        
        return {
            'code': code,
            'error': None,
            'actual_per': actual_per,
            'forecast_per': forecast_per,
            'pbr': pbr,
            'yield': yield_value,
            'market_cap': market_cap,
            'company_name': company_name,
            'current_price': current_price
        }
        
    except Exception as exc:
        return {
            'code': code,
            'error': f"Parse error: {exc}",
            'actual_per': None,
            'forecast_per': None,
            'pbr': None,
            'yield': None,
            'market_cap': None,
            'company_name': None,
            'current_price': None
        }


def display_per_analysis(per_data: Dict[str, Any]):
    """
    PER分析結果を表示する
    
    Args:
        per_data (dict): PERデータの辞書
    """
    if per_data['error']:
        print(f"エラー: {per_data['error']}")
        return
    
    print(f"\n銘柄コード: {per_data['code']}")
    if per_data['company_name']:
        print(f"会社名: {per_data['company_name']}")
    print("=" * 80)
    
    # 基本情報
    print("【基本情報】")
    if per_data.get('current_price'):
        print(f"現在の株価: {per_data['current_price']:,.0f} 円")
    if per_data['market_cap']:
        print(f"時価総額: {per_data['market_cap']}")
    else:
        print("時価総額: データなし")
    
    # PER分析結果
    print("\n【PER分析】")
    if per_data['forecast_per'] is not None:
        print(f"PER: {per_data['forecast_per']:.1f} 倍")
        
        # PERの評価
        print("\n【PER評価】")
        if per_data['forecast_per'] < 10:
            print("PER < 10: 割安（ただし、業績悪化の可能性も考慮）")
        elif per_data['forecast_per'] < 15:
            print("10 ≤ PER < 15: 適正水準")
        elif per_data['forecast_per'] < 20:
            print("15 ≤ PER < 20: やや割高")
        elif per_data['forecast_per'] < 30:
            print("20 ≤ PER < 30: 割高")
        else:
            print("PER ≥ 30: 大幅に割高（成長期待が高い可能性）")
    else:
        print("PER: データなし")
    
    # その他の指標
    print("\n【その他の指標】")
    if per_data['pbr'] is not None:
        print(f"PBR: {per_data['pbr']:.2f} 倍")
    else:
        print("PBR: データなし")
    
    if per_data['yield'] is not None:
        print(f"利回り: {per_data['yield']:.2f}%")
    else:
        print("利回り: データなし")
    
    # 参考情報
    print("\n【参考情報】")
    print("- PER: 株価を1株当たり純利益で割った値（株価収益率）")
    print("- 一般的にPERが低いほど割安、高いほど割高とされます")
    print("- 業界平均や過去のPERと比較することが重要です")
    print("- 成長性の高い企業はPERが高くなる傾向があります")


def main():
    """メイン処理"""
    try:
        # 引数の確認
        if len(sys.argv) != 2:
            print("使用方法: python test_03_stock_per_2.py <銘柄コード>")
            print("例: python test_03_stock_per_2.py 7203")
            sys.exit(1)
        
        stock_code = sys.argv[1]
        
        # 銘柄コードの検証
        if not validate_stock_code(stock_code):
            print(f"エラー: 無効な銘柄コードです: {stock_code}")
            print("銘柄コードは4桁の数字で入力してください。")
            sys.exit(1)
        
        print("株探サイト PER分析スクリプト")
        print("=" * 50)
        
        # セッションを作成
        session = requests.Session()
        
        # 株探サイトからPERデータを取得
        print(f"銘柄コード {stock_code} のPERデータを取得中...")
        per_data = fetch_per_data_from_kabutan(stock_code, session)
        
        # 結果の表示
        display_per_analysis(per_data)
        
        print("\n処理が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
