#!/usr/bin/env python3
"""
株探サイトから指定された銘柄コードの時価総額を取得するスクリプト

使用方法:
    python test_04_stock_total_2.py <銘柄コード>
    
例:
    python test_04_stock_total_2.py 7203  # トヨタ自動車
    python test_04_stock_total_2.py 6758  # ソニーグループ

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


def fetch_market_cap_from_kabutan(code: str, session: requests.Session) -> Dict[str, Any]:
    """
    株探サイトから指定された銘柄の時価総額を取得する
    
    Args:
        code (str): 銘柄コード
        session: requests.Session
        
    Returns:
        dict: 時価総額データの辞書
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
            'market_cap_text': None,
            'market_cap_oku': None,
            'company_name': None
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
        
        # 時価総額を取得
        market_cap_text = None
        market_cap_oku = None
        
        # 時価総額のth要素を検索
        market_cap_th = soup.find("th", class_="v_zika1", string="時価総額")
        if market_cap_th:
            market_cap_td = market_cap_th.find_next_sibling("td", class_="v_zika2")
            if market_cap_td:
                market_cap_text = market_cap_td.get_text(strip=True)
                # 空白を除去
                market_cap_text = re.sub(r"\s+", "", market_cap_text)
                
                # 億円単位に正規化
                market_cap_oku = normalize_market_cap_to_oku_number(market_cap_text)
        
        return {
            'code': code,
            'error': None,
            'market_cap_text': market_cap_text,
            'market_cap_oku': market_cap_oku,
            'company_name': company_name
        }
        
    except Exception as exc:
        return {
            'code': code,
            'error': f"Parse error: {exc}",
            'market_cap_text': None,
            'market_cap_oku': None,
            'company_name': None
        }


def normalize_market_cap_to_oku_number(text: Optional[str]) -> Optional[str]:
    """
    時価総額を億円単位の数値文字列に正規化する
    
    Args:
        text (str): 時価総額のテキスト（例: "46兆6,900億円"）
        
    Returns:
        str: 億円単位の数値文字列（例: "466900"）
        
    Examples:
        "46兆6,900億円" -> "466900"
        "705億円" -> "705"
        "78.3億円" -> "78.3"
    """
    if not text:
        return None
    
    # カンマを除去
    s = text.strip().replace(",", "")
    
    # 正規表現で「兆」と「億」の部分を抽出
    # 例: "46兆6,900億円" -> cho_part="46", oku_part="6900"
    m = re.search(r"(?:(?P<cho>[0-9]+(?:\.[0-9]+)?)兆)?(?:(?P<oku>[0-9]+(?:\.[0-9]+)?)億)?円?", s)
    if not m:
        return None
    
    cho_part = m.group("cho")
    oku_part = m.group("oku")
    
    try:
        total_oku = 0.0
        
        # 兆円部分を億円に変換（1兆円 = 10,000億円）
        if cho_part is not None:
            total_oku += float(cho_part) * 10000.0
        
        # 億円部分をそのまま追加
        if oku_part is not None:
            total_oku += float(oku_part)
        
        # 科学記法を避けて、末尾の0を除去してフォーマット
        formatted = ("%f" % total_oku).rstrip("0").rstrip(".")
        return formatted
        
    except Exception:
        return None


def display_market_cap_analysis(market_cap_data: Dict[str, Any]):
    """
    時価総額分析結果を表示する
    
    Args:
        market_cap_data (dict): 時価総額データの辞書
    """
    if market_cap_data['error']:
        print(f"エラー: {market_cap_data['error']}")
        return
    
    print(f"\n銘柄コード: {market_cap_data['code']}")
    if market_cap_data['company_name']:
        print(f"会社名: {market_cap_data['company_name']}")
    print("=" * 80)
    
    # 時価総額情報
    print("【時価総額情報】")
    if market_cap_data['market_cap_text']:
        print(f"時価総額（元データ）: {market_cap_data['market_cap_text']}")
    else:
        print("時価総額（元データ）: データなし")
    
    if market_cap_data['market_cap_oku']:
        market_cap_oku = float(market_cap_data['market_cap_oku'])
        print(f"時価総額（億円）: {market_cap_oku:,.0f} 億円")
        
        # 時価総額の評価
        print("\n【時価総額評価】")
        if market_cap_oku >= 100000:  # 10兆円以上
            print("時価総額 ≥ 10兆円: 超大企業（トヨタ、ソニー等）")
        elif market_cap_oku >= 10000:  # 1兆円以上
            print("時価総額 ≥ 1兆円: 大企業（日経平均構成銘柄レベル）")
        elif market_cap_oku >= 1000:  # 1000億円以上
            print("時価総額 ≥ 1000億円: 中堅企業")
        elif market_cap_oku >= 100:  # 100億円以上
            print("時価総額 ≥ 100億円: 中小企業")
        else:
            print("時価総額 < 100億円: 小企業")
    else:
        print("時価総額（億円）: データなし")
    
    # 参考情報
    print("\n【参考情報】")
    print("- 時価総額 = 株価 × 発行済み株式数")
    print("- 企業の規模を測る重要な指標です")
    print("- 市場での評価額を表します")


def main():
    """メイン処理"""
    try:
        # 引数の確認
        if len(sys.argv) != 2:
            print("使用方法: python test_04_stock_total_2.py <銘柄コード>")
            print("例: python test_04_stock_total_2.py 7203")
            sys.exit(1)
        
        stock_code = sys.argv[1]
        
        # 銘柄コードの検証
        if not validate_stock_code(stock_code):
            print(f"エラー: 無効な銘柄コードです: {stock_code}")
            print("銘柄コードは4桁の数字で入力してください。")
            sys.exit(1)
        
        print("株探サイト 時価総額取得スクリプト")
        print("=" * 50)
        
        # セッションを作成
        session = requests.Session()
        
        # 株探サイトから時価総額データを取得
        print(f"銘柄コード {stock_code} の時価総額データを取得中...")
        market_cap_data = fetch_market_cap_from_kabutan(stock_code, session)
        
        # 結果の表示
        display_market_cap_analysis(market_cap_data)
        
        print("\n処理が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
