#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
株価データ抽出スクリプト
HTMLテーブルから株価情報を抽出し、CSV形式で出力します。
"""

import os
import sys
import csv
import glob
import time
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
import argparse
import requests
import random


def fetch_html_from_url(url, max_retries=3, delay=1):
    """
    URLからHTMLコンテンツを取得する
    
    Args:
        url (str): 取得するURL
        max_retries (int): 最大リトライ回数
        delay (int): リトライ間隔（秒）
        
    Returns:
        str: HTMLコンテンツ
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    for attempt in range(max_retries):
        try:
            print(f"URLからHTMLを取得中: {url} (試行 {attempt + 1}/{max_retries})")
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # エンコーディングを自動検出
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
                raise
    
    return None


def extract_table_from_html(html_content, table_class='stock_table st_market'):
    """
    HTMLコンテンツから指定されたクラスのテーブルを抽出する
    
    Args:
        html_content (str): HTMLコンテンツ
        table_class (str): 抽出するテーブルのクラス名
        
    Returns:
        str: テーブルのHTML
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 指定されたクラスのテーブルを検索
        table = soup.find('table', class_=table_class)
        if not table:
            print(f"警告: クラス '{table_class}' のテーブルが見つかりません")
            return None
        
        print(f"テーブルを発見: {table_class}")
        return str(table)
        
    except Exception as e:
        print(f"エラー: テーブル抽出に失敗しました: {e}")
        return None


def extract_stock_data_from_html(html_file_path):
    """
    HTMLファイルから株価データを抽出する
    
    Args:
        html_file_path (str): HTMLファイルのパス
        
    Returns:
        list: 抽出された株価データのリスト
    """
    stock_data = []
    
    try:
        with open(html_file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # テーブルを検索
        table = soup.find('table', class_='stock_table')
        if not table:
            print(f"警告: {html_file_path} にテーブルが見つかりません")
            return stock_data
        
        # tbody内の行を取得
        tbody = table.find('tbody')
        if not tbody:
            print(f"警告: {html_file_path} にtbodyが見つかりません")
            return stock_data
        
        rows = tbody.find_all('tr')
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 13:  # 必要な列数があるかチェック
                try:
                    # 各列からデータを抽出
                    code_cell = cells[0]
                    code = code_cell.get_text(strip=True)
                    
                    name_cell = cells[1]
                    name = name_cell.get_text(strip=True)
                    
                    market_cell = cells[2]
                    market = market_cell.get_text(strip=True)
                    
                    price_cell = cells[5]  # 株価は6列目（0ベースで5）
                    price = price_cell.get_text(strip=True)
                    
                    # 前日比（金額とパーセント）
                    change_amount_cell = cells[7]
                    change_amount = change_amount_cell.get_text(strip=True)
                    change_percent_cell = cells[8]
                    change_percent = change_percent_cell.get_text(strip=True)

                    
                    per_cell = cells[10]  # PERは11列目
                    per = per_cell.get_text(strip=True)
                    
                    pbr_cell = cells[11]  # PBRは12列目
                    pbr = pbr_cell.get_text(strip=True)
                    
                    yield_cell = cells[12]  # 利回りは13列目
                    yield_val = yield_cell.get_text(strip=True)
                    
                    # データを辞書として保存
                    stock_data.append({
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
                    print(f"警告: {html_file_path} の行でデータ抽出エラー: {e}")
                    continue
    
    except Exception as e:
        print(f"エラー: {html_file_path} の読み込みに失敗しました: {e}")
    
    return stock_data


def generate_kabutan_urls():
    """
    株探の52週高値ページのURLを生成する
    
    Returns:
        list: 生成されたURLのリスト
    """
    base_url = "https://kabutan.jp/warning/record_w52_high_price"
    urls = []
    
    for market in range(1, 4):  # market=1,2,3
        for page in range(1, 4):  # page=1,2,3
            url = f"{base_url}?market={market}&page={page}"
            urls.append(url)
    
    return urls


def extract_stock_data_from_url(url, table_class='stock_table st_market'):
    """
    URLから株価データを抽出する
    
    Args:
        url (str): 取得するURL
        table_class (str): 抽出するテーブルのクラス名
        
    Returns:
        list: 抽出された株価データのリスト
    """
    try:
        # URLからHTMLを取得
        html_content = fetch_html_from_url(url)
        if not html_content:
            return []
        
        # テーブルを抽出
        table_html = extract_table_from_html(html_content, table_class)
        if not table_html:
            return []
        
        # テーブルからデータを抽出
        soup = BeautifulSoup(table_html, 'html.parser')
        stock_data = []
        
        # tbody内の行を取得
        tbody = soup.find('tbody')
        if not tbody:
            print("警告: tbodyが見つかりません")
            return stock_data
        
        rows = tbody.find_all('tr')
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 13:  # 必要な列数があるかチェック
                try:
                    # 各列からデータを抽出
                    code_cell = cells[0]
                    code = code_cell.get_text(strip=True)
                    
                    name_cell = cells[1]
                    name = name_cell.get_text(strip=True)
                    
                    market_cell = cells[2]
                    market = market_cell.get_text(strip=True)
                    
                    price_cell = cells[5]  # 株価は6列目（0ベースで5）
                    price = price_cell.get_text(strip=True)
                    
                    # 前日比（金額とパーセント）
                    change_amount_cell = cells[7]
                    change_amount = change_amount_cell.get_text(strip=True)
                    change_percent_cell = cells[8]
                    change_percent = change_percent_cell.get_text(strip=True)

                    
                    per_cell = cells[10]  # PERは11列目
                    per = per_cell.get_text(strip=True)
                    
                    pbr_cell = cells[11]  # PBRは12列目
                    pbr = pbr_cell.get_text(strip=True)
                    
                    yield_cell = cells[12]  # 利回りは13列目
                    yield_val = yield_cell.get_text(strip=True)
                    
                    # データを辞書として保存
                    stock_data.append({
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
                    print(f"警告: 行でデータ抽出エラー: {e}")
                    continue
        
        return stock_data
        
    except Exception as e:
        print(f"エラー: URLからのデータ抽出に失敗しました: {e}")
        return []


def extract_stock_data_from_multiple_urls(urls, table_class='stock_table st_market'):
    """
    複数のURLから株価データを抽出する
    
    Args:
        urls (list): 取得するURLのリスト
        table_class (str): 抽出するテーブルのクラス名
        
    Returns:
        list: 抽出された株価データのリスト
    """
    all_data = []
    successful_urls = 0
    skipped_urls = 0
    
    print(f"処理対象URL: {len(urls)}件")
    
    for i, url in enumerate(urls, 1):
        print(f"\n[{i}/{len(urls)}] 処理中: {url}")
        
        try:
            # URLからHTMLを取得
            html_content = fetch_html_from_url(url)
            if not html_content:
                print(f"  スキップ: HTML取得失敗")
                skipped_urls += 1
                # インターバル
                time.sleep(random.uniform(0.1, 0.3))
                continue
            
            # テーブルが存在するかチェック
            soup = BeautifulSoup(html_content, 'html.parser')
            table = soup.find('table', class_=table_class)
            
            if not table:
                print(f"  スキップ: テーブルが見つかりません")
                skipped_urls += 1
                time.sleep(random.uniform(0.1, 0.3))
                continue
            
            # テーブルからデータを抽出
            tbody = table.find('tbody')
            if not tbody:
                print(f"  スキップ: tbodyが見つかりません")
                skipped_urls += 1
                time.sleep(random.uniform(0.1, 0.3))
                continue
            
            rows = tbody.find_all('tr')
            if not rows:
                print(f"  スキップ: データ行が見つかりません")
                skipped_urls += 1
                time.sleep(random.uniform(0.1, 0.3))
                continue
            
            # データを抽出
            page_data = []
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 13:  # 必要な列数があるかチェック
                    try:
                        # 各列からデータを抽出
                        code_cell = cells[0]
                        code = code_cell.get_text(strip=True)
                        
                        name_cell = cells[1]
                        name = name_cell.get_text(strip=True)
                        
                        market_cell = cells[2]
                        market = market_cell.get_text(strip=True)
                        
                        price_cell = cells[5]  # 株価は6列目（0ベースで5）
                        price = price_cell.get_text(strip=True)
                        
                        # 前日比（金額とパーセント）
                        change_amount_cell = cells[7]
                        change_amount = change_amount_cell.get_text(strip=True)
                        change_percent_cell = cells[8]
                        change_percent = change_percent_cell.get_text(strip=True)
    
                        
                        per_cell = cells[10]  # PERは11列目
                        per = per_cell.get_text(strip=True)
                        
                        pbr_cell = cells[11]  # PBRは12列目
                        pbr = pbr_cell.get_text(strip=True)
                        
                        yield_cell = cells[12]  # 利回りは13列目
                        yield_val = yield_cell.get_text(strip=True)
                        
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
                all_data.extend(page_data)
                successful_urls += 1
                print(f"  成功: {len(page_data)}件のデータを抽出")
            else:
                print(f"  スキップ: 抽出可能なデータがありません")
                skipped_urls += 1
            
        except Exception as e:
            print(f"  エラー: {e}")
            skipped_urls += 1
            
        # インターバル（正常・異常問わず毎回）
        time.sleep(random.uniform(0.1, 0.3))
    
    print(f"\n処理結果:")
    print(f"  成功: {successful_urls}件")
    print(f"  スキップ: {skipped_urls}件")
    print(f"  総抽出件数: {len(all_data)}件")
    
    return all_data


def save_to_csv(data, output_file_path):
    """
    データをCSVファイルに保存する
    
    Args:
        data (list): 保存するデータのリスト
        output_file_path (str): 出力ファイルのパス
    """
    if not data:
        print("警告: 保存するデータがありません")
        return
    
    try:
        with open(output_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['コード', '銘柄名', '市場', '株価', '前日比', '前日比（％）', 'PER', 'PBR', '利回り']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            writer.writerows(data)
        
        print(f"CSVファイルを保存しました: {output_file_path}")
        print(f"抽出件数: {len(data)}件")
        
    except Exception as e:
        print(f"エラー: CSVファイルの保存に失敗しました: {e}")


def generate_output_filename():
    """
    現在の日時から出力ファイル名を生成する
    
    Returns:
        str: 出力ファイル名（例: 2025_01_15.csv）
    """
    now = datetime.now()
    return f"{now.year}_{now.month:02d}_{now.day:02d}.csv"


def setup_output_directory(output_dir_arg):
    """
    出力先ディレクトリを設定する
    
    Args:
        output_dir_arg (str): コマンドライン引数で指定された出力先ディレクトリ
        
    Returns:
        Path: 設定された出力先ディレクトリのPathオブジェクト
    """
    if output_dir_arg:
        # コマンドライン引数で指定された場合
        output_dir = Path(output_dir_arg)
    else:
        # デフォルトでdataフォルダを使用
        output_dir = Path('data')
    
    # ディレクトリが存在しない場合は作成
    output_dir.mkdir(parents=True, exist_ok=True)
    
    return output_dir


def main():
    """
    メイン処理
    """
    parser = argparse.ArgumentParser(description='HTMLテーブルから株価データを抽出してCSVに出力')
    parser.add_argument('input_path', nargs='?', help='処理するフォルダのパスまたはURL（省略時は固定URLで複数ページ取得）')
    parser.add_argument('--output', '-o', help='出力ファイル名（指定しない場合は日時で自動生成）')
    parser.add_argument('--output-dir', '-d', help='出力先ディレクトリ（指定しない場合はdataフォルダ）')
    parser.add_argument('--url', '-u', action='store_true', help='入力パスをURLとして扱う')
    parser.add_argument('--table-class', '-t', default='stock_table st_market', help='抽出するテーブルのクラス名')
    parser.add_argument('--multi-page', '-m', action='store_true', help='固定URLで複数ページ（market=1-3, page=1-3）を取得')
    
    args = parser.parse_args()
    
    all_data = []
    
    if args.multi_page or (not args.input_path and not args.url):
        # 複数ページ取得モード（デフォルト）
        print("複数ページ取得モード: 株探の52週高値ページ（market=1-3, page=1-3）")
        
        urls = generate_kabutan_urls()
        data = extract_stock_data_from_multiple_urls(urls, args.table_class)
        all_data.extend(data)
        
        if not all_data:
            print("エラー: どのページからもデータが抽出されませんでした")
            sys.exit(1)
        
        # 出力先ディレクトリを設定
        output_dir = setup_output_directory(args.output_dir)
        
    elif args.url:
        # 単一URL指定の場合
        url = args.input_path
        print(f"URLからデータを取得: {url}")
        
        data = extract_stock_data_from_url(url, args.table_class)
        all_data.extend(data)
        
        if not all_data:
            print("エラー: URLからデータが抽出されませんでした")
            sys.exit(1)
        
        # 出力先ディレクトリを設定
        output_dir = setup_output_directory(args.output_dir)
        
    else:
        # フォルダ指定の場合
        folder_path = Path(args.input_path)
        if not folder_path.exists():
            print(f"エラー: フォルダが見つかりません: {folder_path}")
            sys.exit(1)
        
        if not folder_path.is_dir():
            print(f"エラー: 指定されたパスはフォルダではありません: {folder_path}")
            sys.exit(1)
        
        # HTMLファイルを検索
        html_files = list(folder_path.glob('*'))
        html_files = [f for f in html_files if f.is_file() and not f.name.endswith('.csv')]
        
        if not html_files:
            print(f"警告: {folder_path} にHTMLファイルが見つかりません")
            sys.exit(1)
        
        print(f"処理対象ファイル: {len(html_files)}件")
        for file in html_files:
            print(f"  - {file.name}")
        
        # 全データを統合
        for html_file in html_files:
            print(f"\n処理中: {html_file.name}")
            data = extract_stock_data_from_html(str(html_file))
            all_data.extend(data)
            print(f"  抽出件数: {len(data)}件")
        
        if not all_data:
            print("エラー: データが抽出されませんでした")
            sys.exit(1)
        
        # 出力先ディレクトリを設定
        if args.output_dir:
            # 出力先が指定されている場合はそのディレクトリを使用
            output_dir = setup_output_directory(args.output_dir)
        else:
            # 出力先が指定されていない場合は指定されたフォルダを使用
            output_dir = folder_path
    
    # 出力ファイル名の決定
    if args.output:
        output_filename = args.output
    else:
        output_filename = generate_output_filename()
    
    output_path = output_dir / output_filename
    
    # 出力先ディレクトリの情報を表示
    print(f"出力先ディレクトリ: {output_dir}")
    
    # CSVファイルに保存
    save_to_csv(all_data, str(output_path))
    
    print(f"\n処理完了!")
    print(f"総抽出件数: {len(all_data)}件")
    print(f"出力ファイル: {output_path}")


if __name__ == '__main__':
    main()
