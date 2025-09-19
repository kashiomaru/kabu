#!/usr/bin/env python3
"""
J-Quants APIを使用して銘柄コードファイルから最新の株価を取得するスクリプト

使用方法:
    python get_latest_stock_price.py <銘柄コードファイル>
    
例:
    python get_latest_stock_price.py ../data/stock_codes.txt

前提条件:
    - token.txtファイルにAPIキー（リフレッシュトークン）が記述されていること
    - jquants-api-clientライブラリがインストールされていること
    - 銘柄コードファイルは1行に1つの銘柄コードが記載されていること
"""

import os
import sys
import json
from pathlib import Path
import jquantsapi
import pandas as pd
from datetime import datetime, timedelta
import time


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


def load_stock_codes(file_path):
    """
    銘柄コードファイルを読み込む
    
    Args:
        file_path (str): 銘柄コードファイルのパス
        
    Returns:
        list: 銘柄コードのリスト
        
    Raises:
        FileNotFoundError: ファイルが見つからない場合
        ValueError: ファイルが空の場合
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            codes = [line.strip() for line in file if line.strip()]
            
        if not codes:
            raise ValueError("銘柄コードファイルが空です")
            
        # 銘柄コードの形式を検証
        valid_codes = []
        for code in codes:
            if code.isdigit() and len(code) == 4:
                valid_codes.append(code)
            else:
                print(f"警告: 無効な銘柄コードをスキップしました: {code}")
        
        if not valid_codes:
            raise ValueError("有効な銘柄コードが見つかりませんでした")
            
        return valid_codes
        
    except FileNotFoundError:
        raise FileNotFoundError(f"銘柄コードファイルが見つかりません: {file_path}")
    except Exception as e:
        raise Exception(f"銘柄コードファイルの読み込み中にエラーが発生しました: {e}")


def get_latest_stock_price(client, code):
    """
    J-Quants APIから指定された銘柄の最新株価を取得する
    
    Args:
        client: J-Quants APIクライアント
        code (str): 銘柄コード
        
    Returns:
        dict: 最新株価データの辞書（取得失敗時はNone）
    """
    try:
        # 最新の株価データを取得（過去7日間から最新を取得）
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')
        
        print(f"銘柄コード {code} の最新株価を取得中...")
        
        # 株価データの取得
        price_data = client.get_prices_daily_quotes(
            code=code,
            from_yyyymmdd=start_date,
            to_yyyymmdd=end_date
        )
        
        # データフレームに変換
        df = pd.DataFrame(price_data)
        
        if df.empty:
            print(f"警告: 銘柄コード {code} のデータが見つかりませんでした")
            return None
        
        # 日付でソート（最新が最初に来るように降順）
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.sort_values('Date', ascending=False).reset_index(drop=True)
        
        # 最新のデータを取得
        latest_data = df.iloc[0]
        
        return {
            'code': code,
            'date': latest_data.get('Date', 'N/A'),
            'close': latest_data.get('Close', None),
            'open': latest_data.get('Open', None),
            'high': latest_data.get('High', None),
            'low': latest_data.get('Low', None),
            'volume': latest_data.get('Volume', None)
        }
        
    except Exception as e:
        print(f"エラー: 銘柄コード {code} の株価取得に失敗しました: {e}")
        return None


def get_multiple_stock_prices(client, codes):
    """
    複数の銘柄コードの最新株価を取得する
    
    Args:
        client: J-Quants APIクライアント
        codes (list): 銘柄コードのリスト
        
    Returns:
        list: 株価データのリスト（取得順序を保持）
    """
    results = []
    
    print(f"総銘柄数: {len(codes)}件")
    print("=" * 50)
    
    for i, code in enumerate(codes, 1):
        print(f"[{i}/{len(codes)}] 処理中: {code}")
        
        # APIレート制限を考慮して少し待機
        if i > 1:
            time.sleep(0.5)
        
        price_data = get_latest_stock_price(client, code)
        results.append(price_data)
        
        if price_data:
            close_price = price_data.get('close', 'N/A')
            if close_price != 'N/A':
                print(f"  最新終値: {close_price:,}円")
            else:
                print(f"  最新終値: データなし")
        else:
            print(f"  取得失敗")
    
    return results


def save_stock_prices_to_file(results, output_file_path):
    """
    株価データをファイルに保存する
    
    Args:
        results (list): 株価データのリスト
        output_file_path (str): 出力ファイルのパス
    """
    try:
        # 既存ファイルがある場合は削除
        if os.path.exists(output_file_path):
            os.remove(output_file_path)
            print(f"既存ファイルを削除しました: {output_file_path}")
        
        # ファイルに保存
        with open(output_file_path, 'w', encoding='utf-8') as file:
            for result in results:
                if result and result.get('close'):
                    file.write(f"{result['close']}\n")
                else:
                    file.write("\n")
        
        print(f"株価データを保存しました: {output_file_path}")
        
    except Exception as e:
        print(f"エラー: ファイル保存に失敗しました: {e}")


def output_results(results, output_format='simple'):
    """
    結果を出力する
    
    Args:
        results (list): 株価データのリスト
        output_format (str): 出力形式 ('simple', 'detailed', 'csv', 'values_only')
    """
    if output_format == 'simple':
        # シンプルな形式（銘柄コード: 終値）
        print("\n" + "=" * 50)
        print("最新株価結果")
        print("=" * 50)
        
        for result in results:
            if result and result.get('close'):
                print(f"{result['code']}: {result['close']:,}円")
            else:
                code = result['code'] if result else 'N/A'
                print(f"{code}: データなし")
    
    elif output_format == 'values_only':
        # 値のみの形式（スプレッドシート用）
        for result in results:
            if result and result.get('close'):
                print(result['close'])
            else:
                print("")
    
    elif output_format == 'detailed':
        # 詳細な形式
        print("\n" + "=" * 80)
        print("最新株価詳細結果")
        print("=" * 80)
        
        for result in results:
            if result:
                print(f"銘柄コード: {result['code']}")
                print(f"  日付: {result['date']}")
                print(f"  終値: {result['close']:,}円" if result.get('close') else "  終値: データなし")
                print(f"  始値: {result['open']:,}円" if result.get('open') else "  始値: データなし")
                print(f"  高値: {result['high']:,}円" if result.get('high') else "  高値: データなし")
                print(f"  安値: {result['low']:,}円" if result.get('low') else "  安値: データなし")
                print(f"  出来高: {result['volume']:,}株" if result.get('volume') else "  出来高: データなし")
                print("-" * 40)
            else:
                print("データなし")
                print("-" * 40)
    
    elif output_format == 'csv':
        # CSV形式
        print("\n銘柄コード,終値,日付")
        for result in results:
            if result and result.get('close'):
                print(f"{result['code']},{result['close']},{result['date']}")
            else:
                code = result['code'] if result else 'N/A'
                print(f"{code},,N/A")


def main():
    """メイン処理"""
    try:
        # 引数の確認
        if len(sys.argv) != 2:
            print("使用方法: python get_latest_stock_price.py <銘柄コードファイル>")
            print("例: python get_latest_stock_price.py ../data/stock_codes.txt")
            sys.exit(1)
        
        stock_codes_file = sys.argv[1]
        
        # スクリプトのディレクトリを取得
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        token_file_path = project_root / "token.txt"
        
        print("J-Quants API 最新株価取得スクリプト")
        print("=" * 50)
        
        # APIキーの読み込み
        print(f"APIキーを読み込み中: {token_file_path}")
        refresh_token = load_api_key(token_file_path)
        print("APIキーの読み込み完了")
        
        # 銘柄コードファイルの読み込み
        print(f"銘柄コードファイルを読み込み中: {stock_codes_file}")
        stock_codes = load_stock_codes(stock_codes_file)
        print(f"銘柄コード読み込み完了: {len(stock_codes)}件")
        
        # クライアントの初期化
        client = jquantsapi.Client(refresh_token=refresh_token)
        
        # 株価データの取得
        results = get_multiple_stock_prices(client, stock_codes)
        
        # 結果の出力（値のみ）
        output_results(results, 'values_only')
        
        # 株価データをファイルに保存
        stock_codes_file_path = Path(stock_codes_file)
        output_file_path = stock_codes_file_path.parent / "stock_prices.txt"
        save_stock_prices_to_file(results, str(output_file_path))
        
        print("\n処理が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
