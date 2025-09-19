#!/usr/bin/env python3
"""
J-Quants APIを使用して指定された銘柄コードの財務データのTypeOfDocumentを表示するスクリプト

使用方法:
    python test_10_statement.py <銘柄コード>
    
例:
    python test_10_statement.py 7203  # トヨタ自動車
    python test_10_statement.py 6758  # ソニーグループ
    python test_10_statement.py 228A  # アルファベット付き銘柄コード

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


def display_type_of_document_info(df, code):
    """
    財務データのTypeOfDocument情報を表示する
    
    Args:
        df (pandas.DataFrame): 財務データのデータフレーム
        code (str): 銘柄コード
    """
    if df.empty:
        print(f"銘柄コード {code} の財務データが見つかりませんでした。")
        return
    
    print(f"\n銘柄コード: {code}")
    print("=" * 80)
    print("【財務データのTypeOfDocument一覧】")
    print("-" * 50)
    
    # TypeOfDocumentの一意な値を取得
    if 'TypeOfDocument' in df.columns:
        unique_documents = df['TypeOfDocument'].unique()
        print(f"取得した財務データ件数: {len(df)} 件")
        print(f"TypeOfDocumentの種類数: {len(unique_documents)} 種類")
        
        print(f"\n【TypeOfDocumentの種類】")
        print("-" * 50)
        for i, doc_type in enumerate(unique_documents, 1):
            print(f"{i:2d}. {doc_type}")
        
        # 各TypeOfDocumentの詳細情報を表示
        print(f"\n【各TypeOfDocumentの詳細情報】")
        print("-" * 80)
        
        for doc_type in unique_documents:
            # 該当するTypeOfDocumentのデータをフィルタリング
            doc_data = df[df['TypeOfDocument'] == doc_type]
            
            print(f"\nTypeOfDocument: {doc_type}")
            print(f"  データ件数: {len(doc_data)} 件")
            
            # 最新のデータを表示
            if not doc_data.empty:
                latest_data = doc_data.iloc[0]
                print(f"  最新データの開示日: {latest_data.get('DisclosedDate', 'N/A')}")
                print(f"  最新データの期間タイプ: {latest_data.get('TypeOfCurrentPeriod', 'N/A')}")
                print(f"  最新データの会計年度: {latest_data.get('FiscalYear', 'N/A')}")
                print(f"  最新データの会計期間: {latest_data.get('FiscalPeriod', 'N/A')}")
            
            # 期間タイプの分布を表示
            if 'TypeOfCurrentPeriod' in doc_data.columns:
                period_counts = doc_data['TypeOfCurrentPeriod'].value_counts()
                print(f"  期間タイプ分布:")
                for period, count in period_counts.items():
                    print(f"    {period}: {count} 件")
            
            # 開示日の範囲を表示
            if 'DisclosedDate' in doc_data.columns and not doc_data.empty:
                try:
                    # 日付をdatetimeに変換
                    dates = pd.to_datetime(doc_data['DisclosedDate'])
                    print(f"  開示日範囲: {dates.min().strftime('%Y-%m-%d')} ～ {dates.max().strftime('%Y-%m-%d')}")
                except Exception as e:
                    print(f"  開示日範囲: 計算エラー ({e})")
        
        # 全データの時系列表示
        print(f"\n【全データの時系列表示】")
        print("-" * 120)
        print(f"{'開示日':<12} {'TypeOfDocument':<50} {'TypeOfCurrentPeriod':<20} {'FiscalYear':<12} {'FiscalPeriod':<15}")
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
            
            # TypeOfDocumentの表示（長すぎる場合は省略）
            doc_type = row.get('TypeOfDocument', 'N/A')
            doc_display = doc_type if len(str(doc_type)) <= 50 else str(doc_type)[:47] + "..."
            
            # その他の情報
            period_type = row.get('TypeOfCurrentPeriod', 'N/A')
            fiscal_year = row.get('FiscalYear', 'N/A')
            fiscal_period = row.get('FiscalPeriod', 'N/A')
            
            print(f"{date_display:<12} {doc_display:<50} {str(period_type):<20} {str(fiscal_year):<12} {str(fiscal_period):<15}")
            
            # 表示件数を制限（最初の20件のみ）
            if i >= 19:
                remaining = len(df) - 20
                if remaining > 0:
                    print(f"... 他 {remaining} 件")
                break
    
    else:
        print("TypeOfDocument列が見つかりませんでした。")


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
            print("使用方法: python test_10_statement.py <銘柄コード>")
            print("例: python test_10_statement.py 7203")
            print("例: python test_10_statement.py 228A")
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
        
        print("J-Quants API 財務データTypeOfDocument表示スクリプト")
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
        
        # 結果の表示
        display_type_of_document_info(df, stock_code)
        
        print("\n処理が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
