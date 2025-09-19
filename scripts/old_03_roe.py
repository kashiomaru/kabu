#!/usr/bin/env python3
"""
CSVファイルから銘柄コードを読み取り、J-Quants APIを使用してROEを計算し、CSVに追加するスクリプト

使用方法:
    python main_01_stock_roe.py <CSVファイルパス>
    
例:
    python main_01_stock_roe.py ../data/2025_09_16.csv

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


def is_valid_financial_data(latest_data):
    """
    財務データが有効かどうかを判定する（年次データでROE計算に必要なデータがあるかチェック）
    
    Args:
        latest_data: 財務データの1行
        
    Returns:
        bool: 有効な財務データかどうか
    """
    # 期間タイプをチェック（年次データのみ）
    period_type = latest_data.get('TypeOfCurrentPeriod', '')
    
    # 年次データかどうかをチェック
    is_annual = period_type == 'FY'
    
    # TypeOfDocumentをチェック（財務諸表であることを確認）
    doc_type = latest_data.get('TypeOfDocument', '')
    is_financial_statement = 'FinancialStatements' in doc_type
    
    # 当期純利益が存在するかチェック
    profit = latest_data.get('Profit')
    has_profit = profit is not None and profit != ''
    
    # 純資産が存在するかチェック
    equity = latest_data.get('Equity')
    has_equity = equity is not None and equity != ''
    
    # 年次財務諸表で、かつ必要なデータが揃っているかチェック
    return is_annual and is_financial_statement and has_profit and has_equity


def extract_financial_metrics(df, code):
    """
    財務データから必要な財務指標を抽出する
    
    Args:
        df (pandas.DataFrame): 財務データのデータフレーム
        code (str): 銘柄コード
        
    Returns:
        dict: 抽出した財務指標（有効なデータが見つからない場合はNone）
    """
    if df.empty:
        return None
    
    # 有効な財務データを検索
    valid_data = None
    for _, row in df.iterrows():
        if is_valid_financial_data(row):
            valid_data = row
            break
    
    if valid_data is None:
        # デバッグ用に最新データを表示
        if not df.empty:
            latest_data = df.iloc[0]
            doc_type = latest_data.get('TypeOfDocument', '')
            period_type = latest_data.get('TypeOfCurrentPeriod', '')
            print(f"    デバッグ: TypeOfDocument='{doc_type}', TypeOfCurrentPeriod='{period_type}'")
        return None
    
    # 財務指標の抽出
    metrics = {
        'code': code,
        'profit': valid_data.get('Profit', None),  # 当期純利益
        'equity': valid_data.get('Equity', None),  # 純資産
    }
    
    return metrics


def get_financial_statements(client, code):
    """
    J-Quants APIから指定された銘柄の財務データを取得する
    
    Args:
        client: J-Quants APIクライアント
        code (str): 銘柄コード
        
    Returns:
        dict: 財務指標の辞書（取得できない場合はNone）
    """
    try:
        # 銘柄コードのみで財務データを取得（日付指定なし）
        financial_data = client.get_fins_statements(code=code)
        
        if not financial_data.empty:
            print(f"    財務データを取得しました: {len(financial_data)} 件")
            # DisclosedDateでソート（最新が最初に来るように降順）
            if 'DisclosedDate' in financial_data.columns:
                financial_data['DisclosedDate'] = pd.to_datetime(financial_data['DisclosedDate'])
                financial_data = financial_data.sort_values('DisclosedDate', ascending=False).reset_index(drop=True)
            
            # 年次連結財務諸表を検索
            metrics = extract_financial_metrics(financial_data, code)
            if metrics is not None:
                print(f"    年次連結財務諸表を発見")
                return metrics
            else:
                print(f"    年次連結財務諸表が見つかりませんでした")
                return None
        else:
            print(f"    財務データが見つかりませんでした")
            return None
        
    except Exception as e:
        print(f"  エラー: 銘柄コード {code} の財務データ取得中にエラー: {e}")
        return None


def calculate_roe(profit, equity):
    """
    ROE（自己資本利益率）を計算する
    
    Args:
        profit (float): 当期純利益
        equity (float): 純資産
        
    Returns:
        float: ROE（%）、計算できない場合はNone
    """
    try:
        if profit is None or equity is None:
            return None
        
        # 数値に変換
        try:
            profit_value = float(profit) if isinstance(profit, (int, float, str)) else 0
            equity_value = float(equity) if isinstance(equity, (int, float, str)) else 0
        except (ValueError, TypeError):
            return None
        
        if equity_value <= 0:
            return None
        
        # ROEを計算（%表示）
        roe = (profit_value / equity_value) * 100
        
        # 小数第1位に丸める
        roe = round(roe, 1)
        
        return roe
        
    except Exception as e:
        return None


def process_stock_roe(client, code, company_name):
    """
    単一銘柄のROEを計算する
    
    Args:
        client: J-Quants APIクライアント
        code (str): 銘柄コード
        company_name (str): 会社名（ログ用）
        
    Returns:
        float: ROE（%）、計算できない場合はNone
    """
    print(f"  処理中: {code} ({company_name})")
    
    # 財務データの取得
    financial_metrics = get_financial_statements(client, code)
    
    if not financial_metrics:
        print(f"    スキップ: 財務データが取得できませんでした")
        return None
    
    # ROEの計算
    profit = financial_metrics.get('profit')
    equity = financial_metrics.get('equity')
    
    if profit is not None:
        try:
            profit = float(profit) if isinstance(profit, (int, float, str)) else 0
        except (ValueError, TypeError):
            profit = None
    
    if equity is not None:
        try:
            equity = float(equity) if isinstance(equity, (int, float, str)) else 0
        except (ValueError, TypeError):
            equity = None
    
    roe = calculate_roe(profit, equity)
    
    if roe is not None:
        print(f"    完了: ROE = {roe:.2f}%")
    else:
        print(f"    スキップ: ROE計算に失敗")
    
    return roe


def main():
    """メイン処理"""
    try:
        # 引数の確認
        if len(sys.argv) != 2:
            print("使用方法: python main_01_stock_roe.py <CSVファイルパス>")
            print("例: python main_01_stock_roe.py ../data/2025_09_16.csv")
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
        
        print("CSV銘柄ROE計算スクリプト")
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
        
        # ROE列の確認
        if 'ROE' in df.columns:
            print("ROE列が既に存在します。既存のROE列を更新します。")
        else:
            print("ROE列を新規作成します。")
            df['ROE'] = None
        
        # 銘柄コード列の確認
        if 'コード' not in df.columns:
            print("エラー: 'コード'列が見つかりません。")
            sys.exit(1)
        
        # 銘柄名列の確認（ログ用）
        company_name_col = '銘柄名' if '銘柄名' in df.columns else None
        
        # 各銘柄のROEを計算
        print(f"\nROE計算を開始します...")
        processed_count = 0
        success_count = 0
        
        for index, row in df.iterrows():
            code = str(row['コード']).zfill(4)  # 4桁にゼロパディング
            company_name = row[company_name_col] if company_name_col else f"銘柄{code}"
            
            # 既にROEが計算済みの場合はスキップ
            if pd.notna(row['ROE']) and row['ROE'] != '':
                print(f"  スキップ: {code} ({company_name}) - 既にROEが計算済み")
                continue
            
            # ROE計算
            roe = process_stock_roe(client, code, company_name)
            
            if roe is not None:
                df.at[index, 'ROE'] = roe
                success_count += 1
            
            processed_count += 1
            
            # 進捗表示
            if processed_count % 10 == 0:
                print(f"  進捗: {processed_count}/{len(df)} 件処理完了")
            
            # API制限を考慮して少し待機（最適化により短縮）
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
