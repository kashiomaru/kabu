#!/usr/bin/env python3
"""
J-Quants APIを使用して指定された銘柄コードのROE（自己資本利益率）を計算・表示するスクリプト

使用方法:
    python test_05_stock_roe.py <銘柄コード>
    
例:
    python test_05_stock_roe.py 7203  # トヨタ自動車
    python test_05_stock_roe.py 6758  # ソニーグループ

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
        dict: 財務指標の辞書
        
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
            return extract_financial_metrics(financial_data, code)
        else:
            print(f"銘柄コード {code} の財務データが見つかりませんでした")
            return None
        
    except Exception as e:
        raise Exception(f"財務データの取得中にエラーが発生しました: {e}")


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
            print(f"有効な財務データを発見: {row.get('DisclosedDate', 'N/A')}")
            break
    
    if valid_data is None:
        print("有効な財務データが見つかりませんでした")
        # デバッグ用に最新データを表示
        latest_data = df.iloc[0]
        print(f"\n最新データの内容:")
        for col, value in latest_data.items():
            if col in ['DisclosedDate', 'TypeOfDocument', 'TypeOfCurrentPeriod', 'Profit', 'Equity', 'NetSales']:
                print(f"  {col}: {value}")
        
        # 文書タイプの説明
        doc_type = latest_data.get('TypeOfDocument', '')
        period_type = latest_data.get('TypeOfCurrentPeriod', '')
        print(f"\n文書タイプ分析:")
        print(f"  TypeOfDocument: {doc_type}")
        print(f"  TypeOfCurrentPeriod: {period_type}")
        print(f"  期待値: TypeOfCurrentPeriod='FY', TypeOfDocumentに'FinancialStatements'を含む")
        print(f"  年次財務諸表か: {period_type == 'FY' and 'FinancialStatements' in doc_type}")
        
        return None
    
    # 財務指標の抽出
    metrics = {
        'code': code,
        'disclosed_date': valid_data.get('DisclosedDate', 'N/A'),
        'fiscal_year': valid_data.get('FiscalYear', 'N/A'),
        'fiscal_period': valid_data.get('FiscalPeriod', 'N/A'),
        'doc_type': valid_data.get('TypeOfDocument', 'N/A'),  # 文書タイプ
        'period_type': valid_data.get('TypeOfCurrentPeriod', 'N/A'),  # 期間タイプ
        'profit': valid_data.get('Profit', None),  # 当期純利益
        'equity': valid_data.get('Equity', None),  # 純資産
        'net_sales': valid_data.get('NetSales', None),  # 売上高
        'company_name': valid_data.get('LocalCode', code),  # 会社名（利用可能な場合）
    }
    
    return metrics


def calculate_roe(profit, equity):
    """
    ROE（自己資本利益率）を計算する
    
    Args:
        profit (float): 当期純利益
        equity (float): 純資産
        
    Returns:
        dict: ROE計算結果
    """
    try:
        if profit is None or equity is None:
            return {
                'roe': None,
                'error': '当期純利益または純資産が取得できませんでした'
            }
        
        # 数値に変換
        try:
            profit_value = float(profit) if isinstance(profit, (int, float, str)) else 0
            equity_value = float(equity) if isinstance(equity, (int, float, str)) else 0
        except (ValueError, TypeError):
            return {
                'roe': None,
                'error': '財務データの数値変換に失敗しました'
            }
        
        if equity_value <= 0:
            return {
                'roe': None,
                'error': '純資産が0以下です（ROE計算不可）'
            }
        
        # ROEを計算（%表示）
        roe = (profit_value / equity_value) * 100
        
        return {
            'roe': roe,
            'error': None
        }
        
    except Exception as e:
        return {
            'roe': None,
            'error': f'ROE計算中にエラーが発生しました: {e}'
        }


def evaluate_roe(roe):
    """
    ROEを評価する
    
    Args:
        roe (float): ROE（%）
        
    Returns:
        dict: 評価結果
    """
    if roe is None:
        return {
            'category': '不明',
            'description': 'データ不足のため評価できません'
        }
    
    # ROEの分類（一般的な基準）
    if roe >= 15:
        return {
            'category': '優秀',
            'description': '15%以上の優秀なROE'
        }
    elif roe >= 10:
        return {
            'category': '良好',
            'description': '10%以上の良好なROE'
        }
    elif roe >= 5:
        return {
            'category': '普通',
            'description': '5%以上の普通のROE'
        }
    elif roe >= 0:
        return {
            'category': '低い',
            'description': '0%以上の低いROE'
        }
    else:
        return {
            'category': 'マイナス',
            'description': 'マイナスのROE（赤字）'
        }


def display_roe_analysis(financial_metrics, roe_result):
    """
    ROE分析結果を表示する
    
    Args:
        financial_metrics (dict): 財務指標の辞書
        roe_result (dict): ROE計算結果
    """
    print(f"\n銘柄コード: {financial_metrics['code']}")
    print(f"開示日: {financial_metrics['disclosed_date']}")
    print(f"会計年度: {financial_metrics['fiscal_year']}")
    print(f"会計期間: {financial_metrics['fiscal_period']}")
    print(f"文書タイプ: {financial_metrics['doc_type']}")
    print(f"期間タイプ: {financial_metrics['period_type']}")
    print("=" * 80)
    
    # 基本情報
    print("【基本情報】")
    if financial_metrics['profit'] is not None:
        try:
            profit_value = float(financial_metrics['profit']) if isinstance(financial_metrics['profit'], (int, float, str)) else 0
            print(f"当期純利益: {profit_value:,.0f} 円")
        except (ValueError, TypeError):
            print(f"当期純利益: {financial_metrics['profit']} 円")
    else:
        print("当期純利益: データなし")
    
    if financial_metrics['equity'] is not None:
        try:
            equity_value = float(financial_metrics['equity']) if isinstance(financial_metrics['equity'], (int, float, str)) else 0
            print(f"純資産: {equity_value:,.0f} 円")
        except (ValueError, TypeError):
            print(f"純資産: {financial_metrics['equity']} 円")
    else:
        print("純資産: データなし")
    
    if financial_metrics['net_sales'] is not None:
        try:
            sales_value = float(financial_metrics['net_sales']) if isinstance(financial_metrics['net_sales'], (int, float, str)) else 0
            print(f"売上高: {sales_value:,.0f} 円")
        except (ValueError, TypeError):
            print(f"売上高: {financial_metrics['net_sales']} 円")
    else:
        print("売上高: データなし")
    
    # ROE分析結果
    print("\n【ROE分析】")
    if roe_result['error']:
        print(f"エラー: {roe_result['error']}")
    else:
        roe = roe_result['roe']
        print(f"自己資本利益率(ROE): {roe:.2f}%")
        
        # ROEの評価
        evaluation = evaluate_roe(roe)
        print(f"評価: {evaluation['category']} ({evaluation['description']})")
        
        # 参考情報
        print("\n【参考情報】")
        print("- ROE = 当期純利益 ÷ 純資産 × 100")
        print("- ROEは企業の収益性を測る重要な指標です")
        print("- 一般的にROEが高いほど効率的に利益を上げているとされます")
        print("- 業界平均や過去のROEと比較することが重要です")
        print("- 成長性の高い企業はROEが高くなる傾向があります")


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


def main():
    """メイン処理"""
    try:
        # 引数の確認
        if len(sys.argv) != 2:
            print("使用方法: python test_05_stock_roe.py <銘柄コード>")
            print("例: python test_05_stock_roe.py 7203")
            sys.exit(1)
        
        stock_code = sys.argv[1]
        
        # 銘柄コードの検証
        if not validate_stock_code(stock_code):
            print(f"エラー: 無効な銘柄コードです: {stock_code}")
            print("銘柄コードは4桁の数字で入力してください。")
            sys.exit(1)
        
        # スクリプトのディレクトリを取得
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        token_file_path = project_root / "token.txt"
        
        print("J-Quants API ROE分析スクリプト")
        print("=" * 50)
        
        # APIキーの読み込み
        print(f"APIキーを読み込み中: {token_file_path}")
        refresh_token = load_api_key(token_file_path)
        print("APIキーの読み込み完了")
        
        # クライアントの初期化
        client = jquantsapi.Client(refresh_token=refresh_token)
        
        # 財務データの取得
        financial_metrics = get_financial_statements(client, stock_code)
        
        if not financial_metrics:
            print("エラー: 財務データが取得できませんでした")
            sys.exit(1)
        
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
        
        roe_result = calculate_roe(profit, equity)
        
        # 結果の表示
        display_roe_analysis(financial_metrics, roe_result)
        
        print("\n処理が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
