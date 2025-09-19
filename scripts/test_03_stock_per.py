#!/usr/bin/env python3
"""
J-Quants APIを使用して指定された銘柄コードのPER（株価収益率）を計算・表示するスクリプト

使用方法:
    python test_03_stock_per.py <銘柄コード>
    
例:
    python test_03_stock_per.py 7203  # トヨタ自動車
    python test_03_stock_per.py 6758  # ソニーグループ

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


def get_stock_price(client, code, days=30):
    """
    J-Quants APIから指定された銘柄の最新株価を取得する
    
    Args:
        client: J-Quants APIクライアント
        code (str): 銘柄コード
        days (int): 取得する日数（デフォルト30日）
        
    Returns:
        float: 最新の株価（終値）
        
    Raises:
        Exception: API接続エラー
    """
    try:
        # 日付範囲を設定（過去指定日数間）
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
        
        print(f"銘柄コード {code} の株価データを取得中...")
        print(f"期間: {start_date} ～ {end_date}")
        
        # 株価データの取得
        price_data = client.get_prices_daily_quotes(
            code=code,
            from_yyyymmdd=start_date,
            to_yyyymmdd=end_date
        )
        
        # データフレームに変換
        df = pd.DataFrame(price_data)
        
        if df.empty:
            raise ValueError(f"銘柄コード {code} の株価データが見つかりませんでした")
        
        # 日付でソート（最新が最初に来るように降順）
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.sort_values('Date', ascending=False).reset_index(drop=True)
        
        # 最新の株価を取得
        latest_price = df.iloc[0].get('Close')
        if latest_price is None:
            raise ValueError(f"銘柄コード {code} の最新株価データが取得できませんでした")
        
        return float(latest_price)
        
    except Exception as e:
        raise Exception(f"株価データの取得中にエラーが発生しました: {e}")


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


def extract_financial_metrics(df, code):
    """
    財務データから必要な財務指標を抽出する
    
    Args:
        df (pandas.DataFrame): 財務データのデータフレーム
        code (str): 銘柄コード
        
    Returns:
        dict: 抽出した財務指標
    """
    if df.empty:
        return None
    
    # 最新のデータを取得（DisclosedDateでソート済みのため最初の行）
    latest_data = df.iloc[0]
    
    # 財務指標の抽出
    metrics = {
        'code': code,
        'disclosed_date': latest_data.get('DisclosedDate', 'N/A'),
        'fiscal_year': latest_data.get('FiscalYear', 'N/A'),
        'fiscal_period': latest_data.get('FiscalPeriod', 'N/A'),
        'doc_type': latest_data.get('TypeOfDocument', 'N/A'),  # 文書タイプ
        'profit': latest_data.get('Profit', None),  # 当期純利益
        'issued_shares': latest_data.get('NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock', None),  # 発行済み株式数
        'earnings_per_share': latest_data.get('EarningsPerShare', None),  # 1株当たり純利益（APIから直接取得）
    }
    
    return metrics


def calculate_per(stock_price, financial_metrics):
    """
    PER（株価収益率）を計算する
    
    Args:
        stock_price (float): 最新の株価
        financial_metrics (dict): 財務指標の辞書
        
    Returns:
        dict: PER計算結果
    """
    try:
        # APIから提供されるEPSを優先的に使用
        api_eps = financial_metrics.get('earnings_per_share')
        
        if api_eps is not None:
            # APIから提供されるEPSを使用
            try:
                eps_value = float(api_eps) if isinstance(api_eps, (int, float, str)) else 0
                
                # PERを計算（EPSがマイナスでも計算する）
                per = stock_price / eps_value
                
                return {
                    'per': per,
                    'eps': eps_value,
                    'error': None,
                    'eps_source': 'API'
                }
                
            except (ValueError, TypeError):
                return {
                    'per': None,
                    'eps': None,
                    'error': 'APIから取得したEPSの数値変換に失敗しました'
                }
        
        # APIからEPSが取得できない場合は手動計算
        profit = financial_metrics.get('profit')
        issued_shares = financial_metrics.get('issued_shares')
        
        if profit is None or issued_shares is None:
            return {
                'per': None,
                'eps': None,
                'error': '財務データが不足しています（EPS、当期純利益、または発行済み株式数が取得できません）'
            }
        
        # 数値に変換
        try:
            profit_value = float(profit) if isinstance(profit, (int, float, str)) else 0
            shares_value = float(issued_shares) if isinstance(issued_shares, (int, float, str)) else 0
        except (ValueError, TypeError):
            return {
                'per': None,
                'eps': None,
                'error': '財務データの数値変換に失敗しました'
            }
        
        if shares_value <= 0:
            return {
                'per': None,
                'eps': None,
                'error': '発行済み株式数が0以下です'
            }
        
        # EPS（1株当たり純利益）を計算
        # J-Quants APIの財務データは円単位で提供されているため、直接計算
        eps = profit_value / shares_value
        
        # PERを計算（EPSがマイナスでも計算する）
        per = stock_price / eps
        
        return {
            'per': per,
            'eps': eps,
            'error': None,
            'eps_source': 'Manual'
        }
        
    except Exception as e:
        return {
            'per': None,
            'eps': None,
            'error': f'PER計算中にエラーが発生しました: {e}'
        }


def display_per_analysis(stock_price, financial_metrics, per_result):
    """
    PER分析結果を表示する
    
    Args:
        stock_price (float): 最新の株価
        financial_metrics (dict): 財務指標の辞書
        per_result (dict): PER計算結果
    """
    print(f"\n銘柄コード: {financial_metrics['code']}")
    print(f"開示日: {financial_metrics['disclosed_date']}")
    print(f"会計年度: {financial_metrics['fiscal_year']}")
    print(f"会計期間: {financial_metrics['fiscal_period']}")
    print(f"文書タイプ: {financial_metrics['doc_type']}")
    print("=" * 80)
    
    # 基本情報
    print("【基本情報】")
    print(f"最新株価: {stock_price:,.0f} 円")
    
    if financial_metrics['profit'] is not None:
        try:
            profit_value = float(financial_metrics['profit']) if isinstance(financial_metrics['profit'], (int, float, str)) else 0
            print(f"当期純利益: {profit_value:,.0f} 円")
        except (ValueError, TypeError):
            print(f"当期純利益: {financial_metrics['profit']} 円")
    else:
        print("当期純利益: データなし")
    
    if financial_metrics['issued_shares'] is not None:
        try:
            shares_value = float(financial_metrics['issued_shares']) if isinstance(financial_metrics['issued_shares'], (int, float, str)) else 0
            print(f"発行済み株式数: {shares_value:,.0f} 株")
        except (ValueError, TypeError):
            print(f"発行済み株式数: {financial_metrics['issued_shares']} 株")
    else:
        print("発行済み株式数: データなし")
    
    # PER分析結果
    print("\n【PER分析】")
    if per_result['error']:
        print(f"エラー: {per_result['error']}")
    else:
        eps_source = per_result.get('eps_source', 'Unknown')
        print(f"1株当たり純利益(EPS): {per_result['eps']:.2f} 円 ({eps_source})")
        print(f"株価収益率(PER): {per_result['per']:.2f} 倍")
        
        # PERの評価
        print("\n【PER評価】")
        if per_result['per'] < 10:
            print("PER < 10: 割安（ただし、業績悪化の可能性も考慮）")
        elif per_result['per'] < 15:
            print("10 ≤ PER < 15: 適正水準")
        elif per_result['per'] < 20:
            print("15 ≤ PER < 20: やや割高")
        elif per_result['per'] < 30:
            print("20 ≤ PER < 30: 割高")
        else:
            print("PER ≥ 30: 大幅に割高（成長期待が高い可能性）")
        
        # 参考情報
        print("\n【参考情報】")
        print("- PERは株価を1株当たり純利益で割った値です")
        print("- 一般的にPERが低いほど割安、高いほど割高とされます")
        print("- 業界平均や過去のPERと比較することが重要です")
        print("- 成長性の高い企業はPERが高くなる傾向があります")


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
    
    # 英数字のみかチェック
    if not code.isalnum():
        return False
    
    # 4桁または5桁かチェック
    if len(code) not in [4, 5]:
        return False
        
    return True


def main():
    """メイン処理"""
    try:
        # 引数の確認
        if len(sys.argv) != 2:
            print("使用方法: python test_03_stock_per.py <銘柄コード>")
            print("例: python test_03_stock_per.py 7203")
            print("例: python test_03_stock_per.py 6758")
            print("例: python test_03_stock_per.py AAPL")
            sys.exit(1)
        
        stock_code = sys.argv[1]
        
        # 銘柄コードの検証
        if not validate_stock_code(stock_code):
            print(f"エラー: 無効な銘柄コードです: {stock_code}")
            print("銘柄コードは4桁または5桁の英数字で入力してください。")
            sys.exit(1)
        
        # スクリプトのディレクトリを取得
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        token_file_path = project_root / "token.txt"
        
        print("J-Quants API PER分析スクリプト")
        print("=" * 50)
        
        # APIキーの読み込み
        print(f"APIキーを読み込み中: {token_file_path}")
        refresh_token = load_api_key(token_file_path)
        print("APIキーの読み込み完了")
        
        # クライアントの初期化
        client = jquantsapi.Client(refresh_token=refresh_token)
        
        # 株価データの取得
        stock_price = get_stock_price(client, stock_code)
        
        # 財務データの取得
        financial_metrics = get_financial_statements(client, stock_code)
        
        if not financial_metrics:
            print("エラー: 財務データが取得できませんでした")
            sys.exit(1)
        
        # PERの計算
        per_result = calculate_per(stock_price, financial_metrics)
        
        # 結果の表示
        display_per_analysis(stock_price, financial_metrics, per_result)
        
        print("\n処理が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
