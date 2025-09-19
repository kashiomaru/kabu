#!/usr/bin/env python3
"""
J-Quants APIを使用して指定された銘柄コードの時価総額を計算・表示するスクリプト

使用方法:
    python test_04_stock_total.py <銘柄コード>
    
例:
    python test_04_stock_total.py 7203  # トヨタ自動車
    python test_04_stock_total.py 6758  # ソニーグループ

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


def is_valid_financial_data(latest_data):
    """
    財務データが有効かどうかを判定する（時価総額計算に必要な発行済み株式数があるかチェック）
    
    Args:
        latest_data: 財務データの1行
        
    Returns:
        bool: 有効な財務データかどうか
    """
    # 発行済み株式数が存在するかチェック（時価総額計算に必須）
    issued_shares = latest_data.get('NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock')
    has_shares_data = issued_shares is not None and issued_shares != ''
    
    return has_shares_data


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
            if col in ['DisclosedDate', 'TypeOfDocument', 'NetSales', 'Profit', 'NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock']:
                print(f"  {col}: {value}")
        return None
    
    # 発行済み株式数の候補列を検索
    issued_shares = None
    possible_columns = [
        'NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock',
        'NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYear',
        'NumberOfIssuedShares',
        'OutstandingShares',
        'IssuedShares'
    ]
    
    for col in possible_columns:
        if col in valid_data and valid_data[col] is not None and valid_data[col] != '':
            issued_shares = valid_data[col]
            print(f"発行済み株式数を発見: {col} = {issued_shares}")
            break
    
    # 財務指標の抽出
    metrics = {
        'code': code,
        'disclosed_date': valid_data.get('DisclosedDate', 'N/A'),
        'fiscal_year': valid_data.get('FiscalYear', 'N/A'),
        'fiscal_period': valid_data.get('FiscalPeriod', 'N/A'),
        'issued_shares': issued_shares,  # 発行済み株式数
        'company_name': valid_data.get('LocalCode', code),  # 会社名（利用可能な場合）
    }
    
    return metrics


def calculate_market_cap(stock_price, issued_shares):
    """
    時価総額を計算する
    
    Args:
        stock_price (float): 最新の株価
        issued_shares (float): 発行済み株式数
        
    Returns:
        dict: 時価総額計算結果
    """
    try:
        if issued_shares is None or issued_shares <= 0:
            return {
                'market_cap': None,
                'error': '発行済み株式数が取得できませんでした'
            }
        
        # 時価総額を計算（株価 × 発行済み株式数）
        market_cap = stock_price * issued_shares
        
        return {
            'market_cap': market_cap,
            'error': None
        }
        
    except Exception as e:
        return {
            'market_cap': None,
            'error': f'時価総額計算中にエラーが発生しました: {e}'
        }


def evaluate_market_cap(market_cap):
    """
    時価総額を評価する
    
    Args:
        market_cap (float): 時価総額（円）
        
    Returns:
        dict: 評価結果
    """
    if market_cap is None:
        return {
            'category': '不明',
            'description': 'データ不足のため評価できません'
        }
    
    # 時価総額の分類（一般的な基準）
    if market_cap >= 1_000_000_000_000:  # 1兆円以上
        return {
            'category': '大型株',
            'description': '1兆円以上の大型企業'
        }
    elif market_cap >= 100_000_000_000:  # 1000億円以上
        return {
            'category': '中大型株',
            'description': '1000億円以上の企業'
        }
    elif market_cap >= 10_000_000_000:  # 100億円以上
        return {
            'category': '中型株',
            'description': '100億円以上の企業'
        }
    elif market_cap >= 1_000_000_000:  # 10億円以上
        return {
            'category': '小型株',
            'description': '10億円以上の企業'
        }
    else:
        return {
            'category': 'マイクロ株',
            'description': '10億円未満の企業'
        }


def display_market_cap_analysis(stock_price, financial_metrics, market_cap_result):
    """
    時価総額分析結果を表示する
    
    Args:
        stock_price (float): 最新の株価
        financial_metrics (dict): 財務指標の辞書
        market_cap_result (dict): 時価総額計算結果
    """
    print(f"\n銘柄コード: {financial_metrics['code']}")
    print(f"開示日: {financial_metrics['disclosed_date']}")
    print(f"会計年度: {financial_metrics['fiscal_year']}")
    print(f"会計期間: {financial_metrics['fiscal_period']}")
    print("=" * 80)
    
    # 基本情報
    print("【基本情報】")
    print(f"最新株価: {stock_price:,.0f} 円")
    
    if financial_metrics['issued_shares'] is not None:
        try:
            shares_value = float(financial_metrics['issued_shares']) if isinstance(financial_metrics['issued_shares'], (int, float, str)) else 0
            print(f"発行済み株式数: {shares_value:,.0f} 株")
        except (ValueError, TypeError):
            print(f"発行済み株式数: {financial_metrics['issued_shares']} 株")
    else:
        print("発行済み株式数: データなし")
    
    # 時価総額分析結果
    print("\n【時価総額分析】")
    if market_cap_result['error']:
        print(f"エラー: {market_cap_result['error']}")
    else:
        market_cap = market_cap_result['market_cap']
        print(f"時価総額: {market_cap:,.0f} 円")
        
        # 時価総額の評価
        evaluation = evaluate_market_cap(market_cap)
        print(f"企業規模: {evaluation['category']} ({evaluation['description']})")
        
        # 単位表示（億円で統一）
        print(f"時価総額: {market_cap / 100_000_000:.2f} 億円")
        
        # 参考情報
        print("\n【参考情報】")
        print("- 時価総額 = 株価 × 発行済み株式数")
        print("- 時価総額は企業の市場での評価額を表します")
        print("- 投資判断の重要な指標の一つです")
        print("- 業界や成長段階によって適正な時価総額は異なります")


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
            print("使用方法: python test_04_stock_total.py <銘柄コード>")
            print("例: python test_04_stock_total.py 7203")
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
        
        print("J-Quants API 時価総額分析スクリプト")
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
        
        # 時価総額の計算
        issued_shares = financial_metrics.get('issued_shares')
        if issued_shares is not None:
            try:
                issued_shares = float(issued_shares) if isinstance(issued_shares, (int, float, str)) else 0
            except (ValueError, TypeError):
                issued_shares = None
        
        market_cap_result = calculate_market_cap(stock_price, issued_shares)
        
        # 結果の表示
        display_market_cap_analysis(stock_price, financial_metrics, market_cap_result)
        
        print("\n処理が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
