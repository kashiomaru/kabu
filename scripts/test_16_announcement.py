#!/usr/bin/env python3
"""
J-Quants APIを使用して指定された銘柄コードの財務データから次の報告日を予想するスクリプト

使用方法:
    python test_16_announcement.py <銘柄コード>
    
例:
    python test_16_announcement.py 7203  # トヨタ自動車
    python test_16_announcement.py 6758  # ソニーグループ
    python test_16_announcement.py 228A  # アルファベット付き銘柄コード

前提条件:
    - token.txtファイルにAPIキー（リフレッシュトークン）が記述されていること
    - jquants-api-clientライブラリがインストールされていること
    - stock_database.pyが同じディレクトリに存在すること

特徴:
    - stock_database.pyのStockFinancialDatabaseクラスを使用
    - 最新の四半期報告の開示日を特定
    - 次の報告日を3ヶ月後として予想
    - 前回の開示日と予想報告日を出力
"""

import sys
from pathlib import Path
from stock_database import StockFinancialDatabase
from datetime import datetime, timedelta
import pandas as pd


def find_latest_quarterly_report(data):
    """
    最新の四半期報告を特定する
    
    Args:
        data (dict): 財務データ（stock_databaseから取得）
        
    Returns:
        dict: 最新の四半期報告データ（見つからない場合はNone）
    """
    if not data or 'raw_data' not in data:
        return None
    
    raw_data = data['raw_data']
    if not raw_data:
        return None
    
    # データフレームに変換
    df = pd.DataFrame(raw_data)
    
    if df.empty:
        return None
    
    # 財務諸表のデータのみをフィルタリング
    financial_statements_data = df[
        df['TypeOfDocument'].str.contains('FinancialStatements', na=False)
    ].copy()
    
    if financial_statements_data.empty:
        return None
    
    # DisclosedDateでソート（最新が最初に来るように降順）
    if 'DisclosedDate' in financial_statements_data.columns:
        financial_statements_data['DisclosedDate'] = pd.to_datetime(financial_statements_data['DisclosedDate'])
        financial_statements_data = financial_statements_data.sort_values('DisclosedDate', ascending=False).reset_index(drop=True)
    
    # 四半期報告を特定（1Q, 2Q, 3Q, FY）
    quarterly_reports = financial_statements_data[
        financial_statements_data['TypeOfCurrentPeriod'].isin(['1Q', '2Q', '3Q', 'FY'])
    ].copy()
    
    if quarterly_reports.empty:
        return None
    
    # 最新の四半期報告を取得
    latest_report = quarterly_reports.iloc[0]
    
    return {
        'disclosed_date': latest_report.get('DisclosedDate'),
        'period_type': latest_report.get('TypeOfCurrentPeriod'),
        'fiscal_year': latest_report.get('FiscalYear'),
        'type_of_document': latest_report.get('TypeOfDocument')
    }


def predict_next_report_date(latest_report):
    """
    最新の四半期報告から次の報告日を予想する（3ヶ月後）
    
    Args:
        latest_report (dict): 最新の四半期報告データ
        
    Returns:
        str: 予想報告日（YYYY-MM-DD形式）
    """
    if not latest_report or 'disclosed_date' not in latest_report:
        return None
    
    disclosed_date = latest_report['disclosed_date']
    period_type = latest_report.get('period_type', '')
    
    # 開示日をdatetimeオブジェクトに変換
    if isinstance(disclosed_date, str):
        try:
            disclosed_date = pd.to_datetime(disclosed_date)
        except:
            return None
    elif not isinstance(disclosed_date, datetime):
        return None
    
    # 3ヶ月後の日付を計算
    next_report_date = disclosed_date + timedelta(days=90)  # 約3ヶ月後
    
    # 次の四半期の期間タイプを予想
    if period_type == '1Q':
        next_period_type = '2Q'
    elif period_type == '2Q':
        next_period_type = '3Q'
    elif period_type == '3Q':
        next_period_type = 'FY'
    elif period_type == 'FY':
        next_period_type = '1Q'  # 翌年度の1Q
    else:
        next_period_type = 'N/A'
    
    return {
        'predicted_date': next_report_date.strftime('%Y-%m-%d'),
        'predicted_period_type': next_period_type,
        'days_from_last': (next_report_date - disclosed_date).days
    }


def display_prediction_result(code, latest_report, prediction):
    """
    予想結果を表示する
    
    Args:
        code (str): 銘柄コード
        latest_report (dict): 最新の四半期報告データ
        prediction (dict): 予想結果
    """
    print(f"\n銘柄コード: {code}")
    print("=" * 60)
    print("【次の報告日予想】")
    print("-" * 40)
    
    if latest_report:
        print(f"前回の開示日: {latest_report['disclosed_date']}")
        print(f"前回の期間タイプ: {latest_report['period_type']}")
        print(f"前回の会計年度: {latest_report['fiscal_year']}")
        print(f"前回の文書タイプ: {latest_report['type_of_document']}")
    else:
        print("前回の四半期報告データが見つかりませんでした")
        return
    
    if prediction:
        print(f"\n予想報告日: {prediction['predicted_date']}")
        print(f"予想期間タイプ: {prediction['predicted_period_type']}")
        print(f"前回開示日からの日数: {prediction['days_from_last']} 日")
    else:
        print("\n予想報告日の計算に失敗しました")


def main():
    """メイン処理"""
    try:
        # 引数の確認
        if len(sys.argv) != 2:
            print("使用方法: python test_16_announcement.py <銘柄コード>")
            print("例: python test_16_announcement.py 7203")
            print("例: python test_16_announcement.py 228A")
            sys.exit(1)
        
        stock_code = sys.argv[1]
        
        # 銘柄コードの検証
        if not StockFinancialDatabase.validate_stock_code(stock_code):
            print(f"エラー: 無効な銘柄コードです: {stock_code}")
            print("銘柄コードは4桁または5桁の英数字で入力してください。")
            sys.exit(1)
        
        # スクリプトのディレクトリを取得
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        token_file_path = project_root / "token.txt"
        database_dir = project_root / "database"
        
        print("J-Quants API 次の報告日予想スクリプト")
        print("=" * 60)
        print(f"対象銘柄コード: {stock_code}")
        
        # データベース管理クラスの初期化
        db = StockFinancialDatabase(database_dir=database_dir, token_file_path=token_file_path)
        
        # 銘柄コードの正規化表示
        normalized_code = db._normalize_stock_code(stock_code)
        print(f"正規化後コード: {normalized_code}")
        print("-" * 60)
        
        # 財務データの取得
        print("財務データを取得中...")
        data = db.get_or_update_stock_data(stock_code)
        
        if not data:
            print("エラー: 財務データが取得できませんでした")
            sys.exit(1)
        
        # 最新の四半期報告を特定
        print("最新の四半期報告を特定中...")
        latest_report = find_latest_quarterly_report(data)
        
        if not latest_report:
            print("エラー: 四半期報告データが見つかりませんでした")
            sys.exit(1)
        
        # 次の報告日を予想
        print("次の報告日を予想中...")
        prediction = predict_next_report_date(latest_report)
        
        # 結果の表示
        display_prediction_result(stock_code, latest_report, prediction)
        
        print(f"\n処理が完了しました。")
        print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
