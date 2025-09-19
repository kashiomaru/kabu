#!/usr/bin/env python3
"""
J-Quants APIを使用して指定された銘柄コードの財務データを取得し、
databaseフォルダに生のAPIレスポンスを保存するスクリプト

使用方法:
    python test_12_stock_database.py <銘柄コード>
    
例:
    python test_12_stock_database.py 7203  # トヨタ自動車
    python test_12_stock_database.py 6758  # ソニーグループ

前提条件:
    - token.txtファイルにAPIキー（リフレッシュトークン）が記述されていること
    - jquants-api-clientライブラリがインストールされていること
"""

import sys
from pathlib import Path
from stock_database import StockFinancialDatabase


def main():
    """メイン処理"""
    try:
        # 引数の確認
        if len(sys.argv) != 2:
            print("使用方法: python test_12_stock_database.py <銘柄コード>")
            print("例: python test_12_stock_database.py 7203")
            print("例: python test_12_stock_database.py 228A")
            sys.exit(1)
        
        stock_code = sys.argv[1]
        
        # 銘柄コードの検証
        if not StockFinancialDatabase.validate_stock_code(stock_code):
            print(f"エラー: 無効な銘柄コードです: {stock_code}")
            print("銘柄コードは4文字の英数字で入力してください。")
            sys.exit(1)
        
        # スクリプトのディレクトリを取得
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        token_file_path = project_root / "token.txt"
        database_dir = project_root / "database"
        
        print("J-Quants API 財務データベース保存スクリプト")
        print("=" * 60)
        
        # データベース管理クラスの初期化
        db = StockFinancialDatabase(database_dir=database_dir, token_file_path=token_file_path)
        
        # データの取得または更新
        data = db.get_or_update_stock_data(stock_code)
        
        # データの要約を表示
        db.display_data_summary(data, stock_code)
        
        print("\n処理が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
