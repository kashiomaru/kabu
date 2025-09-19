#!/usr/bin/env python3
"""
市場別銘柄の財務データを一括取得するスクリプト

使用方法:
    python test_14_stock_database.py [オプション]
    
例:
    # 基本実行（プライム、スタンダード、グロース）
    python test_14_stock_database.py
    
    # 特定市場のみ
    python test_14_stock_database.py --markets プライム
    
    # 強制更新
    python test_14_stock_database.py --force-update
    
    # カスタム設定
    python test_14_stock_database.py --delay 1.0 --max-errors 20

前提条件:
    - token.txtファイルにAPIキー（リフレッシュトークン）が記述されていること
    - jquants-api-clientライブラリがインストールされていること

特徴:
    - 市場別銘柄の一括取得
    - 既存データのスキップ機能
    - エラーハンドリングと進捗表示
    - 処理結果の統計情報表示
"""

import sys
import argparse
from pathlib import Path
from stock_database import StockFinancialDatabase


def parse_arguments():
    """コマンドライン引数を解析する"""
    parser = argparse.ArgumentParser(
        description='市場別銘柄の財務データを一括取得するスクリプト',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  python test_14_stock_database.py
  python test_14_stock_database.py --markets プライム
  python test_14_stock_database.py --force-update
  python test_14_stock_database.py --delay 1.0 --max-errors 20
        """
    )
    
    parser.add_argument(
        '--markets',
        nargs='+',
        default=['プライム', 'スタンダード', 'グロース'],
        help='対象市場を指定（デフォルト: プライム スタンダード グロース）'
    )
    
    parser.add_argument(
        '--delay',
        type=float,
        default=0.5,
        help='API呼び出し間隔（秒）（デフォルト: 0.5）'
    )
    
    parser.add_argument(
        '--max-errors',
        type=int,
        default=10,
        help='最大エラー許容数（デフォルト: 10）'
    )
    
    parser.add_argument(
        '--force-update',
        action='store_true',
        help='既存データを強制更新する'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='実際の処理は行わず、対象銘柄数のみ表示する'
    )
    
    return parser.parse_args()


def main():
    """メイン処理"""
    try:
        # 引数の解析
        args = parse_arguments()
        
        # スクリプトのディレクトリを取得
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        token_file_path = project_root / "token.txt"
        database_dir = project_root / "database"
        
        print("市場別銘柄財務データ一括取得スクリプト")
        print("=" * 60)
        print(f"対象市場: {', '.join(args.markets)}")
        print(f"API呼び出し間隔: {args.delay}秒")
        print(f"最大エラー許容数: {args.max_errors}")
        print(f"強制更新: {'有効' if args.force_update else '無効'}")
        print(f"ドライラン: {'有効' if args.dry_run else '無効'}")
        print("=" * 60)
        
        # データベース管理クラスの初期化
        db = StockFinancialDatabase(database_dir=database_dir, token_file_path=token_file_path)
        
        # ドライランモードの場合は銘柄数のみ表示
        if args.dry_run:
            print("ドライランモード: 対象銘柄数を確認中...")
            stock_codes = db.get_market_stock_list(args.markets)
            if stock_codes:
                print(f"対象銘柄数: {len(stock_codes)}")
                print("実際の処理を実行するには --dry-run オプションを外してください。")
            else:
                print("エラー: 銘柄リストが取得できませんでした")
                sys.exit(1)
            return
        
        # 一括取得の実行
        stats = db.batch_get_market_stocks_data(
            markets=args.markets,
            delay_seconds=args.delay,
            max_errors=args.max_errors,
            force_update=args.force_update
        )
        
        if stats is None:
            print("エラー: 一括取得処理が失敗しました")
            sys.exit(1)
        
        # 最終結果の表示
        print("\n" + "=" * 60)
        print("【最終結果】")
        print(f"処理完了: {stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"対象市場: {', '.join(stats['markets'])}")
        print(f"総銘柄数: {stats['total_stocks']}")
        print(f"成功: {stats['success_count']}")
        print(f"スキップ: {stats['skipped_count']}")
        print(f"エラー: {stats['error_count']}")
        print(f"処理時間: {stats['end_time'] - stats['start_time']}")
        
        # エラーがある場合は詳細を表示
        if stats['errors']:
            print(f"\n【エラー詳細】")
            for i, error in enumerate(stats['errors'][:10], 1):  # 最初の10件を表示
                print(f"  {i}. {error}")
            if len(stats['errors']) > 10:
                print(f"  ... 他 {len(stats['errors']) - 10} 件のエラー")
        
        print("\n処理が完了しました。")
        
    except KeyboardInterrupt:
        print("\n\n処理が中断されました。")
        sys.exit(1)
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
