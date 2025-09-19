#!/usr/bin/env python3
"""
J-Quants APIを使用して決算発表予定日の銘柄一覧を取得・表示するスクリプト

使用方法:
    python test_15_announcement.py                    # 全ての決算発表予定日を取得
    python test_15_announcement.py <銘柄コード>        # 特定銘柄の決算発表予定日を取得（4桁または5桁）

前提条件:
    - token.txtファイルにAPIキー（リフレッシュトークン）が記述されていること
    - jquants-api-clientライブラリがインストールされていること

特徴:
    - 翌営業日の決算発表予定銘柄を取得
    - 3月期・9月期決算会社が対象
    - 銘柄コード、会社名、決算期、四半期、市場区分を表示
    - 特定の銘柄コードを指定してその銘柄のみを取得可能
"""

import os
import sys
import argparse
from pathlib import Path
import jquantsapi
import pandas as pd
from datetime import datetime


def validate_stock_code(code):
    """
    銘柄コードの形式を検証する
    
    Args:
        code (str): 検証する銘柄コード
        
    Returns:
        bool: 有効な銘柄コードの場合True、そうでなければFalse
    """
    if not code:
        return False
    
    # 4文字または5文字の英数字であることを確認
    if len(code) not in [4, 5]:
        return False
    
    # 英数字のみであることを確認
    if not code.isalnum():
        return False
    
    return True


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


def get_announcement_data(client, code=None):
    """
    J-Quants APIから決算発表予定日データを取得する
    
    Args:
        client: J-Quants APIクライアント
        code (str, optional): 銘柄コード（指定された場合はその銘柄のみフィルタリング）
        
    Returns:
        pandas.DataFrame: 決算発表予定日データのデータフレーム
        
    Raises:
        Exception: API接続エラー
    """
    try:
        print("決算発表予定日データを取得中...")
        
        # 全ての決算発表予定日データを取得
        announcement_data = client.get_fins_announcement()
        
        # データフレームに変換
        df = pd.DataFrame(announcement_data)
        
        if df.empty:
            print("決算発表予定日データが見つかりませんでした")
            return df
        
        print(f"決算発表予定日データを取得しました: {len(df)} 件")
        
        # 銘柄コードが指定されている場合はフィルタリング
        if code:
            print(f"銘柄コード {code} でフィルタリング中...")
            original_count = len(df)
            df = df[df['Code'] == code]
            
            if df.empty:
                print(f"銘柄コード {code} の決算発表予定日データが見つかりませんでした")
            else:
                print(f"銘柄コード {code} の決算発表予定日データを取得しました: {len(df)} 件")
        
        return df
        
    except Exception as e:
        if code:
            raise Exception(f"銘柄コード {code} の決算発表予定日データの取得中にエラーが発生しました: {e}")
        else:
            raise Exception(f"決算発表予定日データの取得中にエラーが発生しました: {e}")


def display_announcement_data(df, code=None):
    """
    決算発表予定日データを表示する
    
    Args:
        df (pandas.DataFrame): 決算発表予定日データのデータフレーム
        code (str, optional): 銘柄コード（指定された場合はその銘柄のみ表示）
    """
    if df.empty:
        if code:
            print(f"銘柄コード {code} の決算発表予定日データがありません。")
        else:
            print("表示するデータがありません。")
        return
    
    print("\n" + "=" * 100)
    if code:
        print(f"銘柄コード {code} の決算発表予定日一覧")
    else:
        print("決算発表予定日一覧")
    print("=" * 100)
    
    # データを日付順でソート
    if 'Date' in df.columns:
        df_sorted = df.sort_values('Date').reset_index(drop=True)
    else:
        df_sorted = df
    
    # 各レコードを表示
    for i, (_, row) in enumerate(df_sorted.iterrows(), 1):
        print(f"\n[{i:2d}] 銘柄コード: {row.get('Code', 'N/A')}")
        print(f"     会社名: {row.get('CompanyName', 'N/A')}")
        print(f"     発表予定日: {row.get('Date', 'N/A')}")
        print(f"     決算期末: {row.get('FiscalYear', 'N/A')}")
        print(f"     四半期: {row.get('FiscalQuarter', 'N/A')}")
        print(f"     業種: {row.get('SectorName', 'N/A')}")
        print(f"     市場区分: {row.get('Section', 'N/A')}")
        print("-" * 80)
    
    # サマリー情報を表示
    print(f"\n【サマリー】")
    print(f"総件数: {len(df)} 件")
    
    # 市場区分別の件数
    if 'Section' in df.columns:
        section_counts = df['Section'].value_counts()
        print(f"\n市場区分別件数:")
        for section, count in section_counts.items():
            print(f"  {section}: {count} 件")
    
    # 決算期別の件数
    if 'FiscalYear' in df.columns:
        fiscal_counts = df['FiscalYear'].value_counts()
        print(f"\n決算期別件数:")
        for fiscal, count in fiscal_counts.items():
            print(f"  {fiscal}: {count} 件")
    
    # 四半期別の件数
    if 'FiscalQuarter' in df.columns:
        quarter_counts = df['FiscalQuarter'].value_counts()
        print(f"\n四半期別件数:")
        for quarter, count in quarter_counts.items():
            print(f"  {quarter}: {count} 件")


def main():
    """メイン処理"""
    try:
        # 引数の解析
        parser = argparse.ArgumentParser(
            description="J-Quants APIを使用して決算発表予定日の銘柄一覧を取得・表示するスクリプト",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
使用例:
  python test_15_announcement.py                    # 全ての決算発表予定日を取得
  python test_15_announcement.py 7203              # トヨタ自動車の決算発表予定日を取得
  python test_15_announcement.py 6758              # ソニーグループの決算発表予定日を取得
  python test_15_announcement.py 96510             # 5桁銘柄コードの決算発表予定日を取得
            """
        )
        parser.add_argument(
            'code', 
            nargs='?', 
            help='銘柄コード（4文字または5文字の英数字、例: 7203, 96510）'
        )
        
        args = parser.parse_args()
        
        # 銘柄コードの検証
        if args.code and not validate_stock_code(args.code):
            print(f"エラー: 無効な銘柄コードです: {args.code}")
            print("銘柄コードは4文字または5文字の英数字で入力してください。")
            print("例: 7203, 6758, 228A, 96510")
            sys.exit(1)
        
        # スクリプトのディレクトリを取得
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        token_file_path = project_root / "token.txt"
        
        print("J-Quants API 決算発表予定日取得スクリプト")
        print("=" * 60)
        
        if args.code:
            print(f"対象銘柄コード: {args.code}")
        else:
            print("対象: 全銘柄")
        
        # APIキーの読み込み
        print(f"APIキーを読み込み中: {token_file_path}")
        refresh_token = load_api_key(token_file_path)
        print("APIキーの読み込み完了")
        
        # クライアントの初期化
        client = jquantsapi.Client(refresh_token=refresh_token)
        
        # 決算発表予定日データの取得
        df = get_announcement_data(client, args.code)
        
        if df.empty:
            if args.code:
                print(f"銘柄コード {args.code} の決算発表予定日データが取得できませんでした")
                print("（該当銘柄に決算発表予定がない可能性があります）")
            else:
                print("決算発表予定日データが取得できませんでした")
                print("（翌営業日に決算発表予定の銘柄がない可能性があります）")
            return
        
        # データの表示
        display_announcement_data(df, args.code)
        
        print(f"\n処理が完了しました。")
        print(f"取得日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
