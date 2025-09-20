#!/usr/bin/env python3
"""
株式財務データベース管理ライブラリ

J-Quants APIを使用して財務データを取得・保存・管理するための共通ライブラリ
"""

import os
import json
import glob
import re
from pathlib import Path
import jquantsapi
import pandas as pd
from datetime import datetime, timedelta


class StockFinancialDatabase:
    """
    株式財務データベース管理クラス
    
    銘柄ごとの財務データを取得・保存・管理する機能を提供
    """
    
    def __init__(self, database_dir="database", token_file_path="token.txt"):
        """
        初期化
        
        Args:
            database_dir (str): データベースディレクトリのパス
            token_file_path (str): APIキーファイルのパス
        """
        self.database_dir = Path(database_dir)
        self.token_file_path = Path(token_file_path)
        self.client = None
        self._create_database_directory()
        
        # 初期化時にデータベース更新処理を実行
        self._check_and_update_database()
    
    def _create_database_directory(self):
        """データベースディレクトリを作成する"""
        try:
            self.database_dir.mkdir(exist_ok=True)
        except Exception as e:
            raise Exception(f"データベースディレクトリの作成に失敗しました: {e}")
    
    def _load_api_key(self):
        """
        APIキーをファイルから読み込む
        
        Returns:
            str: APIキー（リフレッシュトークン）
        """
        try:
            with open(self.token_file_path, 'r', encoding='utf-8') as file:
                token = file.read().strip()
                
            if not token:
                raise ValueError("APIキーファイルが空です")
                
            return token
            
        except FileNotFoundError:
            raise FileNotFoundError(f"APIキーファイルが見つかりません: {self.token_file_path}")
        except Exception as e:
            raise Exception(f"APIキーの読み込み中にエラーが発生しました: {e}")
    
    def _get_client(self):
        """J-Quants APIクライアントを取得する（遅延初期化）"""
        if self.client is None:
            refresh_token = self._load_api_key()
            self.client = jquantsapi.Client(refresh_token=refresh_token)
        return self.client
    
    def _normalize_stock_code(self, code):
        """
        銘柄コードを5桁に正規化する（後ろに0を追加）
        
        Args:
            code (str): 銘柄コード（4桁または5桁）
            
        Returns:
            str: 5桁の銘柄コード
        """
        if not code:
            return code
        
        # 4桁の場合は後ろに0を追加して5桁にする
        if len(code) == 4:
            return code + '0'
        
        return code
    
    def _file_exists(self, code):
        """
        指定された銘柄コードのファイルが存在するかチェックする
        
        Args:
            code (str): 銘柄コード（5桁に正規化済み）
            
        Returns:
            bool: ファイルが存在する場合はTrue
        """
        filename = f"{self.database_dir}/{code}.json"
        return os.path.exists(filename)
    
    
    def get_financial_statements(self, code):
        """
        J-Quants APIから指定された銘柄の財務データを取得する
        
        Args:
            code (str): 銘柄コード（4桁または5桁）
            
        Returns:
            pandas.DataFrame: 財務データのデータフレーム（取得できない場合は空のDataFrame）
        """
        try:
            # 銘柄コードを5桁に正規化
            normalized_code = self._normalize_stock_code(code)
            
            client = self._get_client()
            print(f"銘柄コード {normalized_code} の財務データを取得中...")
            
            # 正規化された銘柄コードで財務データを取得
            financial_data = client.get_fins_statements(code=normalized_code)
            
            if not financial_data.empty:
                print(f"財務データを取得しました: {len(financial_data)} 件")
                # DisclosedDateでソート（最新が最初に来るように降順）
                if 'DisclosedDate' in financial_data.columns:
                    financial_data['DisclosedDate'] = pd.to_datetime(financial_data['DisclosedDate'])
                    financial_data = financial_data.sort_values('DisclosedDate', ascending=False).reset_index(drop=True)
                return financial_data
            else:
                print(f"銘柄コード {normalized_code} の財務データが見つかりませんでした")
                return pd.DataFrame()
            
        except Exception as e:
            raise Exception(f"財務データの取得中にエラーが発生しました: {e}")
    
    def _convert_dataframe_to_json_serializable(self, data):
        """
        データフレームまたは辞書をJSONシリアライズ可能な形式に変換する
        
        Args:
            data (pandas.DataFrame or dict): データフレームまたは辞書
            
        Returns:
            list: JSONシリアライズ可能な辞書のリスト
        """
        if isinstance(data, dict):
            # 辞書の場合はraw_dataを返す
            if 'raw_data' in data:
                return data['raw_data']
            else:
                return []
        
        # pandas.DataFrameの場合
        if data.empty:
            return []
        
        raw_data_list = []
        for _, row in data.iterrows():
            row_dict = {}
            for col, value in row.items():
                if pd.isna(value):
                    row_dict[col] = None
                elif isinstance(value, pd.Timestamp):
                    row_dict[col] = value.isoformat()
                elif isinstance(value, (pd.Int64Dtype, pd.Float64Dtype)):
                    row_dict[col] = value.item() if not pd.isna(value) else None
                else:
                    row_dict[col] = value
            raw_data_list.append(row_dict)
        
        return raw_data_list
    
    def save_stock_data(self, code, financial_data):
        """
        銘柄の財務データを保存する
        
        Args:
            code (str): 銘柄コード（4桁または5桁）
            financial_data (pandas.DataFrame): 財務データ
        """
        try:
            # 銘柄コードを5桁に正規化
            normalized_code = self._normalize_stock_code(code)
            filename = f"{self.database_dir}/{normalized_code}.json"
            
            # メタデータを追加
            if isinstance(financial_data, dict) and 'metadata' in financial_data:
                # 既存の辞書データ（メタデータ付き）の場合
                data_with_metadata = financial_data.copy()
                # メタデータを更新
                data_with_metadata['metadata'].update({
                    "code": normalized_code,
                    "original_code": code,
                    "retrieved_datetime": datetime.now().isoformat(),
                    "data_count": len(financial_data.get('raw_data', [])),
                    "api_source": "J-Quants API",
                    "file_updated": datetime.now().isoformat()
                })
            else:
                # 新規データまたはDataFrameの場合
                # データ型に応じてdata_countを計算
                if isinstance(financial_data, dict):
                    if 'raw_data' in financial_data:
                        data_count = len(financial_data['raw_data'])
                    else:
                        data_count = 0
                else:
                    # pandas.DataFrameの場合
                    data_count = len(financial_data) if not financial_data.empty else 0
                
                data_with_metadata = {
                    "metadata": {
                        "code": normalized_code,
                        "original_code": code,
                        "retrieved_datetime": datetime.now().isoformat(),
                        "data_count": data_count,
                        "api_source": "J-Quants API",
                        "file_created": datetime.now().isoformat()
                    },
                    "raw_data": self._convert_dataframe_to_json_serializable(financial_data)
                }
            
            # JSONファイルとして保存
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data_with_metadata, f, ensure_ascii=False, indent=2)
            
            print(f"データを保存しました: {filename}")
            
            # 保存されたファイルのサイズを確認
            file_size = os.path.getsize(filename)
            print(f"保存されたファイルサイズ: {file_size} バイト")
            
        except Exception as e:
            print(f"詳細エラー: {e}")
            print(f"エラータイプ: {type(e)}")
            import traceback
            traceback.print_exc()
            raise Exception(f"データの保存中にエラーが発生しました: {e}")
    
    def load_stock_data(self, code):
        """
        既存のデータを読み込む
        
        Args:
            code (str): 銘柄コード（4桁または5桁）
            
        Returns:
            dict: 読み込んだデータ、ファイルが存在しない場合はNone
        """
        try:
            # 銘柄コードを5桁に正規化
            normalized_code = self._normalize_stock_code(code)
            filename = f"{self.database_dir}/{normalized_code}.json"
            
            if not os.path.exists(filename):
                return None
            
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            print(f"既存データを読み込みました: {filename}")
            return data
            
        except Exception as e:
            print(f"既存データの読み込み中にエラーが発生しました: {e}")
            return None
    
    def get_or_update_stock_data(self, code):
        """
        銘柄のデータを取得または更新する（ローカルファイルがあれば使用、なければAPI取得）
        
        Args:
            code (str): 銘柄コード（4桁または5桁）
            
        Returns:
            dict: 銘柄データ
        """
        # 銘柄コードを5桁に正規化
        normalized_code = self._normalize_stock_code(code)
        
        # 既存データをチェック
        existing_data = self.load_stock_data(code)
        if existing_data is not None:
            print(f"銘柄コード {normalized_code} の既存データを使用します。")
            return existing_data
        
        # データを取得・保存
        print(f"銘柄コード {normalized_code} のデータをAPIから取得中...")
        financial_data = self.get_financial_statements(code)
        
        if financial_data.empty:
            raise Exception("財務データが取得できませんでした")
        
        self.save_stock_data(code, financial_data)
        return self.load_stock_data(code)
    
    def get_stock_list(self):
        """
        保存済み銘柄リストを取得する
        
        Returns:
            list: 銘柄コードのリスト（5桁）
        """
        pattern = f"{self.database_dir}/*.json"
        files = glob.glob(pattern)
        
        codes = set()
        for file in files:
            filename = os.path.basename(file)
            # 5桁の銘柄コードファイルを抽出
            match = re.match(r'^(\w{5})\.json$', filename)
            if match:
                codes.add(match.group(1))
        
        return sorted(list(codes))
    
    def get_latest_data_info(self, code):
        """
        銘柄の最新データ情報を取得する
        
        Args:
            code (str): 銘柄コード（4桁または5桁）
            
        Returns:
            dict: データ情報
        """
        data = self.load_stock_data(code)
        if not data:
            return None
        
        metadata = data.get('metadata', {})
        raw_data = data.get('raw_data', [])
        
        return {
            'code': metadata.get('code'),
            'original_code': metadata.get('original_code'),
            'retrieved_datetime': metadata.get('retrieved_datetime'),
            'data_count': metadata.get('data_count', 0),
            'api_source': metadata.get('api_source'),
            'latest_records': raw_data[:3] if raw_data else []
        }
    
    def display_data_summary(self, data, code):
        """
        データの要約を表示する
        
        Args:
            data (dict): 読み込んだデータ
            code (str): 銘柄コード（4桁または5桁）
        """
        if not data:
            print(f"銘柄コード {code} のデータがありません。")
            return
        
        metadata = data.get('metadata', {})
        raw_data = data.get('raw_data', [])
        
        print(f"\n【データ要約】")
        print(f"銘柄コード: {metadata.get('code', 'N/A')}")
        print(f"元のコード: {metadata.get('original_code', 'N/A')}")
        print(f"取得日時: {metadata.get('retrieved_datetime', 'N/A')}")
        print(f"データ件数: {metadata.get('data_count', 0)} 件")
        print(f"APIソース: {metadata.get('api_source', 'N/A')}")
        
        if raw_data:
            print(f"\n【最新財務データ（最初の3件）】")
            for i, record in enumerate(raw_data[:3]):
                print(f"  データ {i+1}:")
                print(f"    開示日: {record.get('DisclosedDate', 'N/A')}")
                print(f"    文書タイプ: {record.get('TypeOfDocument', 'N/A')}")
                print(f"    期間タイプ: {record.get('TypeOfCurrentPeriod', 'N/A')}")
                print(f"    会計年度: {record.get('FiscalYear', 'N/A')}")
    
    def get_market_stock_list(self, markets=['プライム', 'スタンダード', 'グロース']):
        """
        指定された市場の銘柄リストを取得する
        
        Args:
            markets (list): 対象市場のリスト
            
        Returns:
            list: 銘柄コードのリスト
        """
        try:
            client = self._get_client()
            print("銘柄一覧を取得中...")
            
            # 銘柄一覧の取得
            stock_list = client.get_listed_info()
            df = pd.DataFrame(stock_list)
            
            if 'MarketCodeName' not in df.columns:
                print("警告: MarketCodeName列が見つかりません。全銘柄を対象とします。")
                return df['Code'].tolist()
            
            # 対象市場の銘柄をフィルタリング
            market_filter = df['MarketCodeName'].str.contains('|'.join(markets), na=False)
            filtered_df = df[market_filter].copy()
            
            print(f"市場フィルタリング結果:")
            print(f"  全銘柄数: {len(df)}")
            print(f"  対象市場: {markets}")
            print(f"  フィルタ後銘柄数: {len(filtered_df)}")
            
            # 市場別の銘柄数を表示
            if 'MarketCodeName' in filtered_df.columns:
                market_counts = filtered_df['MarketCodeName'].value_counts()
                print(f"  市場別銘柄数:")
                for market, count in market_counts.items():
                    print(f"    {market}: {count} 銘柄")
            
            return filtered_df['Code'].tolist()
            
        except Exception as e:
            raise Exception(f"銘柄リストの取得中にエラーが発生しました: {e}")
    
    def batch_get_market_stocks_data(self, markets=['プライム', 'スタンダード', 'グロース'], 
                                    delay_seconds=0.5, max_errors=10, force_update=False):
        """
        指定市場の全銘柄の財務データを一括取得する
        
        Args:
            markets (list): 対象市場のリスト
            delay_seconds (float): API呼び出し間隔（秒）
            max_errors (int): 最大エラー許容数
            force_update (bool): 既存データを強制更新するか
            
        Returns:
            dict: 処理結果の統計情報
        """
        import time
        
        print("=== 市場別銘柄財務データ一括取得開始 ===")
        print(f"対象市場: {', '.join(markets)}")
        print(f"強制更新: {'有効' if force_update else '無効'}")
        print(f"API呼び出し間隔: {delay_seconds}秒")
        print(f"最大エラー許容数: {max_errors}")
        print("=" * 60)
        
        # 銘柄リストを取得
        stock_codes = self.get_market_stock_list(markets)
        
        if not stock_codes:
            print("エラー: 銘柄リストが取得できませんでした")
            return None
        
        # 統計情報
        stats = {
            'total_stocks': len(stock_codes),
            'success_count': 0,
            'error_count': 0,
            'skipped_count': 0,
            'errors': [],
            'start_time': datetime.now(),
            'end_time': None,
            'markets': markets
        }
        
        print(f"処理対象銘柄数: {len(stock_codes)}")
        print("-" * 60)
        
        for i, code in enumerate(stock_codes, 1):
            try:
                print(f"[{i}/{len(stock_codes)}] 処理中: {code}")
                
                # 既存データのチェック（強制更新でない場合）
                if not force_update and self._file_exists(code):
                    print(f"  → スキップ（既存データあり）")
                    stats['skipped_count'] += 1
                    continue
                
                # 財務データを取得・保存
                financial_data = self.get_financial_statements(code)
                
                if not financial_data.empty:
                    self.save_stock_data(code, financial_data)
                    stats['success_count'] += 1
                    print(f"  → 成功（{len(financial_data)}件）")
                else:
                    print(f"  → データなし")
                    stats['skipped_count'] += 1
                
                # APIレート制限対策
                if i < len(stock_codes):
                    time.sleep(delay_seconds)
                
            except Exception as e:
                error_msg = f"銘柄コード {code}: {str(e)}"
                stats['errors'].append(error_msg)
                stats['error_count'] += 1
                print(f"  → エラー: {e}")
                
                # エラー数が上限に達した場合は処理を停止
                if stats['error_count'] >= max_errors:
                    print(f"エラー数が上限（{max_errors}）に達しました。処理を停止します。")
                    break
        
        stats['end_time'] = datetime.now()
        processing_time = stats['end_time'] - stats['start_time']
        
        # 結果サマリーを表示
        print("\n" + "=" * 60)
        print("【一括取得結果サマリー】")
        print(f"処理時間: {processing_time}")
        print(f"対象市場: {', '.join(markets)}")
        print(f"総銘柄数: {stats['total_stocks']}")
        print(f"成功: {stats['success_count']}")
        print(f"スキップ: {stats['skipped_count']}")
        print(f"エラー: {stats['error_count']}")
        
        if stats['errors']:
            print(f"\n【エラー詳細】")
            for error in stats['errors'][:5]:  # 最初の5件のみ表示
                print(f"  - {error}")
            if len(stats['errors']) > 5:
                print(f"  ... 他 {len(stats['errors']) - 5} 件のエラー")
        
        # 更新状況ファイルを保存
        self._save_batch_status(stats, force_update)
        
        return stats
    
    def _save_batch_status(self, stats, force_update=False):
        """
        一括取得状況をテキストファイルに保存する
        
        Args:
            stats (dict): 一括取得統計情報
            force_update (bool): 強制更新かどうか
        """
        try:
            current_date = datetime.now().strftime('%Y%m%d')
            
            # 統一ファイル名を使用
            filename = f"{self.database_dir}/update_info.txt"
            
            # ファイル内容
            content = f"""市場別銘柄財務データ一括取得状況
取得日時: {stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}
処理日: {current_date}
対象市場: {', '.join(stats['markets'])}
取得モード: {'強制取得' if force_update else '通常取得'}
成功取得数: {stats['success_count']:04d}
総処理数: {stats['total_stocks']}
スキップ数: {stats['skipped_count']}
エラー数: {stats['error_count']}
更新銘柄数: 0
処理時間: {stats['end_time'] - stats['start_time']}
"""
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print(f"\n一括取得状況を保存しました: {filename}")
            
        except Exception as e:
            print(f"一括取得状況ファイルの保存中にエラーが発生しました: {e}")
    
    def _save_update_status(self, stats):
        """
        更新状況をテキストファイルに保存する
        
        Args:
            stats (dict): 更新統計情報
        """
        try:
            current_date = datetime.now().strftime('%Y%m%d')
            success_count = stats['success_count']
            
            # 統一ファイル名を使用
            filename = f"{self.database_dir}/update_info.txt"
            
            # ファイル内容
            content = f"""財務データベース更新状況
更新日時: {stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}
処理日: {current_date}
成功取得数: {success_count:04d}
総処理数: {stats['total_stocks']}
スキップ数: {stats['skipped_count']}
エラー数: {stats['error_count']}
更新銘柄数: {success_count}
処理時間: {stats['end_time'] - stats['start_time']}
"""
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print(f"\n更新状況を保存しました: {filename}")
            
        except Exception as e:
            print(f"更新状況ファイルの保存中にエラーが発生しました: {e}")
    
    def get_update_status_files(self):
        """
        更新状況ファイルの一覧を取得する
        
        Returns:
            list: 更新状況ファイルのパスのリスト
        """
        import glob
        
        pattern = f"{self.database_dir}/*-*.txt"
        files = glob.glob(pattern)
        return sorted(files, reverse=True)  # 新しい順
    
    def get_latest_update_status(self):
        """
        最新の更新状況を取得する
        
        Returns:
            dict: 最新の更新状況情報
        """
        status_files = self.get_update_status_files()
        
        if not status_files:
            return None
        
        latest_file = status_files[0]
        
        try:
            with open(latest_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # ファイル名から日付と成功数を抽出
            filename = os.path.basename(latest_file)
            date_part, count_part = filename.replace('.txt', '').split('-')
            
            return {
                'file_path': latest_file,
                'date': date_part,
                'success_count': int(count_part),
                'content': content
            }
            
        except Exception as e:
            print(f"更新状況ファイルの読み込み中にエラーが発生しました: {e}")
            return None

    @staticmethod
    def validate_stock_code(code):
        """
        銘柄コードの形式を検証する（4桁または5桁の英数字に対応）
        
        Args:
            code (str): 銘柄コード
            
        Returns:
            bool: 有効な形式かどうか
        """
        if not code:
            return False
        
        # 4桁または5桁の英数字かチェック（アルファベットも許可）
        if len(code) not in [4, 5]:
            return False
        
        # 英数字のみかチェック
        if not code.isalnum():
            return False
            
        return True
    
    def _check_and_update_database(self):
        """初期化時にデータベースの状況を確認し、必要に応じて更新する"""
        try:
            current_date = datetime.now().strftime('%Y%m%d')
            print(f"データベース状況確認中... (現在日付: {current_date})")
            
            # 既存ファイルの移行を実行
            self._migrate_old_status_files()
            
            # 最新のステータスファイルを検索
            latest_file = self._find_latest_status_file()
            
            if latest_file is None:
                print("ステータスファイルが見つかりません。全リスト強制取得を実行します。")
                self._perform_full_force_update()
            else:
                # ファイルの日付を解析
                file_date = self._parse_status_file_date(latest_file)
                print(f"最新ステータスファイル: {latest_file} (日付: {file_date})")
                
                if file_date == current_date:
                    # 同じ日でも新規データがあるかチェック
                    if self._check_same_day_updates(file_date, current_date):
                        print("当日の新規財務データを検出しました。当日更新を実行します。")
                        self._perform_same_day_update(current_date)
                    else:
                        print("データは既に最新です。")
                else:
                    print(f"日付範囲更新を実行します: {file_date} → {current_date}")
                    self._perform_date_range_update(file_date, current_date)
                    
        except Exception as e:
            print(f"データベース更新処理中にエラーが発生しました: {e}")
            print("処理を継続します。")
    
    def _find_latest_status_file(self):
        """最新のステータスファイルを検索する"""
        try:
            # 新しい統一ファイル名を優先的に検索
            unified_file = f"{self.database_dir}/update_info.txt"
            if os.path.exists(unified_file):
                return unified_file
            
            # 既存のファイル名形式もフォールバック
            pattern = f"{self.database_dir}/*_*.txt"
            files = glob.glob(pattern)
            
            if not files:
                return None
            
            # ファイル名でソート（日付順）
            status_files = []
            for file in files:
                filename = os.path.basename(file)
                # <日付>_<数字4桁>.txt の形式をチェック
                match = re.match(r'^(\d{8})_(\d{4})\.txt$', filename)
                if match:
                    status_files.append((file, match.group(1)))
            
            if not status_files:
                return None
            
            # 最新のファイルを返す
            latest_file = max(status_files, key=lambda x: x[1])
            return latest_file[0]
            
        except Exception as e:
            print(f"ステータスファイル検索中にエラーが発生しました: {e}")
            return None
    
    def _parse_status_file_date(self, filepath):
        """ステータスファイルから日付を抽出する（内容ベース優先）"""
        try:
            # 新しい統一ファイル名の場合は内容から解析
            if filepath.endswith('/update_info.txt'):
                return self._parse_status_file_content(filepath).get('date')
            
            # 既存のファイル名形式の場合はファイル名から解析
            filename = os.path.basename(filepath)
            match = re.match(r'^(\d{8})_(\d{4})\.txt$', filename)
            if match:
                return match.group(1)
            return None
        except Exception as e:
            print(f"ファイル解析中にエラーが発生しました: {e}")
            return None
    
    def _parse_status_file_content(self, filepath):
        """ステータスファイルの内容を解析する"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            info = {}
            for line in content.split('\n'):
                if ':' in line:
                    key, value = line.split(':', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    if key == '処理日':
                        info['date'] = value
                    elif key == '更新銘柄数':
                        info['updated_count'] = int(value) if value.isdigit() else 0
                    elif key == '成功取得数':
                        info['success_count'] = int(value) if value.isdigit() else 0
                    elif key == '更新日時':
                        info['update_time'] = value
                    elif key == '更新モード':
                        info['update_mode'] = value
                    elif key == '対象日付範囲':
                        info['date_range'] = value
                    elif key == '対象市場':
                        info['markets'] = value
                    elif key == '当日取得財務データ数':
                        info['当日取得財務データ数'] = int(value) if value.isdigit() else 0
                    elif key == '前回取得財務データ数':
                        info['前回取得財務データ数'] = int(value) if value.isdigit() else 0
            
            return info
            
        except Exception as e:
            print(f"ステータスファイル内容解析中にエラーが発生しました: {e}")
            return {}
    
    def _migrate_old_status_files(self):
        """既存のステータスファイルを新しい形式に移行する"""
        try:
            # 既存のファイル名形式のファイルを検索
            pattern = f"{self.database_dir}/*_*.txt"
            files = glob.glob(pattern)
            
            if not files:
                return
            
            # 最新のファイルを特定
            latest_file = None
            latest_date = None
            for file in files:
                filename = os.path.basename(file)
                match = re.match(r'^(\d{8})_(\d{4})\.txt$', filename)
                if match:
                    file_date = match.group(1)
                    if latest_date is None or file_date > latest_date:
                        latest_date = file_date
                        latest_file = file
            
            if latest_file and latest_date:
                # 最新のファイルを新しい形式に変換
                self._convert_to_unified_format(latest_file, latest_date)
                print(f"既存のステータスファイルを新しい形式に移行しました: {latest_file}")
                
        except Exception as e:
            print(f"既存ファイルの移行中にエラーが発生しました: {e}")
    
    def _convert_to_unified_format(self, old_file, date):
        """既存のステータスファイルを統一形式に変換する"""
        try:
            # 既存ファイルの内容を読み込み
            with open(old_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 統一ファイル名で保存
            unified_file = f"{self.database_dir}/update_info.txt"
            with open(unified_file, 'w', encoding='utf-8') as f:
                f.write(content)
            
            # 既存ファイルを削除
            os.remove(old_file)
            
        except Exception as e:
            print(f"ファイル変換中にエラーが発生しました: {e}")
    
    def _perform_full_force_update(self):
        """全銘柄の強制取得を実行する"""
        try:
            print("全銘柄強制取得を開始します...")
            stats = self.batch_get_market_stocks_data(
                markets=['プライム', 'スタンダード', 'グロース'],
                delay_seconds=0.5,
                max_errors=10,
                force_update=True
            )
            print("全銘柄強制取得が完了しました。")
            return stats
        except Exception as e:
            print(f"全銘柄強制取得中にエラーが発生しました: {e}")
            return None
    
    def _perform_date_range_update(self, start_date, end_date):
        """日付範囲での更新を実行する"""
        try:
            # 日付範囲を生成
            date_list = self._generate_date_range(start_date, end_date)
            print(f"対象日付: {', '.join(date_list)}")
            
            # 各日付で財務データを取得し、銘柄コードを収集
            collected_codes = self._collect_stock_codes_from_dates(date_list)
            print(f"収集した銘柄コード数: {len(collected_codes)}")
            
            if not collected_codes:
                print("収集した銘柄コードがありません。")
                return None
            
            # 収集した銘柄コードで財務データを更新
            updated_count = self._update_collected_stocks(collected_codes)
            print(f"更新した銘柄数: {updated_count}")
            
            # ステータスファイルを保存
            self._save_date_range_status(start_date, end_date, len(collected_codes), updated_count)
            
            return {
                'collected_codes': len(collected_codes),
                'updated_count': updated_count,
                'date_range': f"{start_date} - {end_date}"
            }
            
        except Exception as e:
            print(f"日付範囲更新中にエラーが発生しました: {e}")
            return None
    
    def _generate_date_range(self, start_date, end_date):
        """開始日から終了日までの日付リストを生成する"""
        try:
            start_dt = datetime.strptime(start_date, '%Y%m%d')
            end_dt = datetime.strptime(end_date, '%Y%m%d')
            
            date_list = []
            current_dt = start_dt
            while current_dt <= end_dt:
                date_list.append(current_dt.strftime('%Y-%m-%d'))
                current_dt += timedelta(days=1)
            
            return date_list
        except Exception as e:
            print(f"日付範囲生成中にエラーが発生しました: {e}")
            return []
    
    def get_financial_statements_by_date(self, date):
        """指定日付の財務データを取得する"""
        try:
            client = self._get_client()
            print(f"日付 {date} の財務データを取得中...")
            
            # J-Quants APIで日付指定の財務データを取得
            # パラメータ名を 'date' から 'date_yyyymmdd' に変更
            financial_data = client.get_fins_statements(date_yyyymmdd=date)
            
            if not financial_data.empty:
                print(f"日付 {date} の財務データを取得しました: {len(financial_data)} 件")
                return financial_data
            else:
                print(f"日付 {date} の財務データが見つかりませんでした")
                return pd.DataFrame()
            
        except Exception as e:
            print(f"日付 {date} の財務データ取得中にエラーが発生しました: {e}")
            return pd.DataFrame()
    
    def _collect_stock_codes_from_dates(self, date_list):
        """指定日付リストから銘柄コードを収集する"""
        collected_codes = set()
        
        for date in date_list:
            try:
                financial_data = self.get_financial_statements_by_date(date)
                if not financial_data.empty and 'LocalCode' in financial_data.columns:
                    codes = financial_data['LocalCode'].unique()
                    collected_codes.update(codes)
                    print(f"日付 {date}: {len(codes)} 銘柄のコードを収集")
            except Exception as e:
                print(f"日付 {date} の銘柄コード収集中にエラーが発生しました: {e}")
                continue
        
        return list(collected_codes)
    
    def _update_collected_stocks(self, stock_codes):
        """収集した銘柄コードで財務データを更新する"""
        updated_count = 0
        
        for i, code in enumerate(stock_codes, 1):
            try:
                print(f"[{i}/{len(stock_codes)}] 更新中: {code}")
                
                # 日付指定なしで財務データを取得・保存
                financial_data = self.get_financial_statements(code)
                if not financial_data.empty:
                    self.save_stock_data(code, financial_data)
                    updated_count += 1
                    print(f"  → 成功（{len(financial_data)}件）")
                else:
                    print(f"  → データなし")
                
                # APIレート制限対策
                if i < len(stock_codes):
                    import time
                    time.sleep(0.5)
                    
            except Exception as e:
                print(f"  → エラー: {e}")
                continue
        
        return updated_count
    
    def _get_today_financial_data_count(self):
        """当日の財務データ数を取得する"""
        try:
            current_date = datetime.now().strftime('%Y-%m-%d')
            today_data = self.get_financial_statements_by_date(current_date)
            return len(today_data) if not today_data.empty else 0
        except Exception as e:
            print(f"当日財務データ数取得中にエラーが発生しました: {e}")
            return 0
    
    def _get_previous_data_count(self):
        """前回の財務データ数を取得する"""
        try:
            status_info = self._parse_status_file_content(f"{self.database_dir}/update_info.txt")
            return status_info.get('当日取得財務データ数', 0)
        except Exception as e:
            print(f"前回財務データ数取得中にエラーが発生しました: {e}")
            return 0
    
    def _check_same_day_updates(self, file_date, current_date):
        """同じ日でも新規データがあるかチェックする"""
        if file_date != current_date:
            return False
        
        try:
            # 当日の財務データ数を取得
            current_data_count = self._get_today_financial_data_count()
            if current_data_count == 0:
                print("当日の財務データが取得できませんでした。")
                return False
            
            # 前回の財務データ数と比較
            previous_data_count = self._get_previous_data_count()
            
            print(f"前回取得財務データ数: {previous_data_count}")
            print(f"当日取得財務データ数: {current_data_count}")
            
            has_new_data = current_data_count > previous_data_count
            if has_new_data:
                print(f"当日の新規財務データを検出しました（+{current_data_count - previous_data_count}件）")
            else:
                print("当日の新規財務データはありません。")
            
            return has_new_data
            
        except Exception as e:
            print(f"当日新規データチェック中にエラーが発生しました: {e}")
            return False
    
    def _perform_same_day_update(self, current_date):
        """当日のみの更新処理を実行する"""
        try:
            print("=== 当日更新処理開始 ===")
            
            # 当日の財務データを取得
            today_data = self.get_financial_statements_by_date(current_date)
            
            if today_data.empty:
                print("当日の新規財務データはありません。")
                return None
            
            # 銘柄コードを収集
            if 'LocalCode' not in today_data.columns:
                print("LocalCode列が見つかりません。")
                return None
            
            codes = today_data['LocalCode'].unique()
            print(f"当日の新規銘柄数: {len(codes)}")
            
            # 各銘柄の財務データを更新
            updated_count = self._update_collected_stocks(codes)
            
            # ステータスファイルを保存
            self._save_same_day_status(current_date, len(codes), updated_count)
            
            print("=== 当日更新処理完了 ===")
            
            return {
                'collected_codes': len(codes),
                'updated_count': updated_count,
                'update_mode': 'same_day'
            }
            
        except Exception as e:
            print(f"当日更新処理中にエラーが発生しました: {e}")
            return None
    
    def _save_same_day_status(self, current_date, collected_count, updated_count):
        """当日更新のステータスファイルを保存する"""
        try:
            # 当日の財務データ数を取得
            today_data_count = self._get_today_financial_data_count()
            previous_data_count = self._get_previous_data_count()
            
            # 統一ファイル名を使用
            filename = f"{self.database_dir}/update_info.txt"
            
            # ファイル内容
            content = f"""当日財務データ更新状況
更新日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
処理日: {current_date}
収集銘柄数: {collected_count:04d}
更新銘柄数: {updated_count}
更新モード: 当日更新
当日取得財務データ数: {today_data_count}
前回取得財務データ数: {previous_data_count}
"""
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print(f"当日更新状況を保存しました: {filename}")
            
        except Exception as e:
            print(f"当日更新状況ファイルの保存中にエラーが発生しました: {e}")
    
    def _save_date_range_status(self, start_date, end_date, collected_count, updated_count):
        """日付範囲更新のステータスファイルを保存する"""
        try:
            current_date = datetime.now().strftime('%Y%m%d')
            
            # 当日の財務データ数を取得
            today_data_count = self._get_today_financial_data_count()
            previous_data_count = self._get_previous_data_count()
            
            # 統一ファイル名を使用
            filename = f"{self.database_dir}/update_info.txt"
            
            # ファイル内容
            content = f"""日付範囲財務データ更新状況
更新日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
処理日: {current_date}
対象日付範囲: {start_date} - {end_date}
収集銘柄数: {collected_count:04d}
更新銘柄数: {updated_count}
更新モード: 日付範囲更新
当日取得財務データ数: {today_data_count}
前回取得財務データ数: {previous_data_count}
"""
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print(f"日付範囲更新状況を保存しました: {filename}")
            
        except Exception as e:
            print(f"日付範囲更新状況ファイルの保存中にエラーが発生しました: {e}")
    
