import sqlite3
import pandas as pd

class LocalCache:
    '''
    SQLiteのヘルパークラス
    
    Parameters
    ----------
    filename : str
        SQLiteのデータベースファイルへのパス
    
    '''
    
    def __init__(self,dbname):
        self.dbname = dbname
    
    def execute(self,sql):
        '''
        SQLを実行し、結果を取得する
        
        Parameters
        ----------
        columns : str
            実行したいSQL
        '''
        conn = sqlite3.connect(self.dbname)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
        
    def execute_all(self,sqls):
        '''
        複数のSQLをトランザクション配下で実行する
        
        Parameters
        ----------
        columns : strs
            実行したいSQLのリスト
        '''
        conn = sqlite3.connect(self.dbname)
        cur = conn.cursor()
        try:
            for sql in sqls:
                cur.execute(sql)
            conn.commit()
        except sqlite3.Error as e:
            conn.rollback()
        
        cur.close()
        conn.close()
        
    def execute_query(self,sql):
        '''
        select 系のSQLを実行し、結果を全て取得する
        
        Parameters
        ----------
        columns : str
            実行したいSQL
        
        Returns
        ----------
        data: list
            １行分をタプルとし、複数行をリストとして返す
            <例> [('RX100','Sony',35000),('RX200','Sony',42000)]
        '''
        conn = sqlite3.connect(self.dbname)
        cur = conn.cursor()
        cur.execute(sql)
        res = cur.fetchall()
        cur.close()
        conn.close()
        return res
        
    def execute_scalor(self,sql):
        '''
        結果の値が１つしかないSQLを実行し、結果を取得する
        
        Parameters
        ----------
        columns : str
            実行したいSQL
        
        Returns
        ----------
        res:
            実行結果により返された値
        '''
        conn = sqlite3.connect(self.dbname)
        cur = conn.cursor()
        cur.execute(sql)
        res = cur.fetchone()
        cur.close()
        conn.close()
        return res[0] if res != None else None
        
    def create(self,tablename,columns,primarykey = '',isdrop=False):
        '''
        テーブルを作成する
        
        Parameters
        ----------
        columns : str
            「列名」又は「列名＋型」をカンマ区切りで指定
            <例> 'product text,price int,maker,year'
        primarykey: str
            プライマリーキーをカンマ区切りで指定
            <例> 'product,year'
        '''
        if isdrop :
            self.drop(tablename)
        
        pkey = ',primary key({0})'.format(primarykey) if primarykey != '' else ''
        sql = 'create table {0}({1} {2})'.format(tablename,columns,pkey)
        self.execute(sql)
        
    def drop(self,tablename):
        '''
        指定されたテーブルが存在すれば削除、無ければ何もしない
        
        Parameters
        ----------
        tablename : str
            削除したいテーブル名
        '''
        self.execute("drop table if exists {0}".format(tablename))
        
    def exists(self,tablename):
        '''
        指定したテーブル、又はビューの有無を判定する
        
        Parameters
        ----------
        columns : str
            実行したいSQL
        Returns
        ----------
            テーブル又はビューが存在すればTrue 存在しなければ False
        '''
        res = self.execute_scalor("select count(*) from sqlite_master where name='{0}'".format(tablename))
        return True if res > 0 else False
        
    def rename(self,old_tablename,new_tablename):
        '''
        テーブル名を変更する
        
        Parameters
        ----------
        old_tablename : str
            変更前のテーブル名
        new_tablename : str
            変更後のテーブル名
        '''
        self.execute_query("alter table {0} rename to {1}".format(old_tablename,new_tablename))
        
    def add_column(self,tablename,columns):
        '''
        テーブル名を変更する
        
        Parameters
        ----------
        tablename : str
            テーブル名
        columns : str
            「列名」又は「列名＋型」をカンマ区切りで指定
            <例> 'product text,price int,maker,year'
        '''
        sqls = []
        for column in columns.split(','):
            sqls.append("alter table {0} add column {1}".format(tablename,column))
        
        self.execute_all(sqls)
        
    def get_create_statment(self,tablename):
        '''
        指定したテーブルのCreate文を取得する
        
        Parameters
        ----------
        tablename : str
            テーブル名
        
        Returns
        ----------
        res:str
            Create文
        '''
        res = self.execute_scalor("select sql from sqlite_master where name='{0}'".format(tablename))
        return res
        
    def get_table_list(self,table_type=''):
        '''
        登録されているテーブルの一覧を取得する
        
        Parameters
        ----------
        table_type : str
            'table' => テーブルのみ、'view' => ビューのみ、'' => テーブルとビューの両方
        
        Returns
        ----------
        res:str
            リスト形式のテーブル名一覧
            <例>  ['talbe1','table2','table3']
        '''
        res = self.execute_query("select tbl_name from sqlite_master where type like '%{0}%'".format(table_type))
        return [name[0] for name in res]
        
    def get_column_type(self,tablename):
        '''
        指定したテーブルのカラムと型を一覧で取得する
        
        Parameters
        ----------
        tablename : str
            テーブル名
        
        Returns
        ----------
        res:str
            リスト形式でカラム名と型のタプルを返す
            <例>  [('column1','int'),('column2','text'),('column3',real)]
        '''
        res = self.execute_query("Pragma table_info('{0}')".format(tablename))
        return [(name[1],name[2]) for name in res]
        
    def get_column_list(self,tablename):
        '''
        指定したテーブルのカラム名を一覧で取得する
        
        Parameters
        ----------
        tablename : str
            テーブル名
        
        Returns
        ----------
        res:str
            リスト形式のカラム名一覧
            <例>  ['column1','column2','column3']
        '''
        res = self.execute_query("Pragma table_info('{0}')".format(tablename))
        return [name[1] for name in res]
        
    def vacuum(self):
        '''
        データベースの空き領域を解放する
        '''
        self.execute('vacuum')
        
    def create_table_csv(self,tablename,csvpath):
        '''
        csvからtableを作成する
        
        Parameters
        ----------
        tablename : str
            テーブル名
        csvpath : str
            csvファイルパス
        
        '''
        df = pd.read_csv(csvpath)
        conn = sqlite3.connect(self.dbname)
        df.to_sql(tablename, conn, if_exists='replace')
        
    def get_csv(self,filepath,sql):
        '''
        csvからtableを作成する
        
        Parameters
        ----------
        filepath : str
            出力するファイルパス
        sql : str
            取り出すSQL
        
        '''
        conn = sqlite3.connect(self.dbname)
        df = pd.read_sql(sql,conn)
        df.to_csv(filepath)
