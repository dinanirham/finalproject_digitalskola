import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
import time
import numpy as np

class Transformer():
    def __init__(self, engine_sql, engine_postgres):
        self.engine_sql = engine_sql
        self.engine_postgres = engine_postgres
        

    def get_data_from_mysql(self):
        sql = """SELECT * FROM covid_jabar"""
        start_time_get_data_mysql = time.time()
        df = pd.read_sql(sql, con=self.engine_sql)
        print("[INFO] GET DATA FROM MYSQL IS SUCCESSFUL")
        print(f"[INFO] TOTAL GETTING DATA TIME FROM MYSQL IS {time.time()-start_time_get_data_mysql} S")
        return df


    def create_dimension_province(self):
        df = self.get_data_from_mysql()
        df_province = df[['kode_prov', 'nama_prov']]
        df_province = df_province.rename(columns={'kode_prov': 'province_id', 'nama_prov': 'province_name'})
        df_province = df_province.drop_duplicates()

        try:
            drop_table = """DROP TABLE IF EXISTS dim_province"""
            self.engine_postgres.execute(drop_table)
        except SQLAlchemyError as e:
            print(e)
        
        # insert to postgres
        start_time_dim_prov = time.time()
        df_province.to_sql(con=self.engine_postgres, name='dim_province', index=False)
        print("[INFO] INSERTED TO POSTGRES IS SUCCESSFUL")
        print(f"[INFO] TOTAL INSERTED TIME TO POSTGRES IS {time.time()-start_time_dim_prov} S")


    def create_dimension_district(self):
        df = self.get_data_from_mysql()
        df_district = df[['kode_kab', 'kode_prov', 'nama_kab']]
        df_district = df_district.rename(columns={'kode_kab': 'district_id', 'kode_prov': 'province_id', 'nama_district': 'district_name'})
        df_district = df_district.drop_duplicates()

        try:
            drop_table = """DROP TABLE IF EXISTS dim_district"""
            self.engine_postgres.execute(drop_table)
        except SQLAlchemyError as e:
            print(e)
        
        # insert to postgres
        start_time_dim_dist = time.time()
        df_district.to_sql(con=self.engine_postgres, name='dim_district', index=False)
        print("[INFO] INSERTED TO POSTGRES IS SUCCESSFUL")
        print(f"[INFO] TOTAL INSERTED TIME TO POSTGRES IS {time.time()-start_time_dim_dist} S")


    def create_dimension_case(self):
        df = self.get_data_from_mysql()
        column_start = ['suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 
                        'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 
                        'closecontact_meninggal', 'probable_meninggal']

        column_end = ['id', 'status_name', 'status_detail', 'status']

        df = df[column_start]
        df = df[:1]
        df = df.melt(var_name = 'status', value_name = 'total')
        df = df.drop_duplicates('status').sort_values('status')

        df['id'] = np.arange(1, df.shape[0]+1)
        df[['status_name', 'status_detail']] = df['status'].str.split('_', n=1, expand=True)

        df = df[column_end]

        try:
            drop_table = """DROP TABLE IF EXISTS dim_case"""
            self.engine_postgres.execute(drop_table)
        except SQLAlchemyError as e:
            print(e)
        
        # insert to postgres
        start_time_dim_case = time.time() 
        df.to_sql(con=self.engine_postgres, name='dim_case', index=False, if_exists='replace')
        print("[INFO] INSERTED TO POSTGRES IS SUCCESSFUL")
        print(f"[INFO] TOTAL INSERTED TIME TO POSTGRES IS {time.time()-start_time_dim_case} S")
        return df

    def create_province_daily(self):
        df = self.get_data_from_mysql()
        df_case_dim = self.create_dimension_case()

        column_start = ['tanggal', 'kode_prov', 'suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 
                        'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 
                        'closecontact_meninggal', 'probable_meninggal']

        column_end = ['date', 'province_id', 'status', 'total']

        data = df[column_start]
        data = data.melt(id_vars=['tanggal', 'kode_prov'], var_name = 'status', value_name = 'total').sort_values(['tanggal', 'kode_prov', 'status', 'total'])
        data = data.groupby(by=['tanggal', 'kode_prov', 'status']).sum()
        data = data.reset_index()

        data.columns = column_end
        data['id'] = np.arange(1, data.shape[0]+1)
        df_case_dim = df_case_dim.rename({'id' : 'case_id'}, axis=1)

        data = pd.merge(data, df_case_dim, how = 'inner', on = 'status')
        data = data[['id', 'province_id', 'case_id', 'date', 'total']]

        try:
            drop_table = """DROP TABLE IF EXISTS province_daily"""
            self.engine_postgres.execute(drop_table)
        except SQLAlchemyError as e:
            print(e)
        
        # insert to postgres
        start_time_prov_daily = time.time()
        data.to_sql(con=self.engine_postgres, name='province_daily', index=False)
        print("[INFO] INSERTED TO POSTGRES IS SUCCESSFUL")
        print(f"[INFO] TOTAL INSERTED TIME TO POSTGRES IS {time.time()-start_time_prov_daily} S")
        return df


    def create_district_daily(self):
        df = self.get_data_from_mysql()
        df_case_dim = self.create_dimension_case()

        column_start = ['tanggal', 'kode_kab', 'suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 
                        'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 
                        'closecontact_meninggal', 'probable_meninggal']

        column_end = ['date', 'district_id', 'status', 'total']

        data = df[column_start]
        data = data.melt(id_vars = ['tanggal','kode_kab'], var_name = 'status', value_name = 'total').sort_values(['tanggal','kode_kab','status','total'])
        data = data.groupby(by=['tanggal','kode_kab', 'status']).sum()
        data = data.reset_index()

        data.columns = column_end
        data['id'] = np.arange(1, data.shape[0]+1)
        df_case_dim = df_case_dim.rename({'id' : 'case_id'}, axis=1)

        data = pd.merge(data, df_case_dim, how = 'inner', on = 'status')
        data = data[['id', 'district_id', 'case_id', 'date', 'total']]

        try:
            drop_table = "DROP TABLE IF EXISTS district_daily"
            self.engine_postgres.execute(drop_table)
        except SQLAlchemyError as e:
            print(e)
        
        # insert to postgres
        start_time_dist_daily = time.time()
        data.to_sql(con=self.engine_postgres, name='district_daily', index=False)
        print("[INFO] INSERTED TO POSTGRES IS SUCCESSFUL")
        print(f"[INFO] TOTAL INSERTED TIME TO POSTGRES IS {time.time()-start_time_dist_daily} S")
        return df
