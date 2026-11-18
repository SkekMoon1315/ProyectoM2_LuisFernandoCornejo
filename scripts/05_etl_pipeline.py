# %% [markdown]
# # FleetLogix | A3-05 — Pipeline ETL
# Henry · Módulo 2 · PostgreSQL → Snowflake
# 
# Versión ejecutable del ejercicio: extracción histórica e incremental, seis dimensiones, hechos, SCD2 y totales diarios. Conserva el modelo ya creado. Ejecutar en orden; no ejecutar simultáneamente desde dos kernels. No modifica PostgreSQL. La carga histórica incluye únicamente entregas completadas, según el diseño del Avance 3. Las anticipadas se conservan. Los datos de clientes se identifican por nombre y ciudad (aproximación). Los atributos sin fuente se dejan nulos. Las vigencias históricas previas a la primera captura son aproximadas; los cambios nuevos se registran por día de observación. Costos e ingresos estimados en COP; combustible y distancia prorrateados. Calendario colombiano y año fiscal calendario. No se presupone que el código haya sido probado contra tus servidores.

# %%
# Dependencias: pip install psycopg2-binary "snowflake-connector-python[pandas]" pandas numpy schedule holidays python-dotenv tzdata pytz

# %% [markdown]
# ## 1. Configuración y credenciales
# Introduce los datos cuando se soliciten. Para Snowflake usa el identificador organización-cuenta (o locator con región si corresponde), no la URL. Si tu cuenta exige MFA, selecciona el método compatible; externalbrowser permite autenticación mediante navegador cuando está habilitada.

# %%
import os, logging, time, json
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from getpass import getpass
import pandas as pd
import numpy as np
import psycopg2
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import holidays, schedule
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', handlers=[logging.FileHandler('etl_pipeline.log', encoding='utf-8'), logging.StreamHandler()])
TZ = ZoneInfo('America/Bogota')
POSTGRES_CONFIG = dict(host=os.getenv('PGHOST','localhost'), database=os.getenv('PGDATABASE','fleetlogix'), user=os.getenv('PGUSER','postgres'), port=int(os.getenv('PGPORT','5432')), password=os.getenv('PGPASSWORD') or getpass('Contraseña PostgreSQL: '))
# Autenticación PAT; no guardar secretos en el código fuente.
SNOWFLAKE_CONFIG = dict(
    account=os.getenv('SF_ACCOUNT') or input('Identificador de cuenta Snowflake: '),
    user=os.getenv('SF_USER') or input('Usuario Snowflake: '),
    token=os.getenv('SF_TOKEN') or getpass('Token PAT de Snowflake: '),
    authenticator='PROGRAMMATIC_ACCESS_TOKEN',
    warehouse='FLEETLOGIX_WH', database='FLEETLOGIX_DW',
    schema='ANALYTICS', role='FLEETLOGIX_ETL_ROLE', autocommit=False,
    login_timeout=60,
)

# %% [markdown]
# ## 2. Implementación del pipeline
# Se utilizan tablas temporales tipadas, cargas masivas y MERGE. Las tablas permanentes existentes no se reemplazan. Los cambios dimensionales y hechos se confirman en una sola transacción; la creación de tablas temporales se hace antes del DML. Los agregados se recalculan sobre los hechos completos. No se reconstruye historial SCD2 anterior a la primera captura.

# %%
SQL_EXTRACT = """
WITH counts AS (SELECT trip_id, COUNT(*) AS deliveries_in_trip FROM deliveries GROUP BY trip_id)
SELECT d.delivery_id,d.trip_id,d.tracking_number,d.customer_name,d.package_weight_kg,
       d.scheduled_datetime,d.delivered_datetime,d.delivery_status,d.recipient_signature,
       t.vehicle_id,t.driver_id,t.route_id,t.departure_datetime,t.arrival_datetime,
       t.fuel_consumed_liters,r.distance_km,r.toll_cost,r.destination_city,c.deliveries_in_trip
FROM deliveries d JOIN trips t USING(trip_id) JOIN routes r USING(route_id)
JOIN counts c USING(trip_id)
WHERE d.delivery_status='delivered' AND d.delivered_datetime IS NOT NULL
"""
SQL_TOTALS_CREATE = """CREATE TABLE IF NOT EXISTS daily_delivery_totals (
 date_key INT PRIMARY KEY, total_deliveries INT, on_time_deliveries INT,
 total_weight_kg NUMBER(18,2), total_revenue NUMBER(18,2), total_cost NUMBER(18,2))"""
SQL_TOTALS_MERGE = """MERGE INTO daily_delivery_totals t USING (
 SELECT date_key,COUNT(*) total_deliveries,COALESCE(COUNT_IF(is_on_time),0) on_time_deliveries,
 SUM(package_weight_kg) total_weight_kg,SUM(revenue_per_delivery) total_revenue,SUM(cost_per_delivery) total_cost
 FROM fact_deliveries GROUP BY date_key) s ON t.date_key=s.date_key
 WHEN MATCHED THEN UPDATE SET t.total_deliveries=s.total_deliveries,t.on_time_deliveries=s.on_time_deliveries,t.total_weight_kg=s.total_weight_kg,t.total_revenue=s.total_revenue,t.total_cost=s.total_cost
 WHEN NOT MATCHED THEN INSERT(date_key,total_deliveries,on_time_deliveries,total_weight_kg,total_revenue,total_cost)
 VALUES(s.date_key,s.total_deliveries,s.on_time_deliveries,s.total_weight_kg,s.total_revenue,s.total_cost)"""

def datekey(s): return s.dt.strftime('%Y%m%d').astype('int64')
def timekey(s): return s.dt.hour*100+s.dt.minute

def transform_data(df):
    df=df.copy()
    for c in ['scheduled_datetime','delivered_datetime','departure_datetime','arrival_datetime']:
        df[c]=pd.to_datetime(df[c],errors='coerce')
    for c in ['package_weight_kg','distance_km','fuel_consumed_liters','toll_cost','deliveries_in_trip']:
        df[c]=pd.to_numeric(df[c],errors='coerce')
    delta=(df.delivered_datetime-df.scheduled_datetime).dt.total_seconds()/60
    duration=(df.arrival_datetime-df.departure_datetime).dt.total_seconds()/3600
    elapsed=(df.delivered_datetime-df.departure_datetime).dt.total_seconds()/60
    valid=(df.package_weight_kg.gt(0)&df.package_weight_kg.lt(10000)&duration.gt(0)&elapsed.ge(0)&df.scheduled_datetime.notna()&df.deliveries_in_trip.gt(0)&df.customer_name.notna()&df.destination_city.notna())
    rejected=df.loc[~valid,['delivery_id']].copy()
    df=df.loc[valid].copy()
    df['delivery_time_minutes']=elapsed.loc[valid].round().astype(int)
    df['delay_minutes']=delta.loc[valid].clip(lower=0).round().astype(int)
    df['is_on_time']=delta.loc[valid]<=30
    df['deliveries_per_hour']=(df.deliveries_in_trip/duration.loc[valid]).round(2)
    df['fuel_efficiency_km_per_liter']=(df.distance_km/df.fuel_consumed_liters.where(df.fuel_consumed_liters>0)).round(2)
    df['cost_per_delivery']=((df.fuel_consumed_liters*5000+df.toll_cost)/df.deliveries_in_trip).round(2)
    df['revenue_per_delivery']=(20000+df.package_weight_kg*500).round(2)
    for c in ['distance_km','fuel_consumed_liters']: df[c]=(df[c]/df.deliveries_in_trip).round(2)
    for c in ['deliveries_per_hour','fuel_efficiency_km_per_liter']: df[c]=df[c].where(df[c].abs()<1000)
    df['date_key']=datekey(df.scheduled_datetime)
    df['scheduled_time_key']=timekey(df.scheduled_datetime)
    df['delivered_time_key']=timekey(df.delivered_datetime)
    df['has_signature']=df.recipient_signature
    df['is_damaged']=pd.Series(pd.NA,index=df.index,dtype='boolean') # no existe dato en origen
    df['etl_batch_id']=int(time.time())
    return df,rejected

class FleetLogixETL:
    def __init__(self): self.pg_conn=None; self.sf_conn=None
    def connect_databases(self):
        self.pg_conn=psycopg2.connect(**POSTGRES_CONFIG)
        self.pg_conn.set_session(readonly=True, isolation_level='REPEATABLE READ')
        self.sf_conn=snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    def pg(self,sql,params=None):
        with self.pg_conn.cursor() as c:
            c.execute(sql,params); return pd.DataFrame(c.fetchall(),columns=[x.name for x in c.description])
    def sf(self,sql):
        with self.sf_conn.cursor() as c:
            c.execute(sql)
            return c.fetch_pandas_all() if c.description and c.description[0][0].upper() not in ('STATUS','NUMBER OF ROWS INSERTED','NUMBER OF ROWS UPDATED','NUMBER OF ROWS DELETED') and sql.lstrip().upper().startswith('SELECT') else None
    def extract_daily_data(self, historical=False, day=None):
        if historical: return self.pg(SQL_EXTRACT)
        day=day or datetime.now(TZ).date()-timedelta(days=1)
        return self.pg(SQL_EXTRACT+' AND d.delivered_datetime >= %s AND d.delivered_datetime < %s',(day,day+timedelta(days=1)))
    def stage(self,name,df,ddl):
        self.sf(f'CREATE OR REPLACE TEMP TABLE {name} ({ddl})')
        if len(df):
            data=df.copy(); data.columns=data.columns.str.upper()
            ok,_,rows,_=write_pandas(self.sf_conn,data,name.upper(),chunk_size=50000,quote_identifiers=False)
            if not ok or rows!=len(df): raise RuntimeError(f'Carga temporal incompleta: {name}')
    def merge(self,target,stage,keys,cols,update=True):
        on=' AND '.join(f't.{k}=s.{k}' for k in keys)
        nonkeys=[x for x in cols if x not in keys]
        matched=(' WHEN MATCHED THEN UPDATE SET '+','.join(f't.{x}=s.{x}' for x in nonkeys)) if update and nonkeys else ''
        self.sf(f'MERGE INTO {target} t USING {stage} s ON {on}'+matched+' WHEN NOT MATCHED THEN INSERT ('+','.join(cols)+') VALUES ('+','.join('s.'+x for x in cols)+')')
    def scd(self,entity,attrs):
        target='dim_'+entity; stage='tmp_'+entity; key=entity+'_key'; natural=entity+'_id'
        changed=' OR '.join(f't.{a} IS DISTINCT FROM s.{a}' for a in attrs)
        cols=[natural]+attrs
        self.sf(f'UPDATE {target} t SET valid_to=s.observed_date,is_current=FALSE FROM {stage} s WHERE t.{natural}=s.{natural} AND t.is_current AND t.valid_from<s.observed_date AND ({changed})')
        self.sf(f'UPDATE {target} t SET '+','.join(f'{a}=s.{a}' for a in attrs)+f' FROM {stage} s WHERE t.{natural}=s.{natural} AND t.is_current AND t.valid_from=s.observed_date AND ({changed})')
        self.sf(f"INSERT INTO {target} ({key},"+','.join(cols)+",valid_from,valid_to,is_current) SELECT base.mx+ROW_NUMBER() OVER(ORDER BY s."+natural+"),"+','.join('s.'+a for a in cols)+f",IFF(EXISTS(SELECT 1 FROM {target} h WHERE h.{natural}=s.{natural}),s.observed_date,s.initial_date),NULL,TRUE FROM {stage} s CROSS JOIN (SELECT COALESCE(MAX({key}),0) mx FROM {target}) base WHERE NOT EXISTS(SELECT 1 FROM {target} t WHERE t.{natural}=s.{natural} AND t.is_current)")
    def close_connections(self):
        if self.pg_conn: self.pg_conn.close()
        if self.sf_conn: self.sf_conn.close()
    def prepare_dimensions(self,df):
        today=datetime.now(TZ).date()
        bounds=self.pg('SELECT MIN(scheduled_datetime)::date AS first_day, MAX(scheduled_datetime)::date AS last_day FROM deliveries').iloc[0]
        start=bounds.first_day or today; end=max(bounds.last_day or today,today)
        days=pd.date_range(start,end,freq='D'); co=holidays.country_holidays('CO',years=range(start.year,end.year+1))
        dates=pd.DataFrame([dict(date_key=int(d.strftime('%Y%m%d')),full_date=d.date(),day_of_week=d.isoweekday(),day_name=d.day_name(),day_of_month=d.day,day_of_year=d.dayofyear,week_of_year=d.isocalendar().week,month_num=d.month,month_name=d.month_name(),quarter=d.quarter,year=d.year,is_weekend=d.dayofweek>=5,is_holiday=d.date() in co,holiday_name=co.get(d.date()),fiscal_quarter=d.quarter,fiscal_year=d.year) for d in days])
        dates.holiday_name=dates.holiday_name.map(lambda x: x[:50] if isinstance(x,str) else None)
        times=pd.DataFrame([dict(time_key=h*100+m,hour=h,minute=m,second=0,time_of_day=['Madrugada','Mañana','Tarde','Noche'][h//6],hour_24=f'{h:02}:{m:02}',hour_12=f'{h%12 or 12:02}:{m:02} {"AM" if h<12 else "PM"}',am_pm='AM' if h<12 else 'PM',is_business_hour=8<=h<18,shift=f'Turno {h//8+1}') for h in range(24) for m in range(60)])
        vehicles=self.pg("SELECT v.vehicle_id,v.license_plate,v.vehicle_type,v.capacity_kg,v.fuel_type,v.acquisition_date,v.status,(SELECT MAX(m.maintenance_date) FROM maintenance m WHERE m.vehicle_id=v.vehicle_id) last_maintenance_date FROM vehicles v")
        drivers=self.pg("SELECT driver_id,employee_code,CONCAT_WS(' ',first_name,last_name) full_name,license_number,license_expiry,phone,hire_date,status FROM drivers")
        routes=self.pg('SELECT route_id,route_code,origin_city,destination_city,distance_km,estimated_duration_hours,toll_cost FROM routes')
        routes['route_key']=routes.route_id; routes['route_type']=np.where(routes.origin_city==routes.destination_city,'Urbana','Interurbana'); routes['difficulty_level']=None
        for frame,col,out in [(vehicles,'acquisition_date','age_months'),(drivers,'hire_date','experience_months')]:
            frame[out]=frame[col].map(lambda d: max(0,(today.year-d.year)*12+today.month-d.month) if pd.notna(d) else None)
            frame['observed_date']=today; frame['initial_date']=start
        drivers['performance_category']=None
        customers=self.pg("SELECT d.customer_name,r.destination_city city,MIN(d.scheduled_datetime)::date first_delivery_date,COUNT(*) total_deliveries FROM deliveries d JOIN trips t USING(trip_id) JOIN routes r USING(route_id) WHERE d.delivery_status='delivered' AND d.delivered_datetime IS NOT NULL AND d.customer_name IS NOT NULL GROUP BY 1,2")
        customers['customer_type']='Individual'; customers['customer_category']='Regular'
        frames={'date':dates,'time':times,'vehicle':vehicles,'driver':drivers,'route':routes,'customer':customers}
        for entity,frame in frames.items():
            # Match physical types to the existing dimensional model.
            self.sf(f'CREATE OR REPLACE TEMP TABLE tmp_{entity} AS SELECT * FROM dim_{entity} WHERE 1=0')
            if entity in ('vehicle','driver'): self.sf(f'ALTER TABLE tmp_{entity} ADD COLUMN observed_date DATE, initial_date DATE')
            # LIKE inherits NOT NULL on natural fields only; omit identity/keys where generated later.
            if entity in ('vehicle','driver','customer'):
                frame=frame.copy(); frame[entity+'_key']=np.arange(1,len(frame)+1)
            if len(frame):
                frame=frame.copy(); frame.columns=frame.columns.str.upper()
                ok,_,rows,_=write_pandas(self.sf_conn,frame,'TMP_'+entity.upper(),chunk_size=50000,quote_identifiers=False)
                if not ok or rows!=len(frame): raise RuntimeError('Staging dimensional incompleto: '+entity)
        return frames
    def load_dimensions(self,frames):
        for entity in ('date','time','route'):
            self.merge('dim_'+entity,'tmp_'+entity,[entity+'_key'],list(frames[entity].columns))
        for entity in ('vehicle','driver'):
            attrs=[c for c in frames[entity].columns if c not in (entity+'_id','observed_date','initial_date','age_months','experience_months')]
            self.scd(entity,attrs)
            age='age_months' if entity=='vehicle' else 'experience_months'
            self.sf(f'UPDATE dim_{entity} t SET {age}=s.{age} FROM tmp_{entity} s WHERE t.{entity}_id=s.{entity}_id AND t.is_current')
        self.sf("""MERGE INTO dim_customer t USING (SELECT s.*, b.mx+ROW_NUMBER() OVER(ORDER BY customer_name,city) new_key FROM tmp_customer s CROSS JOIN (SELECT COALESCE(MAX(customer_key),0) mx FROM dim_customer) b) s ON t.customer_name=s.customer_name AND t.city=s.city
        WHEN MATCHED THEN UPDATE SET first_delivery_date=s.first_delivery_date,total_deliveries=s.total_deliveries
        WHEN NOT MATCHED THEN INSERT(customer_key,customer_name,customer_type,city,first_delivery_date,total_deliveries,customer_category) VALUES(s.new_key,s.customer_name,s.customer_type,s.city,s.first_delivery_date,s.total_deliveries,s.customer_category)""")
    def load_facts(self,df):
        cols=['date_key','scheduled_time_key','delivered_time_key','vehicle_key','driver_key','route_key','customer_key','delivery_id','trip_id','tracking_number','package_weight_kg','distance_km','fuel_consumed_liters','delivery_time_minutes','delay_minutes','deliveries_per_hour','fuel_efficiency_km_per_liter','cost_per_delivery','revenue_per_delivery','is_on_time','is_damaged','has_signature','delivery_status','etl_batch_id']
        select=[('v.vehicle_key' if c=='vehicle_key' else 'dr.driver_key' if c=='driver_key' else 'r.route_key' if c=='route_key' else 'cu.customer_key' if c=='customer_key' else 's.'+c) for c in cols]
        joins=""" FROM tmp_fact s JOIN dim_date dt ON dt.date_key=s.date_key
        JOIN dim_time st ON st.time_key=s.scheduled_time_key JOIN dim_time et ON et.time_key=s.delivered_time_key
        JOIN dim_vehicle v ON v.vehicle_id=s.vehicle_id AND dt.full_date>=v.valid_from AND (v.valid_to IS NULL OR dt.full_date<v.valid_to)
        JOIN dim_driver dr ON dr.driver_id=s.driver_id AND dt.full_date>=dr.valid_from AND (dr.valid_to IS NULL OR dt.full_date<dr.valid_to)
        JOIN dim_route r ON r.route_id=s.route_id JOIN dim_customer cu ON cu.customer_name=s.customer_name AND cu.city=s.destination_city"""
        check=self.sf('SELECT COUNT(*) N,COUNT(DISTINCT s.delivery_id) U'+joins).iloc[0]
        if int(check.N)!=len(df) or int(check.U)!=len(df): raise RuntimeError('Claves dimensionales faltantes o ambiguas; se cancela el lote')
        upd=','.join(f't.{c}=s.{c}' for c in cols if c!='delivery_id')
        self.sf('MERGE INTO fact_deliveries t USING (SELECT '+','.join(select)+joins+') s ON t.delivery_id=s.delivery_id WHEN MATCHED THEN UPDATE SET '+upd+',t.etl_timestamp=CURRENT_TIMESTAMP() WHEN NOT MATCHED THEN INSERT('+','.join(cols)+') VALUES('+','.join('s.'+c for c in cols)+')')
    def run_etl(self,historical=False,day=None):
        try:
            self.connect_databases()
            raw=self.extract_daily_data(historical,day)
            if raw.empty: print('Sin entregas para el período'); return
            df,rejected=transform_data(raw)
            if len(rejected): rejected.to_csv('etl_rejected.csv',index=False)
            if df.empty: raise RuntimeError('No quedaron registros válidos')
            if df.delivery_id.duplicated().any(): raise RuntimeError('delivery_id duplicado en origen')
            self.sf(SQL_TOTALS_CREATE)
            frames=self.prepare_dimensions(df)
            stage_cols=['date_key','scheduled_time_key','delivered_time_key','vehicle_id','driver_id','route_id','customer_name','destination_city','delivery_id','trip_id','tracking_number','package_weight_kg','distance_km','fuel_consumed_liters','delivery_time_minutes','delay_minutes','deliveries_per_hour','fuel_efficiency_km_per_liter','cost_per_delivery','revenue_per_delivery','is_on_time','is_damaged','has_signature','delivery_status','etl_batch_id']
            strings={'customer_name','destination_city','tracking_number','delivery_status'}; bools={'is_on_time','is_damaged','has_signature'}
            decimals={'package_weight_kg','distance_km','fuel_consumed_liters','deliveries_per_hour','fuel_efficiency_km_per_liter','cost_per_delivery','revenue_per_delivery'}
            ddl=','.join(c+' '+('VARCHAR' if c in strings else 'BOOLEAN' if c in bools else 'NUMBER(18,2)' if c in decimals else 'INT') for c in stage_cols)
            self.stage('TMP_FACT',df[stage_cols],ddl)
            self.sf('BEGIN')
            self.load_dimensions(frames)
            self.load_facts(df)
            self.sf(SQL_TOTALS_MERGE)
            check=self.sf('SELECT COUNT(*) N,COUNT(DISTINCT delivery_id) U FROM fact_deliveries').iloc[0]
            if check.N!=check.U: raise RuntimeError('Hechos duplicados; se cancela el lote')
            self.sf_conn.commit()
            print(dict(extracted=len(raw),transformed=len(df),rejected=len(rejected),processed=len(df),total_facts=int(check.N)))
        except Exception:
            if self.sf_conn: self.sf_conn.rollback()
            logging.exception('ETL fallido'); raise
        finally: self.close_connections()

# %% [markdown]
# ## Ejecución segura desde terminal
# Por defecto carga el día anterior en America/Bogota.
# Ejemplos:
#   python 05_etl_pipeline.py
#   python 05_etl_pipeline.py --day 2026-11-04
#   python 05_etl_pipeline.py --historical
# La carga histórica se ejecuta SOLO con --historical.

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='FleetLogix: ETL PostgreSQL → Snowflake')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--historical', action='store_true', help='Carga histórica completa')
    mode.add_argument('--day', type=date.fromisoformat, metavar='AAAA-MM-DD', help='Carga incremental de una fecha')
    args = parser.parse_args()
    FleetLogixETL().run_etl(historical=args.historical, day=args.day)
