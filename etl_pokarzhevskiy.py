import pandahouse
import pandas as pd

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Параметры соединения

connection = {'host': 'https://clickhouse.lab.karpov.courses',
              'password': 'dpo_python_2020',
              'user': 'student',
              'database': 'simulator'
                }


connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                   'password': '656e2b0c9c',
                   'user': 'student-rw',
                   'database': 'test'
                    }

# Дефолтные параметры для тасков
default_args = {
    'owner': 'p-pokarzhevskiy',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 1)}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

query_mess = """
        SELECT
        CASE
        WHEN l.user_id = 0 THEN r.reciever_id
        ELSE l.user_id END AS user_id,
        l.messages_sent,
        r.messages_received, 
        l.users_sent, 
        r.users_received
        FROM                    
        (SELECT user_id, 
        count(distinct reciever_id) AS users_sent,
        count() AS messages_sent                    
        FROM simulator_20230120.message_actions
        WHERE toDate(time) = today() - 1
        GROUP BY user_id) AS l
        FULL OUTER JOIN
        (SELECT 
        reciever_id, 
        count(distinct user_id) AS users_received,
        count() AS messages_received                    
        FROM simulator_20230120.message_actions
        WHERE toDate(time) = today() - 1
        GROUP BY reciever_id) AS r
        ON l.user_id = r.reciever_id
        """

# Запуск DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def etl_pokarzhevskiy():
    """ DAG получает дефолтные параметры и время регулярного запуска через декоратор dag и передаёт их на таски"""

    @task()
    def extract_users_data():
        # Достаём данные по полу, возрасту и операционной системе каждого пользователя ленты и мессенжера
        
        query_users_data = '''
                SELECT DISTINCT user_id,
                    CASE 
                      WHEN age >= 60 THEN 'old' WHEN age >= 20 AND age < 60 THEN 'adult' ELSE 'young' END AS age,
                    CASE gender
                      WHEN 0 THEN 'female' ELSE 'male' END AS gender,
                  os
                FROM 
                  simulator_20230120.feed_actions
                  
                UNION ALL
                SELECT DISTINCT user_id,
                    CASE 
                      WHEN age >= 60 THEN 'old' WHEN age >= 20 AND age < 60 THEN 'adult' ELSE 'young' END AS age,
                    CASE gender 
                      WHEN 0 THEN 'female' ELSE 'male' END AS gender,
                  os
                FROM 
                  simulator_20230120.message_actions
                WHERE user_id NOT IN (SELECT DISTINCT user_id FROM simulator_20230120.feed_actions)
            '''
        df = pandahouse.read_clickhouse(query=query_users_data, connection=connection)
        return df
        
    @task()
    def extract_act():
        #Таск для получения данных по лайкам и просмотрам из базы feed_actions без колонок по возрасту, полу и операционной системы
                
        query_act = """
        SELECT user_id,
        countIf(action = 'like') as likes,        
        countIf(action = 'view') as views                
        FROM simulator_20230120.feed_actions
        WHERE toDate(time) = today() - 1
        GROUP BY user_id 
        """        
        df = pandahouse.read_clickhouse(query=query_act, connection=connection)
        return df
    
    
    @task()
    def extract_mess():
        
        df = pandahouse.read_clickhouse(query=query_mess, connection=connection)
        return df

    
    @task()
    def dataframes_merge(df_cube_act, df_cube_mess):
        
        # Собираем две базы в одну, заменяем отсутствующие данные на 0 и добавляем колонку с датой
        
        df = pd.merge(left=df_cube_act, right=df_cube_mess, how='outer', left_on='user_id', right_on='user_id')        
        df.fillna(0, inplace=True)                                               # Заменяем несуществующие значения на нули
        df['event_date'] = datetime.date(datetime.today()) - timedelta(days=1)   # Добавляем колонку с датой измерений
        return df
        
    
    @task()
    def add_users_data(df_cube, df_users_data):
        # Добавляем данные пользователя и собираем с финальную таблицу
        
        df = pd.merge(left=df_cube, right=df_users_data, how='left', left_on='user_id', right_on='user_id')        
        return df
    
    @task()
    def gender_transform(data):
        
        df_gender = data.groupby(['gender', 'event_date']).sum().reset_index()
        df_gender['dimension'] = 'gender'
        df_gender.rename(columns={'gender':'dimension_value'}, inplace=True)
        df_gender = df_gender[['event_date', 'dimension', 'dimension_value', 'views',\
                         'likes', 'messages_received', 'messages_sent', 'users_received',\
                         'users_sent']]
        return df_gender 
       
    @task()
    def os_transform(data):        
        
        df_os = data.pivot_table(values=['views', 'likes', 'messages_received', 'messages_sent',\
                                           'users_received', 'users_sent'],\
                                   index=['event_date', 'os'],\
                                   aggfunc='sum').reset_index()
        df_os['dimension'] = 'os'
        df_os.rename(columns={'os':'dimension_value'}, inplace=True)
        df_os = df_os[['event_date', 'dimension', 'dimension_value', 'views',\
                               'likes', 'messages_received', 'messages_sent', 'users_received',\
                               'users_sent']]
        return df_os
    
    
    @task()
    def age_transform(data):        
        
        df_age = data.pivot_table(values=['views', 'likes', 'messages_received', 'messages_sent',\
                                           'users_received', 'users_sent'],\
                                   index=['event_date', 'age'],\
                                   aggfunc='sum').reset_index()
        df_age['dimension'] = 'age'
        df_age.rename(columns={'age':'dimension_value'}, inplace=True)
        df_age = df_age[['event_date', 'dimension', 'dimension_value', 'views',\
                               'likes', 'messages_received', 'messages_sent', 'users_received',\
                               'users_sent']]
        return df_age
    
    @task()
    def concat_data(gender_slice, age_slice, os_slice) :
        df = pd.concat([gender_slice, age_slice, os_slice], axis=0).reset_index(drop=True)
        return df
  
    @task()
    def load(df):
        q = '''
            CREATE TABLE IF NOT EXISTS test.Pokarzhevskiy_etl 
            (   event_date String,
                dimension String,
                dimension_value String,
                views Float64,
                likes Float64,
                messages_received Float64,
                messages_sent Float64,
                users_received Float64,
                users_sent Float64
            ) ENGINE = MergeTree()
            
            '''
        pandahouse.execute(query=q, connection=connection_test)
        pandahouse.to_clickhouse(df=df, table='Pokarzhevskiy_etl', index=False, connection=connection_test)
        
    df_users_data = extract_users_data() #Инициализируем получение данных по всем пользователям в двух базах
    df_cube_feed = extract_act()         #Инициализируем получение данных по лайкам и просмотрам из базы feed_actions
    df_cube_mess = extract_mess()        #Инициализируем получение данных по отправителям, получателям и т.п.из базы  message_actions
    
    df_merged = dataframes_merge(df_cube_feed, df_cube_mess)  # Соединяем две таблицы 
    df_total = add_users_data(df_merged,df_users_data )       # Добавляем данные пользователя в объединенную таблицу
    
    gender_slice = gender_transform(df_total) #Срез по полу пользователя
    age_slice = age_transform(df_total)       #Срез по возрасту пользователя
    os_slice = os_transform(df_total)         #Срез по операционной системе пользователя
    
    concat_data = concat_data(gender_slice, age_slice, os_slice) # Собираем финальную таблицу за день наблюдений
    
    load(concat_data)  #Загружаем собранную таблицу
                       
    # df_cube_source = transfrom_source(df_cube)
    # df_cube_country = transfrom_countries(df_cube)
    # load(df_cube_source, df_cube_country)

dag_pokarzhevskiy = etl_pokarzhevskiy()
