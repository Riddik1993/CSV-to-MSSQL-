import sqlalchemy as sa
import pandas as pd
import datetime


# функция получения текущ. даты и времени
def get_time():
    now = datetime.datetime.today()
    return now.strftime("%Y-%m-%d %H:%M:%S")


# функция для логирования
def write_log(*args):
    str_args = [str(a) for a in args]
    with open(r'C:\Users\SNFrolov\Desktop\Обучение\15-12-2021_09-06-10\etl_log.txt', 'a', encoding='utf-8') as f:
        f.write(' '.join(str_args) + '\n')


write_log('\n', '\r', get_time(), 'Процесс загрузки запущен')

# подключаемся к csv-источнику
df = pd.read_csv(r'C:\Users\SNFrolov\Desktop\Обучение\15-12-2021_09-06-10\cf_test_dataset (2).csv',
                 sep=';',
                 parse_dates=['service_start_date', 'service_end_date'],
                 dayfirst=True,
                 # index_col=r'server_order_id'
                 )

write_log(get_time(), 'Чтение из источника выполнено, количество строк в csv-источнике: ', df.shape[0])

# подключаемся к ms_sql базе
engine = sa.create_engine('mssql://ms-cpd003/FROLOV?driver=SQL+Server+Native+Client+11.0')
write_log(get_time(), 'Подключение к ms-sql выполнено')

# получаем списки существующих в БД записей
df_ex_client = pd.read_sql_table('Client', engine, columns=['ID'])
df_ex_service = pd.read_sql_table('Service', engine, columns=['ID'])
df_ex_order = pd.read_sql_table('Order_', engine, columns=['ID'])

conv_dict = {
    'user_id': 'ID',
    'user_name': 'name',
    'user_surname': 'surname',
    'service_id': 'ID'
}

# отбираем новых клиентов для записи
df_cl_raw = df[['user_id', 'user_name', 'user_surname']].rename(columns=conv_dict)
df_cl = df_cl_raw[~df_cl_raw.ID.isin(df_ex_client.ID)].drop_duplicates()
write_log(get_time(), 'Клиенты для записи отобраны, шт.: ', df_cl.shape[0])

# отбираем новые сервисы для записи
df_serv_raw = df[['service_id', 'server_configuration']].rename(columns=conv_dict)
df_serv = df_serv_raw[~df_serv_raw.ID.isin(df_ex_service.ID)].drop_duplicates()
write_log(get_time(), 'Сервисы для записи отобраны, шт.: ', df_serv.shape[0])

# отбираем новые заказы для записи
conv_dict_order = {
    'server_order_id': 'ID',
    'user_id': 'client_id'
}

df_order_raw = df[['server_order_id', 'service_id', 'user_id',
                   'service_start_date', 'service_end_date', 'price']].rename(columns=conv_dict_order)
df_order = df_order_raw[~df_order_raw.ID.isin(df_ex_order.ID)]
write_log(get_time(), 'Заказы для записи отобраны, шт.: ', df_order.shape[0])

# пишем в mssql
if len(df_cl) != 0:
    df_cl.to_sql('Client',
                 con=engine,
                 if_exists='append',
                 index=False)
    write_log(get_time(), 'Записаны строки в таблицу Client')

if len(df_serv) != 0:
    df_serv.to_sql('Service',
                   con=engine,
                   if_exists='append',
                   index=False)
    write_log(get_time(), 'Записаны строки в таблицу Service')

if len(df_order) != 0:
    df_order.to_sql('Order_',
                    con=engine,
                    if_exists='append',
                    index=False)
    write_log(get_time(), 'Записаны строки в таблицу Order_')