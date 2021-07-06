import time
import pymysql
from sqlalchemy import create_engine
import pickle
import pandas as pd
from Cafe24product_fieldname import final_field
import sys, traceback
import re


def datalist(schema, table, query):
    # user, password는 odbc 계정정보 넣기, 데이터베이스는 스키마명 입력

    conn = pymysql.connect(host='ecommerce-part1.cluster-cg6g43iitkzh.ap-northeast-2.rds.amazonaws.com',
                           port=3306,
                           user='echouser',
                           passwd='Echomarketing123!',
                           db=schema,
                           charset='utf8')
    query = 'select * from `' + schema + '`.`' + table + "`" + query
    print('select * from `' + schema + '`.`' + table)
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        result = cursor.fetchall()  # 실행 완료된 데이터를 다운받는 개념
        df = pd.DataFrame(result)

        return df

    except Exception as e:
        print(e)

    finally:
        cursor.close()
        conn.close()


#과거데이터 가져오기
def datalist_past(schema, table, cols, query):
    # user, password는 odbc 계정정보 넣기, 데이터베이스는 스키마명 입력

    conn2 = pymysql.connect(host='ecommerce-part1.cluster-cg6g43iitkzh.ap-northeast-2.rds.amazonaws.com',
                            port=3306,
                            user='echouser',
                            passwd='Echomarketing123!',
                            db=schema,
                            charset='utf8')
    query = 'select ' + cols + ' from `' + schema + '`.`' + table + "`" + query
    print('select some columns from `' + schema + '`.`' + table)
    cursor2 = conn2.cursor()

    try:

        cursor2.execute(query)
        result = cursor2.fetchall()  # 실행 완료된 데이터를 다운받는 개념
        df = pd.DataFrame(result)
        df.columns = cols.split(', ')

        return df

    except Exception as e:
        print(e)

    finally:
        cursor2.close()
        conn2.close()


def del_data(schema, table, del_query):
    conn = pymysql.connect(host='ecommerce-part1.cluster-cg6g43iitkzh.ap-northeast-2.rds.amazonaws.com', port=3306,
                           user='echouser', passwd='Echomarketing123!', db=schema, charset='utf8')
    query = 'DELETE FROM `' + schema + '`.`' + table + '` ' + del_query

    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        print(schema, table, 'Deleted')
        time.sleep(1)

    except Exception as e:
        print(e)

    finally:
        conn.close()


# def insert_data(data, schema, table):
#     engine = create_engine(
#         "mysql+pymysql://echouser:" + "Echomarketing123!" + "@ecommerce-part1.cluster-cg6g43iitkzh.ap-northeast-2.rds.amazonaws.com:3306/" + schema + "?charset=utf8mb4",
#         encoding='utf8')
#
#     with engine.connect() as conn:
#         try:
#             data.to_sql(name=table, con=engine, if_exists='append', index=False, chunksize=200000)
#             print(schema, table, ' updated')
#         except Exception as e:
#             print(e)


def insert_data(data, schema, table):
    engine = create_engine(
        "mysql+pymysql://echouser:" + "Echomarketing123!" + "@ecommerce-part1.cluster-cg6g43iitkzh.ap-northeast-2.rds.amazonaws.com:3306/" + schema + "?charset=utf8mb4",
        encoding='utf8')

    with engine.connect() as conn:
        try:
            data.to_sql(name=table, con=engine, if_exists='append', index=False, chunksize=200000)
            print(schema, table, ' updated')
        except:
            errmsg = traceback.format_exc()
            errmsg = re.sub('\".*\"','', errmsg)
            print(errmsg)

            for i in range(1, 4):  # 3번 재시도
                try:
                    print("retry :", i)
                    data.to_sql(name=table, con=engine, if_exists='append', index=False, chunksize=200000)
                except:
                    pass
                else:
                    print(schema, table, ' updated')
                    break
                print(schema, table, 'insert failed')


if __name__ == "__main__":

    with open('df_번호수정.pickle', 'rb') as f:
        df = pickle.load(f)
    final_df = df[final_field()]
    print(final_df.shape)

    insert_data(final_df, 'yurica', 'repurchase_kl')

    # with open('sku.pickle', 'rb') as f:
    #     SKU_df = pickle.load(f)
    #
    # insert_data(SKU_df, 'yurica', 'repurchasesku_kl')