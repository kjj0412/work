import datetime
import numpy as np
import pandas as pd
from Cafe24product_fieldname import final_field, field_sorting
import glob
from Data_Upload import datalist, datalist_past, insert_data, del_data
import Data_handler

'''
Interval_Days_SKU_14도 맞게 들어가나 확인해야함
정합성체크 완료되면 핑거수트 제작//
'''

def Broad_RePurchase(df, Brand):
    """
    * First_Purchase_Date : 해당 유저의 첫 구매 일자
        - fake_Order_df : 이번기간 내 첫구매일자를 가져온 뒤 DB의 첫구매일자와 비교

    * Broad_Repurchase_User : 확장재구매 유저 여부 (Y/N)
        - fake_Order_df : Item 기준으로 이번기간 내 확장재구매 유저 여부 판단한 뒤 DB 데이터와 비교
        - Order_df : Item_df에서 Phone_Number별 구매한 Item 리스트를 가져와 과거 데이터와 결합

    * Sequence : 유저(전화번호)별 주문 순서 (날짜/시간 순 오름차순)
        - Order_df : 과거 구매회차 정보를 가져와 이번 기간 내 구매회차에 더함

    * Sequence_Broad : 확장재구매 고려한 구매회차

    * Interval_Days : Phone_Number별로 계산
        - 과거 구매정보 있고 이번 기간 내 구매가 1회차이면 구매일 - DB의 마지막 구매일
        - 과거 구매정보 없는데 이번 기간 내 구매가 1회차면 ""
        - 과거 구매정보 없는데 이번 기간 내 구매가 2회차 이상이면 구매일 - 이번기간 내 이전 구매일

    * Cohort_Days : 재구매 유저의 구매일자 - 첫구매일자
    """

    df = df[['Phone_Number', 'Date_', 'Item', 'Broad_Repurchase_User_DB', 'Last_OrderDate', 'First_Purchase_Date_DB', 'Last_Sequence']]
    df = df.sort_values(['Phone_Number', 'Date_'], ascending=(True, True))

    Item_df = df.groupby(['Phone_Number', 'Date_'])['Item'].apply(list).reset_index()
    Order_df = df[['Phone_Number', 'Date_', 'Broad_Repurchase_User_DB', 'Last_OrderDate', 'First_Purchase_Date_DB', 'Last_Sequence']].drop_duplicates()
    Order_df = pd.merge(left=Order_df, right=Item_df, on=['Phone_Number', 'Date_'], how='left')

    Order_df['indexNum'] = Order_df.index
    Order_df['Sequence_new'] = Order_df.groupby('Phone_Number')['indexNum'].rank()
    Order_df['Sequence'] = Order_df.apply(lambda x : x['Sequence_new'] + x['Last_Sequence'] if x['Last_Sequence'] > 0
                                                 else x['Sequence_new'], axis=1)
    Order_df = Order_df.drop(columns='indexNum')

    if Brand == '클럭':
        fake_Order_df = fakeOrderdf_kl(Order_df)
    elif Brand == '몽제':
        fake_Order_df = fakeOrderdf_mz(Order_df)
    elif Brand == '유리카' or Brand == '티타드':
        fake_Order_df = fakeOrderdf_y_tt(Order_df)
    Order_df = pd.merge(left=Order_df, right=fake_Order_df, on=['Phone_Number'], how='left')

    Order_df['First_Purchase_Date'] = Order_df.apply(lambda x : x['First_Purchase_Date_DB'] if x['Last_Sequence'] > 0 else x['First_Purchase_Date_new'], axis=1)
    Order_df['Broad_Repurchase_User'] = Order_df.apply(lambda x : x['Broad_Repurchase_User_DB'] if x['Last_Sequence'] > 0 else x['Broad_Repurchase_User_new'], axis=1)

    Order_df['Date_shift'] = Order_df['Date_'].shift(1)
    Order_df['Interval_Days'] = Order_df.apply(lambda x :
                x['Date_'] - x['Last_OrderDate'] if (x['Last_Sequence'] > 0) and (x['Sequence_new'] == 1)
          else ("" if x['Sequence'] == 1 else (x['Date_'] - x['Date_shift'])), axis=1)
    Order_df = Order_df.astype({'Interval_Days' : str})
    Order_df['Interval_Days'] = Order_df['Interval_Days'].str.replace(' days.*', '', regex=True)
    Order_df['Interval_Days'] = Order_df['Interval_Days'].apply(lambda x : x.replace('NaT', ''))

    Order_df['Sequence_Broad'] = Order_df.apply(
        lambda x: x['Sequence'] + 1 if (x['Broad_Repurchase_User'] == "Y")
        else ("" if (x['Broad_Repurchase_User'] == "") else x['Sequence']), axis=1)

    Order_df['Cohort_Days'] = Order_df.apply(lambda x: str(x['Date_'] - x['First_Purchase_Date'])
                                                if (x['Sequence'] > 1) else "", axis=1)
    Order_df['Cohort_Days'] = Order_df['Cohort_Days'].replace(' .*', '', regex=True)
    Order_df = Order_df[['Phone_Number', 'Date_', 'First_Purchase_Date', 'Broad_Repurchase_User', 'Sequence', 'Sequence_new', 'Sequence_Broad', 'Interval_Days', 'Cohort_Days']]

    return Order_df


def fakeOrderdf_mz(Order_df):
    """
    확장재구매 기준
    1. (매트 포함) and (베개 미포함) and (베개커버 포함) 건을 재구매로 처리
    2. (베개 포함) and (매트 미포함) and (겉커버 or 겉커버V 포함) 건을  재구매로 처리
    3. 매트,베개 미포함건(=간단한 신규구매)을 재구매로 처리
    """
    fake_Order_df = Order_df[Order_df['Sequence'] == 1]
    fake_Order_df['Broad_Repurchase_User_new'] = fake_Order_df.apply(lambda x:
        'Y' if (('매트' in x['Item']) and ('베개커버' in x['Item']) and not ('베개' in x['Item'])) else
        ('Y' if (('베개' in x['Item']) and ('겉커버' in x['Item'] or '겉커버V' in x['Item']) and not ('매트' in x['Item'])) else
        ('N' if ((x['Item'] == ['-']) or ('@' in x['Item']) or ('대량구매' in x['Item']) or (np.nan in x['Item'])) else
        ('Y' if not (('매트' in x['Item']) or ('베개' in x['Item'])) else "N"))), axis=1)
    fake_Order_df['First_Purchase_Date_new'] = fake_Order_df['Date_']
    fake_Order_df = fake_Order_df[['Phone_Number', 'First_Purchase_Date_new', 'Broad_Repurchase_User_new']]

    return fake_Order_df


def fakeOrderdf_kl(Order_df):
    """
    확장재구매 기준
    1. (미니 or 무릎 포함) and (필터 포함) and (공청기 미포함)
    2. (공청기 포함) and (패드 or 리모컨 or 겔패치 or 본체 포함) and (미니 and 무릎 미포함)
    3. 미니, 무릎, 공청기 미포함건
    """
    fake_Order_df = Order_df[Order_df['Sequence'] == 1]
    fake_Order_df['Broad_Repurchase_User_new'] = fake_Order_df.apply(lambda x: 'Y'
    if (('미니' in x['Item'] or '무릎' in x['Item'])
        and ('필터' in x['Item'])
        and not ('공청기' in x['Item']))
    else ('Y' if (('공청기' in x['Item'])
                  and ('패드' in x['Item'] or '리모컨' in x['Item'] or '겔패치' in x['Item'] or '본체' in x['Item'])
                  and not ('미니' in x['Item'] or '무릎' in x['Item']))
    else ('N' if ((x['Item'] == ['-']) or ('@' in x['Item']) or ('대량구매' in x['Item']) or (np.nan in x['Item']))
    else ('Y' if not ('미니' in x['Item'] or '무릎' in x['Item'] or '공청기' in x['Item'])
                else "N"))), axis=1)
    fake_Order_df['First_Purchase_Date_new'] = fake_Order_df['Date_']
    fake_Order_df = fake_Order_df[['Phone_Number', 'First_Purchase_Date_new', 'Broad_Repurchase_User_new']]

    return fake_Order_df


def fakeOrderdf_y_tt(Order_df):
    """ 유리카, 티타드는 확장재구매 계산 안함 """
    fake_Order_df = Order_df[Order_df['Sequence'] == 1]
    fake_Order_df['First_Purchase_Date_new'] = fake_Order_df['Date_']
    fake_Order_df['Broad_Repurchase_User_new'] = ""
    fake_Order_df = fake_Order_df[['Phone_Number', 'First_Purchase_Date_new', 'Broad_Repurchase_User_new']]

    return fake_Order_df


def mainData(df, Option_df, Brand, Brd, start_date, report_date, update_all):
    """
    * 이번기간 df에 있는 Phone_Number들의 과거 데이터를 db에서 가져오기 (DB_past_df -> get_past_sequence 함수)
      ㄴ Where Date_ < "{}" and Phone_Number in ({})
    * 확장재구매 관련 열들 계산 (Order_df)
    * Pre_Item_Option 은 과거정보 있으면 과거 데이터 Lookup, 없으면 이번기간 데이터로 넣기 (Pre_Item_Option_DB, Pre_Item_Option_New)
    * Simple 테이블 데이터 업데이트
    * 매핑누락 데이터 업데이트
    * SKU 매핑 / Pre_SKU / Sequence_SKU 계산은 Pre_Item_Option과 동일 / Cur_SKU, Option_SKU, Interval_days_SKU 계산
    """
    Phonenum ="\'" + '\',\''.join(df['Phone_Number'].tolist()) + "\'"

    # 부분 업데이트 하는경우
    if update_all == False:
        DB_past_query = 'Where Date_ < "{}" and Phone_Number in ({})'.format(start_date, Phonenum)
        DB_past_df = datalist_past('salesrp', 'tb_salesrp_sku_' + Brd, DB_past_query)
        DB_past_df[[6, 7, 8]]=DB_past_df[[6, 7, 8]].replace('', 0).astype(int)  # sequence, sequence_sku, sequence_broad 형변환
        DB_past_df = DB_past_df[DB_past_df[3] == '일반']

    # 전체 업데이트 하는경우
    elif update_all == True:
        DB_past_df = pd.DataFrame(columns=range(0,11)) # 과거데이터는 빈 데이터프레임
        DB_past_df.loc[0]=[0,'-','-','-','-','-',0,0,0,0,'-']

    df = Data_handler.get_past_sequence(df, DB_past_df) # 유저별 과거 구매정보 lookup

    Order_df = Broad_RePurchase(df, Brand)
    df = pd.merge(left=df, right=Order_df, on=['Date_', 'Phone_Number'], how='left')

    df['Date_'] = df['Date_'].dt.strftime('%Y-%m-%d')
    df['First_Purchase_Date'] = df['First_Purchase_Date'].dt.strftime('%Y-%m-%d')
    df = df.sort_values(by=['Phone_Number', 'Sequence'], ascending=(True, True))

    df = Data_handler.Pre_Item_Option(DB_past_df, df) # Pre_Item_Option

    df = Data_handler.Cur_Item_list(df) # Cur_Item_Option

    simple_df = Data_handler.simple_table(df)

    del_query = 'Where Brand="{}" and Date_ between "{}" and "{}"'.format(Brand, start_date, report_date)
    # del_data('salesrp', 'tb_salesrp_simple', del_query)
    # insert_data(simple_df, 'salesrp', 'tb_salesrp_simple')

    SKU_df = Data_handler.SKU_Mapping(df, Option_df) # SKU, Quantity_Bundle, Quantity_SKU

    NoMapping = Data_handler.MappingCheck(SKU_df, Brd)
    # del_data('salesrp', 'tb_salesrp_mapnull_' + Brd, '')
    # insert_data(NoMapping, 'salesrp', 'tb_salesrp_mapnull_' + Brd)

    SKU_df = Data_handler.Pre_SKU(DB_past_df, SKU_df) # Pre_SKU

    SKU_df = Data_handler.Cur_SKU_list(SKU_df) # Cur_SKU

    SKU_df = Data_handler.Option_SKU_list(SKU_df) # Option_SKU

    SKU_df = Data_handler.get_past_purchase_by_SKU(DB_past_df, SKU_df) # SKU별 마지막 구매날짜, 구매회차 lookup

    SKU_df = Data_handler.Sequence_SKU(SKU_df) # Sequence_SKU

    # Interval_Days_SKU
    if update_all == False:
        SKU_df = Data_handler.Interval_days_SKU_14(SKU_df)
    elif update_all == True:
        SKU_df = Data_handler.Interval_days_SKU_all(SKU_df)

    # SKU_df.to_csv(Brand + '_main_20일.csv', encoding='utf-8-sig', index=False)
    final_df = SKU_df[field_sorting(Brd)]

    return final_df


def errData(df, Option_df, Brand, Brd):

    df = Data_handler.SKU_Mapping(df, Option_df)
    df = Data_handler.Option_SKU_list(df)

    df['Date_'] = df['Date_'].dt.strftime('%Y-%m-%d')
    df['Broad_Repurchase_User'] = ""
    df['Sequence'] = ""
    df['Sequence_Broad'] = ""
    df['First_Purchase_Date'] = ""
    df['Cur_Item_Option'] = ""
    df['Pre_Item_Option'] = ""
    df['Cohort_Days'] = ""
    df['Interval_Days'] = ""
    df['Interval_Days_SKU'] = ""
    df['Pre_SKU'] = ""
    df['Cur_SKU'] = ""
    df['Sequence_SKU'] = ""

    final_df = df[field_sorting(Brd)]

    return final_df


def main(Brand, start, end, update_all):
    """
    * Start_date, end_date : 가공할 데이터의 시작일, 끝일
    * report_date : 인풋 파일 가져올 폴더 날짜
    * Brd : 브랜드별 테이블명에 들어가는 브랜드명 축약어
    * input_folder : 인풋 받아지는 ETL 폴더 내 브랜드별 폴더명

    * 매핑되는 열의 경우 매핑되지 않으면 일괄 @ 로 표시됨
    * 반품/내부번호/오류번호 주문건의 경우 재구매는 제외하고 계산하되, 판매수량은 트래킹해야 해서 필요에 따라 나눠서 가공함 (main_df, err_df)
    * SKU 매핑하며 left outer join이 되기 때문에 Quantity, Sales는 행분리된 개수를 카운팅해서 다시 나눠줌 (Quantity_divide, Sales_divide)
        >> 행분리전의 Quantity_Option 값과 행분리 후의 Quantity_divide 값의 합이 동일해야 함
    """
    start_date = (datetime.datetime.now() - datetime.timedelta(days=start)).strftime('%Y-%m-%d')
    end_date = (datetime.datetime.now() - datetime.timedelta(days=end)).strftime('%Y-%m-%d')
    report_date = (pd.to_datetime(end_date) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    if Brand == '몽제':
        Brd = "mz"
        input_folder = "mongze"
    elif Brand == "클럭":
        Brd = "kl"
        input_folder = "klugkorea"
    elif Brand == '유리카':
        Brd = "yc"
        input_folder = "yuricacosmetic"
    elif Brand == '티타드':
        Brd = "tt"
        input_folder = "titad"

    print(Brd, start_date, report_date)

    # 부분 업데이트 하는 경우
    if update_all == False:
        input_dir = "..\\..\\데일리앤코_Pentaho_리뉴얼_V2\\데일리앤코RD\\" + report_date + "\\input\\카페24\\수기_order\\" + input_folder
        file_list = [f for f in glob.glob(input_dir + '/*.csv')]

        # 인풋누락일경우 로그에 기록
        if len(file_list)<1:
            print(Brd + "RD not found :",len(file_list),"files in current folder.")
        df = Data_handler.data_input(file_list)

    # 전체 업데이트 하는 경우
    elif update_all == True:
        df = pd.read_csv(Brand+'_수기인풋전체.csv', encoding='UTF-8')
        df = df.drop_duplicates(['주문번호', '주문상품명', '상품코드', '상품옵션', '상품품목코드'], keep='last')

    df = df[df['결제일시(입금확인일)'] >= start_date]
    df = df[df['결제일시(입금확인일)'] <= end_date]

    df = Data_handler.PhoneNum_Filter(df)  # 오류번호, 내부번호

    df = Data_handler.orderStatus_filter(df, Brd)  # 반품

    df = Data_handler.CommonColumns(df, Brand) # 쉬운 가공열들

    df = Data_handler.Blacklist_Mapping(df)  # Bulk

    df = Data_handler.Item_Mapping(df, Brd) # Landing, Item

    df = Data_handler.NumColumns(df) # Sales_Total, Quantity_Option

    Option_df = datalist('map', 'tb_map_primecost_option_' + Brd, "")
    Option_df.columns = ['idx', '주문상품명', '상품옵션', 'Item_Code', 'Quantity_Bundle', 'Set', 'Item_Option', 'SKU']

    Option_df.loc[Option_df.상품옵션=='', '상품옵션'] = '@'
    df['상품옵션'] = df['상품옵션'].fillna("@")

    df = Data_handler.Option_Mapping(df, Option_df) # Item Option

    main_df = df[~df['Unused_Data'].isin(['반품', '내부번호', '오류번호'])] # 정상 주문건
    main_df = mainData(main_df, Option_df, Brand, Brd, start_date, report_date, update_all)

    err_df = df[df['Unused_Data'].isin(['반품', '내부번호', '오류번호'])] # 제외할 주문건
    err_df = errData(err_df, Option_df, Brand, Brd)

    final_df = pd.concat([main_df, err_df], ignore_index=True)
    final_df = Data_handler.Row_divide(final_df) # Quantity_divide, Sales_divide

    final_df = final_df[final_field(Brd)]

    # final_df.to_csv(Brand + '_final_20일.csv', encoding='utf-8-sig', index=False)
    print(final_df.shape)

    del_query = 'Where Date_ between "{}" and "{}"'.format(start_date, report_date)
    # del_data('salesrp', 'tb_salesrp_sku_' + Brd + '_current', del_query)
    # insert_data(final_df, 'salesrp', 'tb_salesrp_sku_' + Brd + '_current')

    # CrossSale RD 생성
    if Brand == '유리카':
        value = 'SKU'
    elif Brand == '티타드':
        value = 'Cur_SKU'
    else:
        value = 'Item_Option'

    # 부분 업데이트 하는 경우
    if update_all == False:
        Past_Cross_df = datalist('salesrp', 'tb_salesrp_cross_temp', 'where Brand = "' + Brand + '"')
        Past_Cross_df = Past_Cross_df.drop(columns=0)
        Past_Cross_df.columns = ['Brand', 'Phone_Number', 'First_Purchase_Date', 'Sequence', 'Product']

        Cross_df = Data_handler.CrossItem_List(main_df, Brand, value)
        Cross_df = pd.concat([Past_Cross_df, Cross_df], ignore_index=True)
        Cross_df = Cross_df.astype({'Phone_Number' : str,
                                    'First_Purchase_Date' : str,
                                    'Sequence' : float,
                                    'Product' : str})
        Cross_df = Cross_df.sort_values(by=['Brand', 'Phone_Number', 'Sequence'], ascending=(True, True, True))
        Cross_df = Cross_df.drop_duplicates(['Phone_Number', 'Sequence'], keep='last')

    # 전체 업데이트 하는 경우
    elif update_all == True:
        Cross_df = Data_handler.CrossItem_List(main_df, Brand, value)

    # del_data('salesrp', 'tb_salesrp_cross_temp', 'where Brand = "' + Brand + '"')
    # insert_data(Cross_df, 'salesrp', 'tb_salesrp_cross_temp')

    Cross_df = Data_handler.CrossItem_Pivot(Cross_df, Brand, 'Product')
    #Cross_df.to_csv(Brand + '14일_크로스셀링.csv', encoding='euc-kr', index=False)
    # del_data('salesrp', 'tb_salesrp_cross_' + Brd, "")
    # insert_data(Cross_df, 'salesrp', 'tb_salesrp_cross_' + Brd)


if __name__ == "__main__":
    """
    인풋 폴더 : ETL용 cafe24 인풋 폴더 --> end_date-1 날짜 폴더에서 가져옴 (end=0이면 어제 폴더)
    start, end 변수는 오늘 - n 일을 하고싶은지 입력
    update_all 변수는 전체 업데이트할 경우 True, 부분 업데이트할 경우 False 로 둠 (전체 업데이트하는 경우 start=9000으로 설정)
    """
    print('start time: ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    main('몽제', start=20, end=0, update_all=False)

    # for Brand in ['티타드']: #'유리카', '클럭', '몽제',
    #     try :
    #         main(Brand, start=9000, end=0, update_all=True)
    #     except:
    #         pass

    print('end time: ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print('\n')

    # 'Date_ >= "{}"'.format(date)
    # echoroas4897@
