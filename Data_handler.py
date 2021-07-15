import datetime
import os
import pandas as pd
import pickle
import numpy as np
import re
from Data_Upload import datalist, insert_data, del_data


def is_directory(dir):
    directory = dir
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory


def data_input(Brd, file_dir):
    """경로 내에 있는 모든 csv 파일 합치기"""
    all_df = pd.DataFrame()
    for i in file_dir:
        try:
            df = pd.read_csv(i, encoding='UTF-8')
        except:
            df = pd.read_csv(i, encoding='euc-kr')
        all_df = all_df.append(df)

    # 배송상태만 다른 데이터 처리용 (배송완료&반품완료 등)
    all_df = all_df.drop_duplicates(['주문번호', '주문상품명', '상품코드', '상품옵션', '상품품목코드'], keep='last')

    # 안다르 헤더명 조정
    if Brd == 'an':
        all_df = all_df.rename(columns = {'총 상품구매금액' : '총 상품구매금액(KRW)'})

    return all_df


def orderStatus_filter(df, Brd):
    df['주문 상태'] = df['주문 상태'].fillna("@")
    df = df[~df['주문 상태'].str.contains('취소')]
    df['Unused_Data'] = df.apply(lambda x: '반품' if x['주문 상태'].startswith('반품') else x['Unused_Data'], axis=1)

    return df


def PhoneNum_Filter(df, Brd):
    #전화번호 합치기
    df['Phone_Number'] = df.apply(
        lambda x: x['주문자 휴대전화'] if ((x['주문자 휴대전화'] != "--") and not (pd.isnull(x['주문자 휴대전화'])))
        else (x['주문자 전화번호'] if ((x['주문자 전화번호'] != "--") and not (pd.isnull(x['주문자 전화번호'])))
              else (x['수령인 휴대전화'] if ((x['수령인 휴대전화'] != "--") and not (pd.isnull(x['수령인 휴대전화'])))
                    else x['수령인 전화번호'])), axis=1)
    df = df.astype({'Phone_Number' : str})
    df['Phone_Number'] = df['Phone_Number'].str.replace('+82 ', '0', regex=False)
    df['Phone_Number'] = df.apply(lambda x: x['주문자ID'] if (x['Phone_Number'].startswith('--')) or
                                            (x['Phone_Number'].endswith('--')) or (x['Phone_Number'] == 'nan')
                                            else x['Phone_Number'], axis=1)

    df = df.astype({'Phone_Number': str})

    #오류번호
    df['Unused_Data'] = df.apply(lambda x : '오류번호'
            if (x['Phone_Number'].startswith('000')) or (x['Phone_Number'].endswith('123-1234')) or (x['Phone_Number'].endswith('1234-1234'))
                or (x['Phone_Number'] == '--') or (x['Phone_Number'] == 'nan') else "일반", axis=1)

    df['Num_filter'] = df['Phone_Number'].replace('^[0-9]{0,3}-', '', regex=True)
    df['Num_filter'] = df['Num_filter'].replace('\-', '', regex=True)
    for i in range(10):
        df['Unused_Data'] = df.apply(lambda x: '오류번호' if x['Num_filter'].count(str(i)) >= 7 else x['Unused_Data'], axis=1)

    #내부번호
    if Brd == 'an':
        phone_filter = pd.read_excel('Mapping\\[Repurchase] 재구매_내부번호.xlsx',
                                     sheet_name='내부번호',
                                     names=['Phone_Number'])
    else:
        phone_filter = pd.read_excel('..\\..\\데일리앤코_Pentaho_리뉴얼_V1\\Mapping\\[Repurchase] 재구매_내부번호.xlsx',
                                     sheet_name='내부번호',
                                     names=['Phone_Number'])
    phone_filter['Check'] = 'O'
    df = pd.merge(left=df, right=phone_filter, on=['Phone_Number'], how='left')
    df['Unused_Data'] = df.apply(lambda x: '내부번호' if x['Check'] == 'O' else x['Unused_Data'], axis=1)

    return df

def CommonColumns(df, Brand):
    # 텍스트 replace (Brand, Marketplace)
    if Brand == '안다르':
        pass
    else:
        df['주문경로'] = df['주문경로'].replace('모바일웹', '자사몰 일반')
        df['주문경로'] = df['주문경로'].replace('PC쇼핑몰', '자사몰 일반')
        df['주문경로'] = df['주문경로'].replace('네이버 페이', '자사몰 네이버페이')
    # 기타열 생성
    df['Use_Coupon'] = df.apply(lambda x: "Y" if (x['쿠폰 할인금액'] > 0) else 'N', axis=1)
    df['Use_Credits'] = df.apply(lambda x: "Y" if (x['사용한 적립금액'] > 0) else 'N', axis=1)
    df['Join_Member'] = df.apply(lambda x: "Y" if not (pd.isnull(x['주문자ID'])) else 'N', axis=1)
    # 날짜열
    df['결제일시(입금확인일)'] = pd.to_datetime(df['결제일시(입금확인일)'])
    df['Date_'] = df['결제일시(입금확인일)'].dt.strftime('%Y-%m-%d')
    df['Date_'] = pd.to_datetime(df['Date_'], format='%Y-%m-%d')
    df = df.rename(columns={'주문번호': 'Orderid',
                            '주문 시 회원등급': 'Membership_Level',
                            '상품번호': 'Landing',
                            '사용한 쿠폰명': 'Coupon_Name',
                            '수량': 'Quantity_Option',
                            '주문경로': 'Marketplace',
                            '주문자 가입일': 'Join_Date'
                            })
    df['Brand'] = Brand
    df = df.astype({'Landing': str})

    return df


def Item_Mapping(df, Brd):
    if Brd == 'fs' or Brd == 'an':
        pass
    else:
        item_mapping = datalist('map', 'tb_map_landingprdt_' + Brd, "")
        item_mapping.columns = ['idx', 'Brand', 'Marketplace', 'Landing', 'Item', 'Landing_Type', 'Product', 'Landing_Obj', 'x1', 'x2', 'x3']
        item_mapping = item_mapping[['Brand', 'Landing', 'Item']]
        item_mapping = item_mapping.drop_duplicates(['Brand','Landing'],keep='last')

        #landing을 문자열로 변환
        item_mapping = item_mapping.astype({'Landing': str})
        item_mapping.Landing = item_mapping.Landing.str.replace('.0', '', regex=False)
        df = df.astype({'Landing': str})
        df.Landing = df.Landing.str.replace('.0', '', regex=False)

        df = pd.merge(left=df, right=item_mapping, on=['Brand', 'Landing'], how='left')
        df['Item'] = df['Item'].fillna("@")

        # 인풋 자체에서 랜딩이 null로 나오는 경우 처리
        df['Landing'] = df.apply(lambda x: "@" if ('nan' in x['Landing']) else x['Landing'], axis=1)
        df['Item'] = df.apply(lambda x: "미니" if (x['Landing'] == "@") and (x['Brand'] == '클럭') else x['Item'], axis=1)

    return df


def Blacklist_Mapping(df, Brd):
    if Brd == 'an':
        Black_mapping = pd.read_excel('Mapping\\[Repurchase] 재구매_내부번호.xlsx',
                                      sheet_name='블랙리스트',
                                      names=['Brand', 'Phone_Number'])
    else:
        Black_mapping = pd.read_excel('..\\..\\데일리앤코_Pentaho_리뉴얼_V1\\Mapping\\[Repurchase] 재구매_내부번호.xlsx',
                                     sheet_name='블랙리스트',
                                     names=['Brand', 'Phone_Number'])
    Black_mapping = Black_mapping.drop_duplicates()
    Black_mapping['Bulk'] = '블랙리스트'
    df = pd.merge(left=df, right=Black_mapping, on=['Brand', 'Phone_Number'], how='left')
    df['Bulk'] = df.apply(lambda x: x['Bulk'] if x['Bulk'] == '블랙리스트'
                                    else("대량구매" if ('대량구매' in x['주문상품명']) else '일반'), axis=1)

    return df


def get_Codes(Brd, df):
    '''
    * 안다르 주문상품명에서 Style_Code, Color_Code 추출
    * 세트상품은 행분리
    '''

    if Brd == 'an':
        # product 코드 추출
        Regex = re.compile('\[[0-9a-zA-Z]*-[0-9a-zA-Z]*\]|\([0-9a-zA-Z]*-[0-9a-zA-Z]*\)')
        df['Product_Code'] = df['주문상품명(세트상품 포함)'].apply(lambda x: Regex.findall(str(x)))  # 코드가 리스트로 들어감, 대괄호 중괄호 모두 들어옴
        df['Product_Code'] = df['Product_Code'].apply(lambda x: re.sub('[\[\]\(\)\']', '', str(x)) if len(x) > 0 else '구분불가')

        # 행분리
        df = tidy_split(df, 'Product_Code', sep=',')
        df['Product_Code'] = df['Product_Code'].apply(lambda x:x.strip())

        # Style code와 color code 분리
        df['Style_Code'] = df['Product_Code'].apply(
            lambda x: x if x == '구분불가' else x[:-3] if len(x.split('-')[0]) > 4 else x[:-2] if len(
                x.split('-')[0]) == 4 else x)
        df['Color_Code'] = df['Product_Code'].apply(
            lambda x: x if x == '구분불가' else x[-3:] if len(x.split('-')[0]) > 4 else x[-2:] if len(
                x.split('-')[0]) == 4 else x)
    else:
        pass

    return df


def get_PaymentMethod(Brd, df):
    '''
    * 결제방식(대)
      - 주문경로 = 네이버 페이 이면 네이버페이, 그 외 일반구매
    * 결제방식(중)
      - 결제수단 = 적립금 이고 결제업체 = NULL 이면 적립금
      - 결제수단이 적립금이 아니고 결제업체 = NULL 이면 네이버페이
      - 그 외에는 결제업체 값과 동일
    '''
    if Brd == 'an':
        df['결제방식(대)'] = df['Marketplace'].map({'PC쇼핑몰':'일반구매', '모바일웹':'일반구매', '모바일앱':'일반구매', '네이버 페이':'네이버페이'})
        df['결제업체'] = df['결제업체'].astype('str')
        df['결제방식(중)'] = df.apply(
            lambda x: '적립금'
            if ((x['결제수단'] == '적립금') and (x['결제업체'] == 'nan'))
            else '네이버페이' if ((x['결제수단'] != '적립금') and (x['결제업체'] == 'nan'))
            else (x['결제업체']), axis=1)

    else:
        pass
    return df

def get_Option_df(Brd):
    '''
    * DB에서 옵션매핑 테이블 가져오기
    * 안다르 : SA DB > andar.prdtinfo
    * 데일리앤코 : ECOMMERCE DB > map.tb_map_primecost_option_Brd
    * return 전 매핑기준열이 비어있을경우 '@' 로 채운다
    '''
    if Brd == 'an':
        Option_df = datalist('andar', 'prdtinfo', "")
        Option_df.columns = ['idx', 'Style_Code', '주문상품명', 'Tag_Price', 'Price', 'Grade', 'Tier', 'Category1', 'Category2', 'Category3', 'Prime_Cost']
        Option_df = Option_df[['Style_Code', 'Category1', 'Category2', 'Category3']]
        Option_df.loc[Option_df.Style_Code == '', 'Style_Code'] = '@'

    else:
        Option_df = datalist('map', 'tb_map_primecost_option_' + Brd, "")
        if Brd == 'fs':
            Option_df.columns = ['idx', '주문상품명', '상품옵션', 'SKU', 'Quantity_Bundle', 'Set', 'Item', 'Shape', 'Lineup',
                                 'Collection']
        else:
            Option_df.columns = ['idx', '주문상품명', '상품옵션', 'Item_Code', 'Quantity_Bundle', 'Set', 'Item_Option', 'SKU']
        Option_df.loc[Option_df.상품옵션=='', '상품옵션'] = '@'

    return Option_df


def Option_Mapping(Brd, df, Option_df):
    '''
    불러온 Option_df 정보를 구매정보에 매핑
    * 안다르 : Style_Code 기준으로 매핑
    * 데일리앤코 : 주문상품명, 상품옵션 기준으로 매핑
    '''

    if Brd == 'an':
        unique_cols = list(Option_df.columns)
        Option_df = Option_df.drop_duplicates(['Style_Code'], keep='last')  # 매핑 중복기입 이슈 방지용
        df = pd.merge(left=df, right=Option_df, on=['Style_Code'], how='left')
        df[unique_cols] = df[unique_cols].fillna('@')
    else:
        if Brd == 'fs':
            unique_cols = ['Item', 'Shape', 'Lineup', 'Collection']
        else:
            unique_cols = ['Item_Option']
        Option_df = Option_df[['주문상품명', '상품옵션', 'Set'] + unique_cols]
        Option_df = Option_df.drop_duplicates(['주문상품명', '상품옵션'], keep='last')  # 매핑 중복기입 이슈 방지용
        df = pd.merge(left=df, right=Option_df, on=['주문상품명', '상품옵션'], how='left')
        df[unique_cols] = df[unique_cols].fillna('@')

    return df


def SKU_Mapping(Brd, df, Option_df):
    """
    같은 주문상품명&상품옵션에 대해 SKU가 여러개 들어있는 경우 left outer join되어 행이 늘어남
    """
    if Brd == 'an':
        df['SKU']=df.Style_Code
        df['Quantity_Bundle'] = 1 #quantity_bundle이 안다르는 옵션매핑에서 들어오지 않음
        df['Quantity_SKU'] = df['Quantity_Option'] * df['Quantity_Bundle']
    else:
        Option_df = Option_df[['주문상품명', '상품옵션', 'Set', 'Quantity_Bundle', 'SKU']]
        Option_df = Option_df[~((Option_df['Set'] == '세트') & (Option_df['SKU'] == 'nan'))] #세트인데 SKU 매핑이 null이면 필터링
        Option_df = Option_df[Option_df['SKU'] != '증정품'] #증정품 필터링
        Option_df = Option_df.drop_duplicates(['주문상품명', '상품옵션', 'Quantity_Bundle', 'SKU'], keep='last')
        Option_df['SKU'] = Option_df['SKU'].fillna("@") #단품인데 null이면 @로 채움
        Option_df = Option_df.drop(columns="Set")
        df = pd.merge(left=df, right=Option_df, on=['주문상품명', '상품옵션'], how='left')
        df['SKU'] = df['SKU'].fillna("@") #단품인데 null이면 @로 채움

        df['Quantity_Bundle'] = df['Quantity_Bundle'].fillna(0).replace('', 0).astype(int) #quantity_bundle이 안다르는 옵션매핑에서 들어오지 않음
        df['Quantity_SKU'] = df['Quantity_Option'] * df['Quantity_Bundle']

    return df


def MappingCheck(df, Brd):
    if Brd == 'fs':
        cols = ['Item', '주문상품명', '상품옵션', 'SKU', 'Shape', 'Lineup', 'Collection']
    elif Brd == 'an':
        cols = ['주문상품명', '상품옵션', 'SKU', 'Style_Code', 'Color_Code', 'Category1', 'Category2', 'Category3']
    else:
        cols = ['Landing', 'Item', '주문상품명', '상품옵션', 'Item_Option', 'Quantity_Option', 'SKU']

    NoMapping = df[cols]

    crit3 = (NoMapping['SKU'].isnull()) | (NoMapping['SKU'] == "@")
    if Brd == "yc" or Brd == "tt":
        crit1 = (NoMapping['Item'].isnull()) | (NoMapping['Item'] == "@")
        NoMapping = NoMapping[crit1 | crit3]
        NoMapping = NoMapping.drop_duplicates(['Landing', '주문상품명', '상품옵션'])
    elif Brd == "fs":
        crit1 = (NoMapping['Item'].isnull()) | (NoMapping['Item'] == "@")
        crit4 = (NoMapping['Shape'].isnull()) | (NoMapping['Shape'] == "@")
        crit5 = (NoMapping['Lineup'].isnull()) | (NoMapping['Lineup'] == "@")
        crit6 = (NoMapping['Collection'].isnull()) | (NoMapping['Collection'] == "@")
        NoMapping = NoMapping[crit1 | crit3 | crit4 | crit5 | crit6]
        NoMapping = NoMapping.drop_duplicates(['주문상품명', '상품옵션'])
    elif Brd == "an":
        crit7 = (NoMapping['Style_Code'].isnull()) | (NoMapping['Style_Code'] == "@")
        crit8 = (NoMapping['Color_Code'].isnull()) | (NoMapping['Color_Code'] == "@")
        crit9 = (NoMapping['Category1'].isnull()) | (NoMapping['Category1'] == "@")
        crit10 = (NoMapping['Category2'].isnull()) | (NoMapping['Category2'] == "@")
        crit11 = (NoMapping['Category3'].isnull()) | (NoMapping['Category3'] == "@")
        NoMapping = NoMapping[crit7 | crit8 | crit9 | crit10 | crit11]
        NoMapping = NoMapping.drop_duplicates(['주문상품명', '상품옵션'])
    else:
        crit1 = (NoMapping['Item'].isnull()) | (NoMapping['Item'] == "@")
        crit2 = (NoMapping['Item_Option'].isnull()) | (NoMapping['Item_Option'] == "@")
        NoMapping = NoMapping[crit1 | crit2 | crit3]
        NoMapping = NoMapping.drop_duplicates(['Landing', '주문상품명', '상품옵션'])

    return NoMapping


def NumColumns(df):
    # 상품별 금액
    df['item_percentage'] = df['옵션+판매가'] * df['Quantity_Option'] / df['총 상품구매금액(KRW)']
    df['item_discount'] = df['사용한 적립금액'] + df['쿠폰 할인금액'] + df['회원등급 추가할인금액'] + df['배송비 할인금액']
    # 매출 (스스 별도 가공)
    df['Sales_Total'] = df.apply(
        lambda x: (x['총 실결제금액(최초정보)'] * x['item_percentage'] / 1.1) if (x['Marketplace'] == '스마트스토어')
        else ((((x['총 상품구매금액(KRW)'] + x['총 배송비'] - x['item_discount']) * x['item_percentage']) - x['상품별 추가할인금액']) / 1.1), axis=1)
    df['Sales_Total'] = df.apply(lambda x: ((x['옵션+판매가'] * x['Quantity_Option'] - x['상품별 추가할인금액'])/1.1)
            if (x['총 상품구매금액(KRW)'] == 'Nan') or  (x['총 상품구매금액(KRW)'] == 0) else x['Sales_Total'], axis=1)

    # 증정품 수량 0
    df['Quantity_Option'] = df.apply(
        lambda x: 0 if ((x['Brand'] == '몽제') and (x['옵션+판매가'] < 1000)) else (x['Quantity_Option']), axis=1)
    df['Quantity_Option'] = df.apply(
        lambda x: 0
        if ((x['Brand'] == '클럭') and (x['옵션+판매가'] == 0))
        or ((x['Brand'] == '클럭') and
            (('증정품' in x['주문상품명']) or ('증정품' in str(x['상품옵션']))))
        else (x['Quantity_Option']), axis=1)

    return df


def get_past_sequence(df, past_df):
    """
    14일 로직에서 DB에 쌓인 과거 데이터에서 Phone_Number별 구매정보 붙여주기
    * Broad_Repurchase_User_DB :확장재구매 유저 여부 (Y/N)
    * Last_OrderDate : 마지막 구매 날짜 (yyyy-MM-dd)
    * First_Purchase_Date_DB : 첫구매 날짜 (yyyy-MM-dd)
    * Last_Sequence : 마지막 구매회차 (int)
    """
    past_user_df = past_df[['Phone_Number',
                            'Broad_Repurchase_User',
                            'Date_',
                            'First_Purchase_Date',
                            'Sequence']]

    past_user_df.columns = ['Phone_Number',
                            'Broad_Repurchase_User_DB',
                            'Last_OrderDate',
                            'First_Purchase_Date_DB',
                            'Last_Sequence']

    past_user_df = past_user_df.sort_values(by=['Last_OrderDate', 'Last_Sequence'], ascending=(True, True))
    past_user_df = past_user_df.drop_duplicates(subset='Phone_Number', keep='last')

    past_user_df['Last_OrderDate'] = pd.to_datetime(past_user_df['Last_OrderDate'], format='%Y-%m-%d')
    df = pd.merge(left=df, right=past_user_df, on='Phone_Number', how='left')

    return df


def Pre_Item_list(df):
    Pre_Item_df = df[['Phone_Number', 'Sequence', 'Item_Option']]
    Pre_Item_df = Pre_Item_df.groupby(['Phone_Number', 'Sequence']).agg(lambda x: list(x)).reset_index()
    Pre_Item_df['Item_Option'] = Pre_Item_df['Item_Option'].apply(lambda x: str(sorted(set(x))))
    Pre_Item_df = Pre_Item_df.sort_values(by=['Phone_Number', 'Sequence'], ascending=(True, True))
    Pre_Item_df['Sequence'] = Pre_Item_df['Sequence'] + 1
    Pre_Item_df = Pre_Item_df.rename(columns={'Item_Option': 'Pre_Item_Option'})

    return Pre_Item_df


def Cur_Item_list(Brd, df):
    if Brd == 'fs' or Brd == 'an':
        pass
    else:
        cur_item_df = df[['Date_', 'Phone_Number', 'Sequence', 'Item_Option']]
        cur_item_df = cur_item_df.groupby(['Date_', 'Phone_Number', 'Sequence']).agg(lambda x: list(x)).reset_index()
        cur_item_df['Item_Option'] = cur_item_df['Item_Option'].apply(lambda x: str(sorted(set(x))))
        cur_item_df = cur_item_df.sort_values(by=['Date_', 'Phone_Number', 'Sequence'], ascending=(True, True, True))
        cur_item_df = cur_item_df.rename(columns={'Item_Option': 'Cur_Item_Option'})
        df = pd.merge(left=df, right=cur_item_df, on=['Date_', 'Phone_Number', 'Sequence'], how='left')

    return df


def Pre_SKU_list(SKU_df):
    Pre_SKU_df = SKU_df[['Phone_Number', 'Sequence', 'SKU', 'Quantity_SKU']]

    Pre_SKU_df = Pre_SKU_df.groupby(['Phone_Number', 'Sequence', 'SKU']).sum('Quantity_SKU').reset_index()
    Pre_SKU_df = Pre_SKU_df.astype({'Quantity_SKU' : str})
    Pre_SKU_df['Quantity_SKU'] = Pre_SKU_df['Quantity_SKU'].str.replace('.0', '', regex=False)
    Pre_SKU_df['SKU'] = Pre_SKU_df['SKU'] + "^" + Pre_SKU_df['Quantity_SKU']
    Pre_SKU_df = Pre_SKU_df.drop(columns='Quantity_SKU')
    Pre_SKU_df = Pre_SKU_df.groupby(['Phone_Number', 'Sequence']).agg(lambda x: list(x)).reset_index()
    Pre_SKU_df['SKU'] = Pre_SKU_df['SKU'].apply(lambda x: str(sorted(set(x))))
    Pre_SKU_df = Pre_SKU_df.rename(columns={'SKU' : 'Pre_SKU'})
    Pre_SKU_df = Pre_SKU_df.sort_values(by=['Phone_Number', 'Sequence'], ascending=(True, True))
    Pre_SKU_df['Sequence'] = Pre_SKU_df['Sequence'] + 1

    return Pre_SKU_df


def Pre_Item_Option(Brd, DB_past_df, df):
    if Brd == 'fs' or Brd == 'an':
        pass
    else:
        Pre_Option_DB = DB_past_df[['Phone_Number', 'Sequence', 'Item_Option']]
        Pre_Option_DB = Pre_Item_list(Pre_Option_DB)
        Pre_Option_DB = Pre_Option_DB.drop_duplicates(subset='Phone_Number', keep='last')
        Pre_Option_DB = Pre_Option_DB.rename(columns={'Pre_Item_Option': 'Pre_Item_Option_DB'})
        df = pd.merge(left=df, right=Pre_Option_DB, on=['Phone_Number', 'Sequence'], how='left')

        Pre_Option_New = Pre_Item_list(df)
        Pre_Option_New = Pre_Option_New.rename(columns={'Pre_Item_Option': 'Pre_Item_Option_New'})
        df = pd.merge(left=df, right=Pre_Option_New, on=['Phone_Number', 'Sequence'], how='left')

        df['Pre_Item_Option_DB'] = df['Pre_Item_Option_DB'].fillna("")
        df['Pre_Item_Option_New'] = df['Pre_Item_Option_New'].fillna("")
        df['Pre_Item_Option'] = df.apply(
            lambda x: x['Pre_Item_Option_DB'] if x['Pre_Item_Option_DB'] != '' else x['Pre_Item_Option_New'], axis=1)

    return df


def Pre_SKU(DB_past_df, SKU_df):
    Pre_SKU_df_DB = DB_past_df[['Phone_Number', 'Sequence', 'SKU', 'Quantity_SKU']]
    Pre_SKU_df_DB = Pre_SKU_list(Pre_SKU_df_DB)
    Pre_SKU_df_DB = Pre_SKU_df_DB.drop_duplicates(subset='Phone_Number', keep='last')
    Pre_SKU_df_DB = Pre_SKU_df_DB.rename(columns={'Pre_SKU': 'Pre_SKU_DB'})
    SKU_df = pd.merge(left=SKU_df, right=Pre_SKU_df_DB, on=['Phone_Number', 'Sequence'], how='left')

    Pre_SKU_df_New = Pre_SKU_list(SKU_df)
    Pre_SKU_df_New = Pre_SKU_df_New.rename(columns={'Pre_SKU': 'Pre_SKU_New'})
    SKU_df = pd.merge(left=SKU_df, right=Pre_SKU_df_New, on=['Phone_Number', 'Sequence'], how='left')

    SKU_df['Pre_SKU_DB'] = SKU_df['Pre_SKU_DB'].fillna("")
    SKU_df['Pre_SKU_New'] = SKU_df['Pre_SKU_New'].fillna("")
    SKU_df['Pre_SKU'] = SKU_df.apply(lambda x: x['Pre_SKU_DB'] if x['Pre_SKU_DB'] != '' else x['Pre_SKU_New'], axis=1)
    return SKU_df



def Cur_SKU_list(SKU_df):
    Cur_SKU_df = SKU_df[['Date_', 'Phone_Number', 'SKU', 'Quantity_Bundle', 'Sequence']]
    Cur_SKU_df = Cur_SKU_df.groupby(['Date_', 'Phone_Number', 'Sequence', 'SKU']).sum('Quantity_Bundle').reset_index()
    Cur_SKU_df = Cur_SKU_df.astype({'Quantity_Bundle': str})
    Cur_SKU_df['Quantity_Bundle'] = Cur_SKU_df['Quantity_Bundle'].str.replace('0', '', regex=False)
    Cur_SKU_df['SKU'] = Cur_SKU_df['SKU'] + "^" + Cur_SKU_df['Quantity_Bundle']
    Cur_SKU_df = Cur_SKU_df.drop(columns=['Quantity_Bundle'])
    Cur_SKU_df = Cur_SKU_df.groupby(['Date_', 'Phone_Number', 'Sequence']).agg(lambda x: list(x)).reset_index()
    Cur_SKU_df['SKU'] = Cur_SKU_df['SKU'].apply(lambda x: str(sorted(set(x))))
    Cur_SKU_df = Cur_SKU_df.rename(columns={'SKU': 'Cur_SKU'})
    Cur_SKU_df = Cur_SKU_df.sort_values(by=['Date_', 'Phone_Number', 'Sequence'], ascending=(True, True, True))
    SKU_df = pd.merge(left=SKU_df, right=Cur_SKU_df, on=['Date_', 'Phone_Number', 'Sequence'], how='left')

    return SKU_df


def Option_SKU_list(SKU_df):
    Option_SKU_df = SKU_df[['Date_', 'Phone_Number', '주문상품명', '상품옵션', 'SKU', 'Quantity_Bundle']]
    Option_SKU_df.SKU = Option_SKU_df.SKU.fillna('@')
    Option_SKU_df = Option_SKU_df.astype({'Quantity_Bundle': str})
    Option_SKU_df['Quantity_Bundle'] = Option_SKU_df['Quantity_Bundle'].str.replace('\b0\b', '', regex=True)
    Option_SKU_df['SKU'] = Option_SKU_df['SKU'] + "^" + Option_SKU_df['Quantity_Bundle']  # SKU와 번들 이어붙인 새로운 열 생성
    Option_SKU_df = Option_SKU_df.drop(columns=['Quantity_Bundle'])
    Option_SKU_df = Option_SKU_df.groupby(['Date_', 'Phone_Number', '주문상품명', '상품옵션']).agg(lambda x: list(x)).reset_index()
    Option_SKU_df['SKU'] = Option_SKU_df['SKU'].apply(lambda x: str(sorted(set(x))))
    Option_SKU_df = Option_SKU_df.rename(columns={'SKU': 'Option_SKU'})
    Option_SKU_df = Option_SKU_df.sort_values(by=['Date_', 'Phone_Number', '주문상품명', '상품옵션'],
                                              ascending=(True, True, True, True))
    SKU_df = pd.merge(left=SKU_df, right=Option_SKU_df, on=['Date_', 'Phone_Number', '주문상품명', '상품옵션'],
                      how='left')
    return SKU_df


def get_past_purchase_by_SKU(Brd, DB_past_df, SKU_df):
    '''
    Phone_Number, SKU 기준으로 과거 정보 붙이기
    * DB 데이터에서 Phone_Number, SKU별 마지막 구매회차 lookup (Last_Sequence_SKU)
    * DB 데이터에서 Phone_Number, SKU별 마지막 구매날짜 lookup (Last_Date_SKU)
    '''

    if Brd == 'an':
        Past_df = DB_past_df[['Phone_Number', 'SKU', 'Date_']]
    else:
        Past_df = DB_past_df[['Phone_Number', 'SKU', 'Date_', 'Last_Sequence_SKU']]
        # DB 내 SKU별 마지막 구매회차
        Sequence_SKU_DB = Past_df.drop(columns='Date_').groupby(['Phone_Number', 'SKU']).max('Last_Sequence_SKU').reset_index()
        SKU_df = pd.merge(left=SKU_df, right=Sequence_SKU_DB, on=['Phone_Number', 'SKU'], how='left')
        Past_df = Past_df.drop(columns='Last_Sequence_SKU')

    # DB 내 SKU별 마지막 구매일자
    Past_df = Past_df.sort_values(['Phone_Number', 'SKU', 'Date_'], ascending=(True, True, True))
    Past_df = Past_df.drop_duplicates(subset=['Phone_Number', 'SKU'], keep='last')
    Past_df = Past_df.rename(columns={'Date_' : 'Last_Date_SKU'})
    SKU_df = pd.merge(left=SKU_df, right=Past_df, on=['Phone_Number', 'SKU'], how='left')
    print(SKU_df.columns)
    SKU_df['Date_'] = pd.to_datetime(SKU_df['Date_'])
    SKU_df['Last_Date_SKU'] = pd.to_datetime(SKU_df['Last_Date_SKU'])

    return SKU_df

#### 신규
def Sequence_SKU(Brd, SKU_df):
    '''
    * 이번 기간 데이터에서 Phone_Number, SKU별 Sequence의 순서 매기기 (Sequence_SKU_new)
    * Sequence_SKU : SKU별 구매회차 구하기
      - 과거 구매정보 있으면 과거 Sequence_SKU + 현재 Sequence_SKU
      - 과거 구매정보 없으면 현재 Sequence_SKU
    '''

    if Brd == 'an':
        pass
    else:
        SKU_df['Sequence_SKU_new'] = SKU_df.groupby(['SKU', 'Phone_Number'])['Sequence'].rank(method='dense')
        SKU_df['Sequence_SKU'] = SKU_df.apply(
            lambda x: x['Sequence_SKU_new'] + x['Last_Sequence_SKU'] if x['Last_Sequence_SKU'] > 0
            else x['Sequence_SKU_new'], axis=1)
        SKU_df.drop(columns=['Sequence_SKU_new', 'Last_Sequence_SKU'])

    return SKU_df


def Interval_days_SKU_14(SKU_df):
    """
    14일 로직에서 Interval_days_SKU 계산
    * 이번 기간 데이터를 전화번호, sku, 날짜 기준으로 중복제거 (Interval_SKU_df)
    * 이번 기간 데이터에서 Phone_Number, SKU별 DateNum의 순서 매기기 (Sequence_SKU_2)
    * Phone_Number, SKU별 Interval_days_SKU 구하기
      - 과거 구매정보 있고 이번 기간 내 구매가 1회차이면 구매일 - DB의 마지막 구매일 (DB의 마지막 구매일은 get_past_purchase_by_SKU 에서 매핑됨)
      - 과거 구매정보 없는데 이번 기간 내 구매가 1회차면 ""
      - 과거 구매정보 없는데 이번 기간 내 구매가 2회차 이상이면 구매일 - 이번기간 내 이전 구매일 (이번기간 기준으로 별도 번호/SKU/날짜 매핑테이블 만들어서 merge)
    """

    Interval_SKU_df = SKU_df[['Phone_Number', 'SKU', 'Date_', 'Last_Date_SKU']]
    Interval_SKU_df = Interval_SKU_df.sort_values(['Phone_Number', 'SKU', 'Date_', 'Last_Date_SKU'], ascending=(True, True, True, True)).reset_index()
    Interval_SKU_df = Interval_SKU_df.drop_duplicates(subset=['Phone_Number', 'SKU', 'Date_'])

    Interval_SKU_df['Date_NUM'] = Interval_SKU_df['Date_'].dt.strftime('%Y%m%d')
    Interval_SKU_df = Interval_SKU_df.astype({'Date_NUM': int})
    Interval_SKU_df['Sequence_SKU_2'] = Interval_SKU_df.groupby(['Phone_Number', 'SKU'])['Date_NUM'].rank(method='min')

    Interval_SKU_df['Previous_Phone_Number'] = Interval_SKU_df['Phone_Number'].shift(1)
    Interval_SKU_df['Previous_OrderDate'] = Interval_SKU_df['Date_'].shift(1)
    Interval_SKU_df['Previous_SKU'] = Interval_SKU_df['SKU'].shift(1)

    Interval_SKU_df['Interval_Days_SKU'] = Interval_SKU_df.apply(lambda x : x['Date_'] - x['Previous_OrderDate'] if x['Sequence_SKU_2'] > 1
                                               else (x['Date_'] - x['Last_Date_SKU'] if ((x['Last_Date_SKU'] != 'NaT') and (x['Sequence_SKU_2'] == 1))
                                                     else ""), axis=1)

    Interval_SKU_df = Interval_SKU_df.astype({'Interval_Days_SKU': str})
    Interval_SKU_df['Interval_Days_SKU'] = Interval_SKU_df['Interval_Days_SKU'].str.replace(' days.*', '', regex=True)
    Interval_SKU_df['Interval_Days_SKU'] = Interval_SKU_df['Interval_Days_SKU'].apply(lambda x: x.replace('NaT', ''))
    Interval_SKU_df['Interval_Days_SKU'] = Interval_SKU_df.apply(lambda x: "" if (x['SKU'] == "@") else x['Interval_Days_SKU'], axis=1)

    SKU_df =  SKU_df.drop(columns='Last_Date_SKU')
    Interval_SKU_df['Date_'] = Interval_SKU_df['Date_'].dt.strftime('%Y-%m-%d')
    SKU_df['Date_'] = SKU_df['Date_'].dt.strftime('%Y-%m-%d')
    print(Interval_SKU_df.Date_.dtype, SKU_df.Date_.dtype)
    SKU_df = pd.merge(left=SKU_df, right=Interval_SKU_df, on=['Phone_Number', 'SKU', 'Date_'], how='left')

    return SKU_df


def Interval_days_SKU_all(SKU_df):
    '''
    전체 로직에서 Interval_days_SKU 계산
    * Minus_Date : 직전구매일. 전화번호, SKU가 동일한 케이스는 직전구매일을 동일하게 넣어줌
    * Interval_Days_SKU : 구매일 - 직전구매일 (별도 번호/SKU/날짜 매핑테이블 만들어서 merge)
    '''
    SKU_df = SKU_df.sort_values(['Phone_Number', 'SKU', 'Date_'], ascending=(True, True, True)).reset_index()
    SKU_df['Date_'] = pd.to_datetime(SKU_df['Date_'])
    Interval_SKU_df = SKU_df[['Phone_Number', 'SKU', 'Date_']]
    Interval_SKU_df = Interval_SKU_df.sort_values(['Phone_Number', 'SKU', 'Date_'], ascending=(True, True, True))
    Interval_SKU_df = Interval_SKU_df.drop_duplicates(subset=['Phone_Number', 'SKU', 'Date_'])

    Interval_SKU_df['Previous_Phone_Number'] = Interval_SKU_df['Phone_Number'].shift(1)
    Interval_SKU_df['Previous_OrderDate'] = Interval_SKU_df['Date_'].shift(1)
    Interval_SKU_df['Previous_SKU'] = Interval_SKU_df['SKU'].shift(1)
    Interval_SKU_df['Minus_Date'] = Interval_SKU_df.apply(lambda
            x: x['Previous_OrderDate'] if ((x['Phone_Number'] == x['Previous_Phone_Number']) and (x['SKU'] == x['Previous_SKU']))
            else "", axis=1)
    SKU_df = pd.merge(left=SKU_df, right=Interval_SKU_df, on=['Phone_Number', 'SKU', 'Date_'], how='left')

    SKU_df['Minus_Date'] = pd.to_datetime(SKU_df['Minus_Date'])
    SKU_df['Interval_Days_SKU'] = SKU_df['Date_'] - SKU_df['Minus_Date']
    SKU_df = SKU_df.astype({'Interval_Days_SKU' : str})
    SKU_df['Interval_Days_SKU'] = SKU_df['Interval_Days_SKU'].apply(lambda x : x.replace(' days', ''))
    SKU_df['Interval_Days_SKU'] = SKU_df['Interval_Days_SKU'].apply(lambda x : x.replace('NaT', ''))
    SKU_df['Date_'] = SKU_df['Date_'].dt.strftime('%Y-%m-%d')

    return SKU_df



def simple_table(df):
    """
    재구매 전체RD 요약해서 내보내기
     > 같은 날짜에 한 사람이 대량구매/일반 으로 각각 산 경우 Simple은 2건, 전체RD에서는 1건으로 집계되는 점 참고
    """
    df = df[['Date_', 'Bulk', 'Brand', 'Phone_Number', 'Sequence_Broad', 'Sequence', 'Quantity_Option', 'Sales_Total']]
    df['Purchase'] = df['Date_'] + df['Phone_Number']
    df['Sales'] = df['Sales_Total']

    try:
        df['Repurchase'] = df.apply(lambda x: x['Purchase'] if (x['Sequence_Broad'] >= 2) else '', axis=1)
        df['Re_Quantity'] = df.apply(lambda x: x['Quantity_Option'] if (x['Sequence_Broad'] >= 2) else 0, axis=1)
        df['Re_Sales'] = df.apply(lambda x: x['Sales_Total'] if (x['Sequence_Broad'] >= 2) else 0, axis=1)
    except TypeError:
        df['Repurchase'] = df.apply(lambda x: x['Purchase'] if (x['Sequence'] >= 2) else "", axis=1)
        df['Re_Quantity'] = df.apply(lambda x: x['Quantity_Option'] if (x['Sequence'] >= 2) else 0, axis=1)
        df['Re_Sales'] = df.apply(lambda x: x['Sales_Total'] if (x['Sequence'] >= 2) else 0, axis=1)

    simple_df = df.groupby(['Date_', 'Bulk', 'Brand'], as_index=False).agg(
        {"Re_Quantity": "sum",
         "Re_Sales" : "sum",
         "Sales": "sum",
         "Purchase" : 'nunique'})

    simple_repurchase = df[df['Repurchase'] != ""]
    simple_repurchase = simple_repurchase.groupby(
        ['Date_', 'Bulk', 'Brand'], as_index=False).agg({"Repurchase": "nunique"})
    simple_df = pd.merge(simple_df, simple_repurchase, how='left')
    simple_df = simple_df[['Date_', 'Bulk', 'Brand', 'Repurchase', 'Purchase', 'Re_Quantity', 'Re_Sales', 'Sales']]

    return simple_df


def Row_divide(df):
    """그룹 기준별 행 개수 세기"""
    df['상품옵션'] = df['상품옵션'].fillna('@') #merge 위해 null값 없애주기

    Row_df = df.copy()
    Row_df['Quantity_Rows'] = 1
    Row_df = Row_df.groupby(['Date_', 'Phone_Number', 'Orderid', 'Unused_Data', '주문상품명', '상품품목코드', '상품옵션'])['Quantity_Rows']\
        .sum().reset_index()

    df = pd.merge(left=df, right=Row_df,
                  on=['Date_', 'Phone_Number', 'Orderid', 'Unused_Data', '주문상품명', '상품품목코드', '상품옵션'],
                  how='left')
    df['Quantity_Divide'] = df['Quantity_Option'] / df['Quantity_Rows']
    df['Sales_Divide'] = df['Sales_Total'] / df['Quantity_Rows']
    df = df.rename(columns={'주문상품명':'SKU_Name'})

    return df

def CrossItem_List(df, Brand, value):
    """크로스세일 RD용 - 번호별 회차별 구매아이템 list"""
    CrossItem_df = df[['Phone_Number', 'First_Purchase_Date', 'Sequence', value]]
    CrossItem_df = CrossItem_df[CrossItem_df['Sequence'] <= 5]
    CrossItem_df = CrossItem_df.fillna("@")
    CrossItem_df = CrossItem_df.groupby(['Phone_Number', 'First_Purchase_Date', 'Sequence']).agg(lambda x: list(x)).reset_index()
    CrossItem_df[value] = CrossItem_df[value].apply(lambda x: list(set(x)))
    CrossItem_df[value] = CrossItem_df[value].apply(lambda x: ",".join(x))
    CrossItem_df['Brand'] = Brand
    CrossItem_df = CrossItem_df[['Brand', 'Phone_Number', 'First_Purchase_Date', 'Sequence', value]]
    CrossItem_df = CrossItem_df.rename(columns={value: 'Product'})

    # Cur_SKU로 들어간 Product 열에서 SKU만 남기기
    if value == 'Cur_SKU':
        CrossItem_df['Product'] = CrossItem_df.Product.apply(lambda x: re.sub('[\[\]\'\ \^0-9]', '', x))

    return CrossItem_df

def CrossItem_Pivot(df, Brand, value):
    """크로스세일 RD용 - 피벗 & 행분리 가공"""
    CrossItem_df = df.drop(columns="Brand")
    CrossItem_df = CrossItem_df.pivot(index=['Phone_Number', 'First_Purchase_Date'], columns='Sequence', values=value).reset_index()

    # 최대 Sequence가 5 미만일경우 열 추가
    max_sequence = len(CrossItem_df.columns) - 2
    if max_sequence < 5:
        for i in range(0, 5-max_sequence):
            CrossItem_df[float(max_sequence + i + 1)] = ''

    # Cur_SKU로 들어가는 경우 행분리 하지 않음
    for i in [1.0, 2.0, 3.0, 4.0, 5.0]:
        if Brand != '티타드':
            CrossItem_df = tidy_split(CrossItem_df, i, sep=',')
        CrossItem_df[i] = CrossItem_df[i].replace('nan', '').fillna('')

    CrossItem_df.columns = ['Phone_Number', 'First_Purchase_Date', 'Item_1st', 'Item_2nd', 'Item_3rd', 'Item_4th', 'Item_5th']

    return CrossItem_df

def tidy_split(df, column, sep='|', keep=False):
    """CrossItem 함수에서 쓰는 행분리 로직"""
    indexes = list()
    new_values = list()
    # df = df.dropna(subset=[column])
    for i, presplit in enumerate(df[column].astype(str)):
        values = presplit.split(sep)
        if keep and len(values) > 1:
            indexes.append(i)
            new_values.append(presplit)
        for value in values:
            indexes.append(i)
            new_values.append(value)
    new_df = df.iloc[indexes, :].copy()
    new_df[column] = new_values

    return new_df