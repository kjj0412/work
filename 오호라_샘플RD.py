import pandas as pd
import numpy as np
import random
from Data_Upload_Ohora import datalist, insert_data, del_data

'''
crm1에서 가져온 상품목록
'''
df = pd.read_csv('오호라 상품목록.csv', encoding='UTF-8')

# 제품군
df['prd_category'] = df.prd_name.apply(lambda x: '기타' if x=='' else ('N' if x[0]=='N' else ('P' if x[0]=='P' else '기타')))
print(df.head(3))

# 색상
color = ['red', 'orange', 'yellow', 'green', 'blue', 'navy', 'purple', 'white', 'black', 'grey', 'multi_color']
df['color'] = [random.choice(color) for i in range(len(df))]
print(df.head(3))

# 채도
saturation = [1, 2, 3]
df['saturation'] = [random.choice(saturation) for i in range(len(df))]
print(df.head(3))

# 디자인 요소
design = ['캐릭터', '라인', '호피', '도트', '마블', '글리터', '자석', '시럽']
df['design'] = [random.choice(design) for i in range(len(df))]
print(df.head(3))

# 파츠 여부
parts = ['Y', 'N']
df['parts'] = [random.choice(parts) for i in range(len(df))]
print(df.head(3))

# 기타 상품 처리
df.loc[df.prd_category =='기타', ['color', 'saturation', 'design', 'parts']] = ['-','-','-','-']

# 적재
# del_data('ohora', 'prd_mapping_sample', '')
# insert_data(df, 'ohora', 'prd_mapping_sample')


'''
crm_joined6 에 상품명기준으로 매핑
2020.6~2020.12 기간만 있음, 5318908행
'''
df = df.rename(columns={'prd_name' : 'First_SKU'})
print(df.columns)

cols = 'Date_, Orderid, Phone_Number, Sequence, First_Purchase_Date, First_Orderid, First_SKU, Cohort_Days'
crm_joined6 = datalist('ohora','crm_joined6',cols,'')
crm_joined6.columns = ['Date_', 'Orderid', 'Phone_Number', 'Sequence', 'First_Purchase_Date', 'First_Orderid', 'First_SKU', 'Cohort_Days']
crm_joined6 = pd.merge(left=crm_joined6, right=df, on = ['First_SKU'], how='left')

del_data('ohora', 'crm_joined6_sample', '')
insert_data(crm_joined6, 'ohora', 'crm_joined6_sample')



