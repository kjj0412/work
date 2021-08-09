


def field_sorting(Brd):
    if Brd == 'fs':
        ColumnName = [
            'Date_',
            'Orderid',
            'Marketplace',
            'Phone_Number',
            'Join_Member',
            'Join_Date',
            'Membership_Level',
            'Broad_Repurchase_User',
            'Bulk',
            'Unused_Data',
            'Use_Credits',
            'Item',
            # 'Item_Option',
            # 'Pre_Item_Option',
            # 'Cur_Item_Option',
            'SKU_Code', #추가됨
            'SKU',
            'Pre_SKU',
            'Cur_SKU',
            'Option_SKU',
            'Sequence_SKU',
            # 'Landing',
            'Coupon_Name',
            'Sequence',
            'Sequence_Broad',
            'First_Purchase_Date',
            'Cohort_Days',
            'Interval_Days',
            'Interval_Days_SKU',
            'Sales_Total',
            'Quantity_Option',
            'Quantity_SKU',
            'Quantity_Bundle',
            '주문상품명',
            '상품품목코드',
            '상품옵션',
            'Set',
            'Shape',  # 추가됨
            'Lineup',  # 추가됨
            'Collection'  # 추가됨
        ]
    elif Brd == 'an':
        ColumnName = [
            'Date_',
            'Orderid',
            'Marketplace',
            'Phone_Number',
            'Join_Member',
            'Join_Date',
            'Membership_Level',
            'Broad_Repurchase_User',
            'Bulk',
            'Unused_Data',
            'Use_Credits',
            'Order_Path',  # 추가됨
            'Payment_Method',  # 추가됨
            # 'Item_Option',
            # 'Pre_Item_Option',
            # 'Cur_Item_Option',
            'SKU',
            'Pre_SKU',
            'Cur_SKU',
            'Option_SKU',
            # 'Landing',
            'Coupon_Name',
            'Sequence',
            'Sequence_Broad',
            'First_Purchase_Date',
            'Cohort_Days',
            'Interval_Days',
            'Interval_Days_SKU',
            'Sales_Total',
            'Quantity_Option',
            'Quantity_SKU',
            'Quantity_Bundle',
            '주문상품명',
            '상품품목코드',
            '상품옵션',
            'Style_Code',  # 추가됨
            'Color_Code',  # 추가됨
            'Category1',   # 추가됨
            'Category2',   # 추가됨
            'Category3'    # 추가됨
        ]
    else:
        ColumnName = [
            'Date_',
            'Orderid',
            'Marketplace',
            'Phone_Number',
            'Join_Member',
            'Join_Date',
            'Membership_Level',
            'Broad_Repurchase_User',
            'Bulk',
            'Unused_Data',
            'Use_Credits',
            'Item',
            'Item_Option',
            'Pre_Item_Option',
            'Cur_Item_Option',
            'SKU',
            'Pre_SKU',
            'Cur_SKU',
            'Option_SKU',
            'Sequence_SKU',
            'Landing',
            'Coupon_Name',
            'Sequence',
            'Sequence_Broad',
            'First_Purchase_Date',
            'Cohort_Days',
            'Interval_Days',
            'Interval_Days_SKU',
            'Sales_Total',
            'Quantity_Option',
            'Quantity_SKU',
            'Quantity_Bundle',
            '주문상품명',
            '상품품목코드',
            '상품옵션',
            'Set'
        ]
    return ColumnName


def final_field(Brd):

    if Brd == 'fs':
        ColumnName = [
            'Date_',
            'Orderid',
            'Marketplace',
            'Phone_Number',
            'Join_Member',
            'Join_Date',
            'Membership_Level',
            'Broad_Repurchase_User',
            'Bulk',
            'Unused_Data',
            'Use_Credits',
            'Set',
            'Item',
            # 'Item_Option',
            # 'Pre_Item_Option',
            # 'Cur_Item_Option',
            'SKU_Code',  # 추가됨
            'SKU',
            'Pre_SKU',
            'Cur_SKU',
            'Option_SKU',
            'Shape',  # 추가됨
            'Lineup',  # 추가됨
            'Collection',  # 추가됨
            # 'Landing',
            'Coupon_Name',
            'Sequence',
            'Sequence_SKU',
            'Sequence_Broad',
            'First_Purchase_Date',
            'Cohort_Days',
            'Interval_Days',
            'Interval_Days_SKU',
            'Quantity_Option',
            'Quantity_SKU',
            'Quantity_Bundle',
            'Quantity_Rows',
            'Quantity_Divide',
            'Sales_Total',
            'Sales_Divide'
        ]
    elif Brd == 'an':
        ColumnName = [
            'Date_',
            'Orderid',
            'Marketplace',
            'Phone_Number',
            'Join_Member',
            'Join_Date',
            'Membership_Level',
            'Broad_Repurchase_User',
            'Bulk',
            'Unused_Data',
            'Use_Credits',
            'Order_Path', # 추가됨
            'Payment_Method', # 추가됨
            # 'Item_Option',
            # 'Pre_Item_Option',
            # 'Cur_Item_Option',
            'Product_Name', # 추가됨
            'SKU',
            'Pre_SKU',
            'Cur_SKU',
            'Option_SKU',
            'Style_Code',  # 추가됨
            'Color_Code',  # 추가됨
            'Category1',  # 추가됨
            'Category2',  # 추가됨
            'Category3',  # 추가됨
            # 'Landing',
            'Coupon_Name',
            'Sequence',
            'Sequence_Broad',
            'First_Purchase_Date',
            'Cohort_Days',
            'Interval_Days',
            'Interval_Days_SKU',
            'Quantity_Option',
            'Quantity_SKU',
            'Quantity_Bundle',
            'Quantity_Rows',
            'Quantity_Divide',
            'Sales_Total',
            'Sales_Divide'
        ]
    else:
        ColumnName = [
            'Date_',
            'Orderid',
            'Marketplace',
            'Phone_Number',
            'Join_Member',
            'Join_Date',
            'Membership_Level',
            'Broad_Repurchase_User',
            'Bulk',
            'Unused_Data',
            'Use_Credits',
            'Set',
            'Item',
            'Item_Option',
            'Pre_Item_Option',
            'Cur_Item_Option',
            'SKU',
            'Pre_SKU',
            'Cur_SKU',
            'Option_SKU',
            'Landing',
            'Coupon_Name',
            'Sequence',
            'Sequence_SKU',
            'Sequence_Broad',
            'First_Purchase_Date',
            'Cohort_Days',
            'Interval_Days',
            'Interval_Days_SKU',
            'Quantity_Option',
            'Quantity_SKU',
            'Quantity_Bundle',
            'Quantity_Rows',
            'Quantity_Divide',
            'Sales_Total',
            'Sales_Divide'
        ]
    return ColumnName