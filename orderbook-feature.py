import pandas as pd

######################### Midprice, Midprice_wt, Midprice_mkt ####################

def cal_mid_price (gr_bid_level, gr_ask_level):

    level = 5

    if len(gr_bid_level) > 0 and len(gr_ask_level) > 0:
        bid_top_price = gr_bid_level.iloc[0].price
        bid_top_level_qty = gr_bid_level.iloc[0].quantity
        ask_top_price = gr_ask_level.iloc[0].price
        ask_top_level_qty = gr_ask_level.iloc[0].quantity
    

        mid_price = (bid_top_price + ask_top_price) * 0.5
        mid_price_wt = ((gr_bid_level.iloc[2].price) + (gr_ask_level.iloc[2].price)) * 0.5
        mid_price_mkt = ((bid_top_price*ask_top_level_qty) + (ask_top_price*bid_top_level_qty))/(bid_top_level_qty+ask_top_level_qty)

        return mid_price, mid_price_wt, mid_price_mkt

    else: 
        print ('Error: serious cal_mid_price')
        return (-1)


#################################### Book Imbalance ##########################

# Feature: calculating 'bookI' using orderbook 
# book imbalance

# @params [ratio, level, interval]

# gr_bid_level: all bid level
# gr_ask_level: all ask level
# var: can be empty
# mid: midprice


def calc_book_imb(param, gr_bid_level, gr_ask_level, var, mid):
    
    mid_price = mid

    ratio = param[0]; level = param[1]; interval = param[2]
    #print ('processing... %s %s,level:%s,interval:%s' % (sys._getframe().f_code.co_name,ratio,level,interval)), 

    quant_v_bid = (gr_bid_level.quantity)**ratio  #gr_bid_level.quantity와 .price가 str 자료형으로 인식돼어 연산이 불가능.
    price_v_bid = (gr_bid_level.price)*quant_v_bid #따라서 아래에서 astype()을 이용해서 float으로 변환.

    quant_v_ask = (gr_ask_level.quantity)**ratio
    price_v_ask = (gr_ask_level.price)*quant_v_ask
    
    askQty = quant_v_ask.values.sum()
    bidPx = price_v_bid.values.sum()
    bidQty = quant_v_bid.values.sum()
    askPx = price_v_ask.values.sum()
    bid_ask_spread = interval
        
    book_price = 0 #because of warning, divisible by 0
    if bidQty > 0 and askQty > 0:
        book_price = (((askQty*bidPx)/bidQty) + ((bidQty*askPx)/askQty)) / (bidQty+askQty)

        
    indicator_value = (book_price - mid_price) / bid_ask_spread
    
    return indicator_value


##################################### Top Bid/Ask Price Ratio ##################################

def calc_bid_ask_ratio(gr_ask_level, gr_bid_level):
    bid_top_price = gr_bid_level.iloc[0].price
    ask_top_price = gr_ask_level.iloc[0].price

    ratio = bid_top_price/ask_top_price
    return ratio

###################################### Main Code ###########################################

print('start')

loaded_fn = '2023-11-10-bithumb-btc-orderbook.csv'
split_fn = loaded_fn.split('-')
saved_fn = '{0}-{1}-{2}-{3}-{4}-feature.csv'.format(split_fn[0],split_fn[1],split_fn[2],split_fn[3],split_fn[4])


df = pd.read_csv(loaded_fn).apply(pd.to_numeric,errors='ignore')

groups = df.groupby('timestamp')

keys = groups.groups.keys()
features = pd.DataFrame({'timestamp' : [],'mid_price' : [], 'mid_price_wt':[], 'mid_price_mkt':[], 'book_I_0.2_5_1': [], 'book_I_0.4_5_1':[], 'book_I_0.6_5_1':[], 'book_I_0.8_5_1':[], 'B/A_ratio':[]})

for i in keys:
    gr_o = groups.get_group(i)
    
    gr_bid_level = gr_o[(gr_o.type == 0)]
    gr_ask_level = gr_o[(gr_o.type == 1)]

    mid, mid_wt, mid_mkt = cal_mid_price(gr_bid_level, gr_ask_level) 

    param = [0.2, 5, 1] # ratio, level, interval
    bookI_a = calc_book_imb(param, gr_bid_level, gr_ask_level,None, mid)
    
    param = [0.4, 5, 1] # ratio, level, interval
    bookI_b = calc_book_imb(param, gr_bid_level, gr_ask_level,None, mid)

    param = [0.6, 5, 1] # ratio, level, interval
    bookI_c = calc_book_imb(param, gr_bid_level, gr_ask_level,None, mid)

    param = [0.8, 5, 1] # ratio, level, interval
    bookI_d = calc_book_imb(param, gr_bid_level, gr_ask_level,None, mid)

    ratio = calc_bid_ask_ratio(gr_bid_level, gr_ask_level) # Top_Bid/Top_Ask로 나타낸 비율

    oneFeature = pd.DataFrame({'timestamp' : [i],'mid_price' : [mid], 'mid_price_wt':[mid_wt], 'mid_price_mkt':[mid_mkt], 'book_I_0.2_5_1': [bookI_a], 'book_I_0.4_5_1':[bookI_b], 'book_I_0.6_5_1':[bookI_c], 'book_I_0.8_5_1':[bookI_d], 'B/A_ratio':[ratio]})
    features = pd.concat([features, oneFeature], ignore_index=True)

features.to_csv(saved_fn, index=False, header=True, mode = 'a')

print('done')