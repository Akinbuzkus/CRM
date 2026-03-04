
###############################################################
# RFM ile Müşteri Segmentasyonu (Customer Segmentation with RFM)
###############################################################

###############################################################
# İş Problemi (Business Problem)
###############################################################
# FLO müşterilerini segmentlere ayırıp bu segmentlere göre pazarlama stratejileri belirlemek istiyor.
# Buna yönelik olarak müşterilerin davranışları tanımlanacak ve bu davranış öbeklenmelerine göre gruplar oluşturulacak..

###############################################################
# Veri Seti Hikayesi
###############################################################

# Veri seti son alışverişlerini 2020 - 2021 yıllarında OmniChannel(hem online hem offline alışveriş yapan) olarak yapan müşterilerin geçmiş alışveriş davranışlarından
# elde edilen bilgilerden oluşmaktadır.

# master_id: Eşsiz müşteri numarası
# order_channel : Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile, Offline)
# last_order_channel : En son alışverişin yapıldığı kanal
# first_order_date : Müşterinin yaptığı ilk alışveriş tarihi
# last_order_date : Müşterinin yaptığı son alışveriş tarihi
# last_order_date_online : Muşterinin online platformda yaptığı son alışveriş tarihi
# last_order_date_offline : Muşterinin offline platformda yaptığı son alışveriş tarihi
# order_num_total_ever_online : Müşterinin online platformda yaptığı toplam alışveriş sayısı
# order_num_total_ever_offline : Müşterinin offline'da yaptığı toplam alışveriş sayısı
# customer_value_total_ever_offline : Müşterinin offline alışverişlerinde ödediği toplam ücret
# customer_value_total_ever_online : Müşterinin online alışverişlerinde ödediği toplam ücret
# interested_in_categories_12 : Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi

###############################################################
# GÖREVLER
###############################################################


############################################
# Görev 1:  Veriyi Anlama ve Hazırlama
############################################


### Adım 1 : flo_data_20K.csv verisini okuyunuz.Dataframe’in kopyasını oluşturunuz.
import pandas as pd
import datetime as dt
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)
pd.set_option('display.float_format', lambda x: '%.2f' % x)


flo_data = pd.read_csv("C:/flo_data_20k.csv")
df = flo_data.copy()

### Adım 2:   Veri setinde
#               a. İlk 10 gözlem,
#               b. Değişken isimleri,
#               c. Betimsel istatistik,
#               d. Boş değer,
#               e. Değişken tipleri, incelemesi yapınız.
#               f. Boyut bilgisi


df.head(10)
df.columns
df.describe().T
df.isnull().sum()
df.info()
df.shape



### Adım 3:  Omnichannel müşterilerin hem online'dan hem de offline platformlardan alışveriş yaptığını ifade etmektedir.
# Her bir müşterinin toplam alışveriş sayısı ve toplam harcaması için yeni değişkenler oluşturunuz.

df['total_number_purchases'] = df['order_num_total_ever_online'] + df['order_num_total_ever_offline']
df['total_price'] = df['customer_value_total_ever_offline'] + df['customer_value_total_ever_online']
df.head()


### Adım 4: Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz
df.info()


# 1. yol
df['first_order_date'] = pd.to_datetime(df['first_order_date'])
df['last_order_date'] = pd.to_datetime(df['last_order_date'])
df['last_order_date_online'] = pd.to_datetime(df['last_order_date_online'])
df['last_order_date_offline'] = pd.to_datetime(df['last_order_date_offline'])


date_columns = ["first_order_date", "last_order_date",  "last_order_date_online", "last_order_date_offline"]
df[date_columns] = df[date_columns].astype("datetime64[ns]")

date_columns = df.columns [df.columns.str.contains("date")]
df[date_columns] = df[date_columns].apply(pd.to_datetime)

date_columns_ = [col for col in df.columns if 'date' in col]

# 2. yol
df[date_columns_] = df[date_columns_].apply(pd.to_datetime, errors='coerce')

# 3. yol
date_columns = df.columns[df.columns.str.contains("date")]
df[date_columns] = df[date_columns].apply(pd.to_datetime, errors='coerce')

df.info()


### Adım 5: Alışveriş kanallarındaki müşteri sayısının, toplam alınan ürün sayısının ve toplam harcamaların dağılımına bakınız.

df.groupby('order_channel').agg({'master_id': 'count',
                                 'total_number_purchases': 'sum',
                                 'total_price': 'sum'})


### Adım 6: En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.
df.sort_values(by='total_price', ascending=False)[:10] #.head()

df.groupby('master_id').agg({'total_price': 'sum'}).sort_values('total_price', ascending=False).head(10)


### Adım 7: En fazla siparişi veren ilk 10 müşteriyi sıralayınız.
df.sort_values(by='total_number_purchases', ascending=False)[:10] #.head()

df.groupby('master_id').agg({'total_number_purchases': 'sum',}).sort_values('total_number_purchases', ascending=False).head(10)



### Adım 8: Veri ön hazırlık sürecini fonksiyonlaştırınız.
def data_preprocessing(dataframe):
    dataframe['total_number_purchases'] = dataframe['order_num_total_ever_online'] + dataframe['order_num_total_ever_offline']
    dataframe['total_price'] = dataframe['customer_value_total_ever_offline'] + dataframe['customer_value_total_ever_online']
    date_columns = dataframe.columns[dataframe.columns.str.contains("date")]
    dataframe[date_columns] = dataframe[date_columns].apply(pd.to_datetime, errors='coerce')

    return dataframe


#############################################
# GÖREV 2  RFM Metriklerinin Hesaplanması
#############################################

### Adım 1: Recency, Frequency ve Monetary tanımlarını yapınız.
# Recency (analizin yapıldığı tarih - müşterinin son satın alma tarihi),
# Frequency (Müşterinin yaptığı toplam satın alma),
# Monetary (Müşterinin bıraktığı toplam para)

### Adım 2: Müşteri özelinde Recency, Frequency ve Monetary metriklerini hesaplayınız.

# Recency i hesaplamak için bir tarih belirlememiz gerekiyor.
df['last_order_date'].max()

# Recency değerini hesaplamak için analiz tarihini maksimum tarihten 2 gün sonrası seçebilirsiniz.
today_date = dt.datetime(2021,6,1)



df.groupby('master_id').agg({'last_order_date': lambda lod: (today_date - lod.max()).days,
                                   'total_number_purchases': lambda x : x.sum(),
                                   'total_price': lambda x : x.sum()})



### Adım 3: Hesapladığınız metrikleri rfm isimli bir değişkene atayınız.
rfm = df.groupby('master_id').agg({'last_order_date': lambda lod: (today_date - lod.max()).days,
                                   'total_number_purchases': lambda x : x.sum(),
                                   'total_price': lambda x : x.sum()})
rfm.head()


### Adım 4: Oluşturduğunuz metriklerin isimlerini  recency, frequency ve monetary olarak değiştiriniz.
rfm.columns = ['recency','frequency','monetary']


#########################################
# GÖREV 3:  RF Skorunun Hesaplanması
#########################################

### Adım 1: Recency, Frequency ve Monetary metriklerini qcut yardımı ile 1-5 arasında skorlara çeviriniz.
### Adım 2: Bu skorları recency_score, frequency_scoreve monetary_score olarak kaydediniz.

rfm['recency_score'] = pd.qcut(rfm['recency'],5, labels=[5,4,3,2,1])
rfm['frequency_score'] = pd.qcut(rfm['frequency'].rank(method='first'),5, labels=[1,2,3,4,5])
rfm['monetary_score'] = pd.qcut(rfm['monetary'],5, labels=[1,2,3,4,5])

### Adım 3: recency_score ve frequency_score’u tek bir değişken olarak ifade ediniz ve RF_SCORE olarak kaydediniz.
rfm['RF_SCORE'] = rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str)

rfm.info()
rfm.head()


#######################################################
# Görev 4:  RF Skorunun Segment Olarak Tanımlanması
#######################################################

### Adım 1: Oluşturulan RF skorları için segment tanımlamaları yapınız.
### Adım 2: Aşağıdaki seg_map yardımı ile skorları segmentlere çeviriniz.

# regex yapısı
seg_map = {
    r'[1-2][1-2]' : 'hibernating',
    r'[1-2][3-4]' : 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]' : 'about_to_sleep',
    r'33' : 'need_attention',
    r'[3-4][4-5]' : 'loyal_customers',
    r'41' : 'promising',
    r'51' : 'new_customers',
    r'[4-5][2-3]' : 'potential_loyalists',
    r'5[4-5]' : 'champions'
}


rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)

rfm.head()
rfm.reset_index(inplace=True)


################################
# Görev 5:  Aksiyon Zamanı !
################################

### Adım1:  Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleyiniz
rfm[['segment','recency','frequency','monetary']].groupby('segment').agg({'recency':['mean','count'],
                                                                        'frequency':['mean','count'],
                                                                        'monetary':['mean','count']})




### Adım 2: RFM analizi yardımıyla aşağıda verilen 2 case için ilgili profildeki müşterileri bulun ve müşteri id'lerini csv olarak kaydediniz.

# a. FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri
#  tercihlerinin üstünde. Bu nedenle markanın tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak
#  iletişime geçmek isteniliyor. Sadık müşterilerinden(champions, loyal_customers) ve kadın kategorisinden alışveriş
#  yapan kişiler özel olarak iletişim kurulacak müşteriler. Bu müşterilerin id numaralarını csv dosyasına kaydediniz.


target_segment_ids = rfm[rfm['segment'].isin(['champions','loyal_customers'])]['master_id']

target_customers_id = df[(df['master_id'].isin(target_segment_ids)) & (df['interested_in_categories_12'].str.contains('KADIN'))]['master_id']
target_customers_id.to_csv('yeni_marka_hedef_müsteri_id.csv', index=False)



# b. Erkek ve Çocuk ürünlerinde %40'a yakın indirim planlanmaktadır. Bu indirimle ilgili kategorilerle ilgilenen geçmişte
# iyi müşteri olan ama uzun süredir alışveriş yapmayan kaybedilmemesi gereken müşteriler, uykuda olanlar ve yeni
# gelen müşteriler özel olarak hedef alınmak isteniyor. Uygun profildeki müşterilerin id'lerini csv dosyasına kaydediniz

target_segment_ids_ = rfm[rfm['segment'].isin(['hibernating','at_Risk','cant_loose', 'about_to_sleep', 'new_customers'])]['master_id']

target_customers_id_ = df[(df['master_id'].isin(target_segment_ids_)) &
                          ((df['interested_in_categories_12'].str.contains('ERKEK')) |
                           ((df['interested_in_categories_12'].str.contains('ÇOCUK'))
                           ))]['master_id']

target_customers_id_.to_csv("indirim_hedef_müsteri_id.csv", index=False)




##################################
# BONUS
##################################

def rfm_create(dataframe):

    # Veriyi Hazırlama
    dataframe['total_number_purchases'] = dataframe['order_num_total_ever_online'] + dataframe['order_num_total_ever_offline']
    dataframe['total_price'] = dataframe['customer_value_total_ever_offline'] + dataframe['customer_value_total_ever_online']
    date_columns = dataframe.columns[dataframe.columns.str.contains("date")]
    dataframe[date_columns] = dataframe[date_columns].apply(pd.to_datetime, errors='coerce')

    # RFM Metriklerinin Hesaplanması
    df['last_order_date'].max()
    today_date = dt.datetime(2021, 6, 1)
    rfm = df.groupby('master_id').agg({'last_order_date': lambda lod: (today_date - lod.max()).days,
                                       'total_number_purchases': lambda x: x.sum(),
                                       'total_price': lambda x: x.sum()})
    rfm.columns = ['recency', 'frequency', 'monetary']

    # RF Skorunun Hesaplanması

    rfm['recency_score'] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])
    rfm['frequency_score'] = pd.qcut(rfm['frequency'].rank(method='first'), 5, labels=[1, 2, 3, 4, 5])
    rfm['monetary_score'] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])
    rfm['RF_SCORE'] = rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str)

    # Segmentation
    seg_map = {
        r'[1-2][1-2]': 'hibernating',
        r'[1-2][3-4]': 'at_Risk',
        r'[1-2]5': 'cant_loose',
        r'3[1-2]': 'about_to_sleep',
        r'33': 'need_attention',
        r'[3-4][4-5]': 'loyal_customers',
        r'41': 'promising',
        r'51': 'new_customers',
        r'[4-5][2-3]': 'potential_loyalists',
        r'5[4-5]': 'champions'
    }

    rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)
    rfm.reset_index(inplace=True)

    return rfm
