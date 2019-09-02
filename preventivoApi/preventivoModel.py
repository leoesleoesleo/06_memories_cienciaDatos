# -*- coding: utf-8 -*-
"""
Created on Mon May 27 16:00:44 2019

@author: leonardo.patino
"""
import pandas as pd
import numpy as np
import math
import datetime
import mysql.connector
import sqlalchemy # Import dataframe into MySQL
#from sklearn.preprocessing import Normalizer
import subprocess

#insertar bd
def insert_bd(database_ip,database_username,database_password,database_name,name_table,df_data,metodo):
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))
    df_data.to_sql(con=database_connection, name=name_table, if_exists=metodo)
    return 'registros Insertados'


def insumo(train,cliente,fecha):
    query_tuneo          = "SELECT a.*,b.nombre FROM conf_tuneo a JOIN conf_clientes b on a.id_cliente = b.id_cliente where nombre = " + "'" + cliente + "'" + ""
    query_correo         = "SELECT ruta FROM conf_conf WHERE id = 2"
    query_estim          = "SELECT *,CONVERT(intervalo, time) as intervalocast FROM df_pandas_estimadores"
    if train == 1:
        query_vol        = "SELECT *,CAST(fecha as CHAR(50)) as fechacast FROM v_dataframe_vol where fecha between STR_TO_DATE(date_format(date_sub(now(),interval 7 month), '%d/%m/%Y'),'%d/%m/%Y') and  STR_TO_DATE(date_format(now(), '%d/%m/%Y'),'%d/%m/%Y')"
        query_oper       = "SELECT * FROM v_dataframe_oper_ where fecha between STR_TO_DATE(date_format(date_sub(now(),interval 7 month), '%d/%m/%Y'),'%d/%m/%Y') and  STR_TO_DATE(date_format(now(), '%d/%m/%Y'),'%d/%m/%Y')"
        query_estim_oper = "none"
    else:
        query_vol        = "SELECT *,CAST(fecha as CHAR(50)) as fechacast FROM v_dataframe_vol where fecha = " + "'" + fecha + "'" + ""
        query_oper       = "SELECT * FROM v_dataframe_oper_ where fecha = " + "'" + fecha + "'" + ""
        query_estim_oper = "SELECT *,CONVERT(intervalo, time) as intervalocast FROM df_pandas_estimadores_oper"
    return query_vol,query_oper,query_estim,query_estim_oper,query_tuneo,query_correo   
       
#EJECUTAR QUERYS
def ejecutar_querys(train,database_ip,database_username,database_password,database_name,query_vol,query_oper,query_estim,query_estim_oper,query_tuneo,query_correo):    
    mydb = mysql.connector.connect(
      host=database_ip, 
      user=database_username,
      passwd=database_password,
      database=database_name
    )
    mycursor = mydb.cursor()

    mycursor.execute(query_vol)
    myresult_vol  = mycursor.fetchall() #método, que recupera todas las filas de la última instrucción ejecutada.
    data_vol      = pd.DataFrame(data=myresult_vol)
    data_vol      = data_vol.rename(columns = {1:'cliente',2:'fecha',3:'dia_pago',4:'nom_dia',5:'intervalo',6:'llamadas',7:'volumenes',8:'transacciones',9:'fechacast'})
    
    mycursor.execute(query_oper)
    myresult_oper = mycursor.fetchall() 
    data_oper     = pd.DataFrame(data=myresult_oper)
    data_oper     = data_oper.rename(columns = {0:'cliente',1:'fecha',2:'dia_pago',3:'nom_dia',4:'intervalo',5:'transacciones',6:'fechacast',7:'cod_tran',8:'cod_rst_oper'})
    
    mycursor.execute(query_tuneo)
    myresult_tuneo = mycursor.fetchall() 
    data_tuneo     = pd.DataFrame(data=myresult_tuneo)
    
    mycursor.execute(query_correo)
    myresult_correo = mycursor.fetchall() 
    data_correo     = pd.DataFrame(data=myresult_correo)
    
    mycursor.execute(query_estim)
    myresult_estim = mycursor.fetchall() 
    data_estim     = pd.DataFrame(data=myresult_estim)
    data_estim     = data_estim.rename(columns = {1:'intervalo',2:'de_llam',3:'de_vol',4:'de_tran',5:'media_llam',6:'media_vol',7:'media_tran',8:'cliente',9:'fecha',10:'intervalocast'})
    
    if train != 1:    
        mycursor.execute(query_estim_oper)
        myresult_estim_oper = mycursor.fetchall() 
        data_estim_oper     = pd.DataFrame(data=myresult_estim_oper)
        data_estim_oper     = data_estim_oper.rename(columns = {1:'intervalo',2:'transacciones',3:'cod_tran',4:'rslt',5:'de_tran',6:'media_tran',7:'cliente',8:'fecha',9:'intervalocast'})
    else:
        data_estim_oper  = 0
    
    return data_vol,data_oper,data_tuneo,data_estim,data_estim_oper,data_correo           


def calcularestimadores_vol(data,estimadores,intervaloini,incremento,intervalofin,intervalofinestim,nom_dia,dia_pago,fecha,train,incremen_estimadorLlam,incremen_estimadorVol,incremen_estimadorTran,incremen_estimadorLlamMin,database_ip,database_username,database_password,database_name):
    #fechas con formato
    f_intervaloini      = datetime.timedelta(hours=int(intervaloini[:2]),minutes=int(intervaloini[3:5]))
    f_incremento        = datetime.timedelta(hours=int(incremento[:2]),minutes=int(incremento[3:5]))
    f_intervalofin      = datetime.timedelta(hours=int(intervalofin[:2]),minutes=int(intervalofin[3:5]))
    f_intervalofinestim = datetime.timedelta(hours=int(intervalofinestim[:2]),minutes=int(intervalofinestim[3:5]))
    
    list_llam = []
    list_vol = []
    list_tran = []
    
    list_intervalo = []
    list_nom_dia = []
    list_dia_pago = []

    list_de_llam = []
    list_de_vol = []
    list_de_tran = []

    list_media_llam = []
    list_media_vol = []
    list_media_tran = []

    list_cvmax_llam = []
    list_cvmax_vol = []
    list_cvmax_tran = []
    list_cvmin_llam = []
    
    list_cvmaxporce_llam = []
    list_cvmaxporce_vol = []
    list_cvmaxporce_tran = []
    list_cvminporce_llam = []
    
    list_cliente = []
    list_fecha   = []    
    #data = data.loc[:, ['llamadas', 'volumenes','transacciones']]
    
    fecha_estim = estimadores.at[0,'fecha']
    if train == 0 and fecha_estim != fecha:
        train = 1
        
    while f_intervaloini <= f_intervalofinestim: 
        llam_dataframe = data[(data['nom_dia'].isin([nom_dia]) & data['dia_pago'].isin([dia_pago]) & data['intervalo'].isin([f_intervaloini]) & data['fechacast'].isin([fecha]))]
        
        t = int(len(llam_dataframe.index))
        #VARIABLES
        if t == 0:
            llam    = 0
            vol     = 0
            tran    = 0
            cliente = 'none'
        else:
            llam = llam_dataframe.iloc[0,6]
            vol = llam_dataframe.iloc[0,7]
            tran = llam_dataframe.iloc[0,8] 
            cliente = llam_dataframe.iloc[0,1]
        
        list_llam.append(llam)
        list_tran.append(tran)
        list_cliente.append(cliente) 
            
        if train == 1:
            de_dataframe      = data[(data['nom_dia'].isin([nom_dia]) & data['dia_pago'].isin([dia_pago]) & data['intervalo'].isin([f_intervaloini]))].std()
            media_dataframe   = data[(data['nom_dia'].isin([nom_dia]) & data['dia_pago'].isin([dia_pago]) & data['intervalo'].isin([f_intervaloini]))].mean()
            mediana_dataframe = data[(data['nom_dia'].isin([nom_dia]) & data['dia_pago'].isin([dia_pago]) & data['intervalo'].isin([f_intervaloini]))].median()
            # D E                        
            de_llam = de_dataframe[3]            
            de_vol = de_dataframe[4]                
            de_tran = de_dataframe[5]                 
            list_de_llam.append(de_llam) 
            list_de_vol.append(de_vol)   
            list_de_tran.append(de_tran)
            
            # MEDIA
            media_llam = media_dataframe[3]                
            media_vol = media_dataframe[4]
            media_tran = mediana_dataframe[5]                  
            list_media_llam.append(media_llam)  
            list_media_vol.append(media_vol)    
            list_media_tran.append(media_tran)
        else:
            de_dataframe = estimadores[(estimadores['intervalocast'].isin([f_intervaloini]))]
            media_dataframe = estimadores[(estimadores['intervalocast'].isin([f_intervaloini]))]
            # D E
            de_llam = de_dataframe.iloc[0,2]
            list_de_llam.append(de_llam)    
            de_vol = de_dataframe.iloc[0,3]
            list_de_vol.append(de_vol)    
            de_tran = de_dataframe.iloc[0,4]
            list_de_tran.append(de_tran)    
            # MEDIA
            media_llam = media_dataframe.iloc[0,5]
            list_media_llam.append(media_llam)    
            media_vol = media_dataframe.iloc[0,6]
            list_media_vol.append(media_vol)    
            media_tran = media_dataframe.iloc[0,7]
            list_media_tran.append(media_tran)
            
        list_intervalo.append(str(f_intervaloini))
        list_nom_dia.append(nom_dia)  
        list_dia_pago.append(dia_pago)
        list_fecha.append(fecha)
        
        #PROYECTAR VOL        
        diff = f_intervalofin-f_intervaloini
        limi = '06:50:00' #cuando llegue a esta diferencia de intervalo proyecta vol
        f_limi = datetime.timedelta(hours=int(limi[:2]),minutes=int(limi[3:5]))
        
        if diff <= f_limi and f_intervalofin >= f_intervaloini:
            if vol < (media_vol-de_vol):
                vol = media_vol

        list_vol.append(vol)       
        
        # C V MAX
        cvmax_llam = (de_llam*incremen_estimadorLlam)+media_llam
        list_cvmax_llam.append(cvmax_llam)  
        if llam == 0:
            cvmax_porce_llam = 0
        else:
            cvmax_porce_llam = 1-(cvmax_llam/llam)
        list_cvmaxporce_llam.append(cvmax_porce_llam)
        
        cvmax_vol = (de_vol*incremen_estimadorVol)+media_vol
        list_cvmax_vol.append(cvmax_vol)  
        if vol ==0:
           cvmax_porce_vol = 0
        else:
            cvmax_porce_vol = 1-(cvmax_vol/vol)
        list_cvmaxporce_vol.append(cvmax_porce_vol)
        
        cvmax_tran = (de_tran*incremen_estimadorTran)+media_tran
        #eliminar atipicos        
        if cvmax_tran >= 20:
            cvmax_tran = 20
        if cvmax_tran <= 5:
            cvmax_tran = 5
            
        list_cvmax_tran.append(cvmax_tran)
        if tran == 0:
            cvmax_porce_tran = 0
        else:
            cvmax_porce_tran = 1-(cvmax_tran/tran)
        list_cvmaxporce_tran.append(cvmax_porce_tran)
        
        # C V MIN   
        cvmin_llam = (media_llam-de_llam)/incremen_estimadorLlamMin
        list_cvmin_llam.append(cvmin_llam)  
        if llam == 0:
            cvminporce_llam = 0
        else:
            cvminporce_llam = 1-(cvmin_llam/llam)
        list_cvminporce_llam.append(cvminporce_llam)
    
        f_intervaloini = f_intervaloini + f_incremento    
    
    if train == 1:
        #dataframe estimadores
        d = {
             'intervalo'        : list_intervalo, 
             'de_llam'          : list_de_llam, 
             'de_vol'           : list_de_vol, 
             'de_tran'          : list_de_tran,
             'media_llam'       : list_media_llam, 
             'media_vol'        : list_media_vol, 
             'media_tran'       : list_media_tran,
             'cliente'          : list_cliente,
             'fecha'            : list_fecha
             }
    
        df_ = pd.DataFrame(data=d)
        df_ = df_.fillna(0)
        metodo = 'replace'
        name_table = 'df_pandas_estimadores'
        insert_bd(database_ip,database_username,database_password,database_name,name_table,df_,metodo)
        
    #contruir nuevo dataframe
    d = {
         'intervalo'        : list_intervalo, 
         'nom_dia'          : list_nom_dia, 
         'dia_pago'         : list_dia_pago, 
         'llam'             : list_llam, 
         'vol'              : list_vol, 
         'tran'             : list_tran,
         'de_llam'          : list_de_llam, 
         'de_vol'           : list_de_vol, 
         'de_tran'          : list_de_tran,
         'media_llam'       : list_media_llam, 
         'media_vol'        : list_media_vol, 
         'media_tran'       : list_media_tran,
         'cvmax_llam'       : list_cvmax_llam,
         'cvmaxporce_llam'  : list_cvmaxporce_llam,
         'cvmax_vol'        : list_cvmax_vol,
         'cvmaxporce_vol'   : list_cvmaxporce_vol,
         'cvmax_tran'       : list_cvmax_tran,
         'cvmaxporce_tran'  : list_cvmaxporce_tran,
         'cvmin_llam'       : list_cvmin_llam,
         'cvminporce_llam'  : list_cvminporce_llam,
         'cliente'          : list_cliente,
         'fecha'            : list_fecha
         }
    
    df = pd.DataFrame(data=d)
    df = df.fillna(0)
    return df


def calcularestimadores_oper(data,intervaloini,incremento,intervalofin,nom_dia,dia_pago,fecha,train,database_ip,database_username,database_password,database_name):
    #fechas con formato
    f_intervaloini = datetime.timedelta(hours=int(intervaloini[:2]),minutes=int(intervaloini[3:5]))
    f_incremento = datetime.timedelta(hours=int(incremento[:2]),minutes=int(incremento[3:5]))
    f_intervalofin = datetime.timedelta(hours=int(intervalofin[:2]),minutes=int(intervalofin[3:5]))

    list_intervalo      = []
    list_nom_dia        = []
    list_dia_pago       = []
    list_cod_tran       = []
    list_rslt           = []

    list_tran           = []
    list_de_tran        = []
    list_media_tran     = []
    list_cvmaxporce_tran= []
    list_cvminporce_tran= []
    
    list_cliente        = []
    list_fecha          = []
        
    while f_intervaloini <= f_intervalofin:    
        llam_dataframe = data[(data['nom_dia'].isin([nom_dia]) & data['dia_pago'].isin([dia_pago]) & data['intervalo'].isin([f_intervaloini]) & data['fechacast'].isin([fecha]))]
            
        n = len(llam_dataframe)
        i = 0
        
        while i < n:
            media_dataframe = data[(data['nom_dia'].isin([nom_dia]) & data['dia_pago'].isin([dia_pago]) & data['intervalo'].isin([f_intervaloini]) & data['cod_tran'].isin([llam_dataframe.iloc[i,7]]) & data['cod_rst_oper'].isin([llam_dataframe.iloc[i,8]]) )].mean()
            de_dataframe = data[(data['nom_dia'].isin([nom_dia]) & data['dia_pago'].isin([dia_pago]) & data['intervalo'].isin([f_intervaloini]) & data['cod_tran'].isin([llam_dataframe.iloc[i,7]]) & data['cod_rst_oper'].isin([llam_dataframe.iloc[i,8]]) )].std()
            # D E            
            de_tran = de_dataframe['transacciones']
            if math.isnan(de_tran):
                de_tran = 0
            list_de_tran.append(de_tran)    
            # MEDIA            
            media_tran = media_dataframe['transacciones']
            if math.isnan(media_tran):
                media_tran = 0
            list_media_tran.append(media_tran)

            #VARIABLES
            tran =      llam_dataframe.iloc[i,5]
            cod_tran =  llam_dataframe.iloc[i,7]
            rslt =      llam_dataframe.iloc[i,8]
            cliente =   llam_dataframe.iloc[i,0]
            
            list_tran.append(tran) 
            list_cod_tran.append(cod_tran)
            list_rslt.append(rslt)
            list_cliente.append(cliente)
            list_fecha.append(fecha)
            
            # C V MAX
            try:
                cvmaxporce_tran = 1-(tran/(media_tran + de_tran))
                #cvmaxporce_tran = media_tran + de_tran
            except ZeroDivisionError:
                cvmaxporce_tran = 0
            list_cvmaxporce_tran.append(cvmaxporce_tran) 
            # C V MIN 
            try:
                cvminporce_tran = 1-(tran/(media_tran - de_tran))
                #cvminporce_tran = media_tran - de_tran
            except ZeroDivisionError:
                cvminporce_tran = 0                
            list_cvminporce_tran.append(cvminporce_tran)
                    
            list_intervalo.append(str(f_intervaloini))
            list_nom_dia.append(nom_dia)  
            list_dia_pago.append(dia_pago) 
        
            i = i + 1
    
        f_intervaloini = f_intervaloini + f_incremento

    d = {
         'intervalo'        : list_intervalo, 
         'nom_dia'          : list_nom_dia, 
         'dia_pago'         : list_dia_pago, 
         'tran'             : list_tran,
         'cod_tran'         : list_cod_tran,
         'rslt'             : list_rslt,  
         'de_tran'          : list_de_tran,
         'media_tran'       : list_media_tran,
         'cvmaxporce_tran'  : list_cvmaxporce_tran,
         'cvminporce_tran'  : list_cvminporce_tran,
         'cliente'          : list_cliente,
         'fecha'            : list_fecha
         }

    df_ = pd.DataFrame(data=d)
    metodo = 'replace'
    name_table = 'df_pandas_estimadores_oper'
    insert_bd(database_ip,database_username,database_password,database_name,name_table,df_,metodo)

    df = pd.DataFrame(data=d)
    return df
    
"""
#NORMALIZAR
atributos = df.loc[:,'intervalo':'media_tran']
estimadores = df.loc[:,'cvmaxporce_llam':'cvminporce_tran']
normalize = Normalizer().fit(estimadores)
estimadores_n = normalize.transform(estimadores)
df_estimadores = np.concatenate((atributos, estimadores_n), axis=1)
"""

def algoritmo_oper(data,intervalo,fecha):
    
    list_intervalo   = []
    list_tran        = []
    list_max_tran    = []
    list_cv_max_tran = []
    list_cod_tran    = []
    list_rslt        = []
    list_cliente     = []
    
    n = len(data)
    i = 0
    while i < n:
        if data.iloc[i,0] == intervalo:
            list_intervalo.append(data.iloc[i,0])
            list_tran.append(data.iloc[i,3])
            list_max_tran.append(data.iloc[i,6] + data.iloc[i,7])
            list_cv_max_tran.append(data.iloc[i,8])
            list_cod_tran.append(data.iloc[i,4])
            list_rslt.append(data.iloc[i,5])
            list_cliente.append(data.iloc[i,10])
        i = i + 1    
        
    d = {
         'intervalo'        : list_intervalo, 
         'tran'             : list_tran,
         'max_tran'         : list_max_tran,
         'cv_max_tran'      : list_cv_max_tran,
         'cod_tran'         : list_cod_tran,
         'rslt'             : list_rslt,
         'cliente'          : list_cliente,
         'fecha'            : fecha           
         }
    df = pd.DataFrame(data=d)
    #df = df.sort_values(['cv_max_tran'], ascending=[1])
    #return df[0:10]
    return df

def armar_reglas(param):
    if 'le' in param and 'te' in param:
        p = 'lete'
        s = 'Naranja'
        c = 6
    elif 'ld' in param and 've' in param:
        p = 'ldve'
        s = 'Naranja'
        c = 7
    elif 'le' in param and 've' in param:
        p = 'leve'
        s = 'Naranja'
        c = 5
    elif 'le' in param:
        p = 'le'
        s = 'Verde'
        c = 1
    elif 'te' in param:
        p = 'te'
        s = 'Amarillo'
        c = 2
    elif 've' in param:
        p = 've'
        s = 'Verde'
        c = 3
    elif 'ld' in param:
        p = 'ld'
        s = 'Verde'
        c = 4
    else:
        p = ''
        s = ''
        c = ''
    return p,s,c    

def algoritmo_preventivo(df,data,factor1,factor2,factor3,inter_deslizante,factor_estacional,fecha,database_ip,database_username,database_password,database_name,intervaloini,incremento,intervalofin,nom_dia,dia_pago,train):
    
    list_intervalo          = []
    list_nom_dia            = []
    list_dia_pago           = []
        
    vector_alerta           = []
    vector_param            = []
    vector_env_param        = [] 
    vector_semaforo         = []
    vector_correo           = []
    vector_componente       = []
    
    vector_res_le           = []
    vector_reiteratividad_le= []        

    vector_res_ld           = []
    vector_reiteratividad_ld= []

    vector_res_ve           = []
    vector_reiteratividad_ve= []
    
    vector_res_te           = []
    vector_reiteratividad_te= []  
    
    vector_tran_afectadas   = []
    
    list_llam               = []
    list_vol                = []
    list_tran               = []    
    list_dellam             = []
    list_devol              = []
    list_detran             = []
    list_mediallam          = []
    list_mediavol           = []
    list_mediatran          = []
    list_cvmax_llam         = []
    list_cvmax_vol          = []
    list_cvmax_tran         = []
    list_cvmaxporce_llam    = []
    list_cvmaxporce_vol     = []
    list_cvmaxporce_tran    = []
    list_cvmin_llam         = []
    list_cvminporce_llam    = []
    
    list_cliente            = []
    list_fecha              = []
    
    n = len(df)
    i = 0
    
    df_oper = calcularestimadores_oper(data,intervaloini,incremento,intervalofin,nom_dia,dia_pago,fecha,train,database_ip,database_username,database_password,database_name)
                
    while i < n:
    #te    
        #res
        if df.iloc[i,17] >= factor3:
            res_te = 4
        elif df.iloc[i,17] >= factor2:
            res_te = 3
        elif df.iloc[i,17] >= factor1: 
            res_te = 2
        elif df.iloc[i,17] > 0:    
            res_te = 1
        else:
            res_te = 0  
        vector_res_te.append(res_te)     
        #reiteratividadAcumulada 
        if i >= inter_deslizante: #iniciar en este intervalo
            if res_te == 0:
                reiteratividad_te = 0
            else:
                reiteratividad_te = sum(vector_res_te[(i-inter_deslizante):(i+1)])
        else:
            reiteratividad_te = 0      
        vector_reiteratividad_te.append(reiteratividad_te)
        #parametroEnviado
        if reiteratividad_te >= factor_estacional:
            param_te = 'te'
            
            #proceso oper
            tran_afectadas = algoritmo_oper(df_oper,df.iloc[i,0],fecha)
            df_ = tran_afectadas
            metodo = 'replace'
            name_table = 'df_pandas_tran'
            insert_bd(database_ip,database_username,database_password,database_name,name_table,df_,metodo)
            vector_tran_afectadas.append(1) 
            
        else:
            param_te = ''                
            
    #le    
        #res
        if df.iloc[i,13] >= factor3:
            res_le = 4
        elif df.iloc[i,13] >= factor2:
            res_le = 3
        elif df.iloc[i,13] >= factor1: 
            res_le = 2
        elif df.iloc[i,13] > 0:
            res_le = 1
        else:
            res_le = 0 
        vector_res_le.append(res_le)    
        #reiteratividadAcumulada 
        if i >= inter_deslizante: #iniciar en este intervalo
            if res_le == 0:
                reiteratividad_le = 0
            else:
                reiteratividad_le = sum(vector_res_le[(i-inter_deslizante):(i+1)])
        else:
            reiteratividad_le = 0      
        vector_reiteratividad_le.append(reiteratividad_le)
        #parametroEnviado
        if reiteratividad_le >= factor_estacional:
            param_le = 'le'            
        else:
            param_le = ''

    #ld    
        #res
        if df.iloc[i,19] <= (-factor3):
            res_ld = 4
        elif df.iloc[i,19] <= (-factor2):
            res_ld = 3
        elif df.iloc[i,19] <= (-factor1): 
            res_ld = 2
        elif df.iloc[i,19] < 0:
            res_ld = 1
        else:
            res_ld = 0     
        vector_res_ld.append(res_ld)     
        #reiteratividadAcumulada 
        if i >= inter_deslizante: #iniciar en este intervalo
            if res_ld == 0:
                reiteratividad_ld = 0
            else:
                reiteratividad_ld = sum(vector_res_ld[(i-inter_deslizante):(i+1)])
        else:
            reiteratividad_ld = 0      
        vector_reiteratividad_ld.append(reiteratividad_ld)
        #parametroEnviado
        if reiteratividad_ld >= factor_estacional:
            param_ld = 'ld'            
        else:
            param_ld = '' 
    
    #ve    
        #res
        if df.iloc[i,15] >= factor3:
            res_ve = 4
        elif df.iloc[i,15] >= factor2:
            res_ve = 3
        elif df.iloc[i,15] >= factor1: 
            res_ve = 2
        elif df.iloc[i,15] > 0:
            res_ve = 1
        else:
            res_ve = 0 
        vector_res_ve.append(res_ve)     
        #reiteratividadAcumulada 
        if i >= inter_deslizante: #iniciar en este intervalo
            if res_ve == 0:
                reiteratividad_ve = 0
            else:
                reiteratividad_ve = sum(vector_res_ve[(i-inter_deslizante):(i+1)])
        else:
            reiteratividad_ve = 0      
        vector_reiteratividad_ve.append(reiteratividad_ve)
        #parametroEnviado
        if reiteratividad_ve >= factor_estacional:
            param_ve = 've'            
        else:
            param_ve = ''         
           
        #ARMAR REGLAS            
        param = param_le,param_ld,param_ve,param_te    
        vector_param.append(param)
        
        p = armar_reglas(param)[0]
        s = armar_reglas(param)[1]
        c = armar_reglas(param)[2]
        
        vector_env_param.append(p)
        vector_semaforo.append(s)
        vector_componente.append(c)
        vector_correo.append(0)

        alerta = df.iloc[i,3] #llam  
            
        vector_alerta.append(alerta) 
        list_cliente.append(df.iloc[i,20])
        list_intervalo.append(df.iloc[i,0])
        list_fecha.append(df.iloc[i,21])
        list_nom_dia.append(df.iloc[i,1])
        list_dia_pago.append(df.iloc[i,2])        
        list_llam.append(df.iloc[i,3])
        list_vol.append(df.iloc[i,4])
        list_tran.append(df.iloc[i,5])
        
        list_dellam.append(df.iloc[i,6])
        list_devol.append(df.iloc[i,7])
        list_detran.append(df.iloc[i,8])
        list_mediallam.append(df.iloc[i,9])
        list_mediavol.append(df.iloc[i,10])
        list_mediatran.append(df.iloc[i,11])
        
        list_cvmax_llam.append(df.iloc[i,12])
        list_cvmaxporce_llam.append(df.iloc[i,13])
        
        list_cvmax_vol.append(df.iloc[i,14])
        list_cvmaxporce_vol.append(df.iloc[i,15])
        
        list_cvmax_tran.append(df.iloc[i,16])
        list_cvmaxporce_tran.append(df.iloc[i,17])
        
        list_cvmin_llam.append(df.iloc[i,18])
        list_cvminporce_llam.append(df.iloc[i,19])
        
        #print("Intervalo: ", list_intervalo,"Index: ",i)
        i = i + 1
        
    #contruir nuevo dataframe
    d = {
         'cliente'          : list_cliente,       
         'list_intervalo'   : list_intervalo, 
         'fecha'            : list_fecha,
         'list_nom_dia'     : list_nom_dia,  
         'list_dia_pago'    : list_dia_pago, 
         'llam'             : list_llam,
         'vol'              : list_vol,
         'tran'             : list_tran,
         'de_llam'          : list_dellam,
         'de_vol'           : list_devol,
         'de_tran'          : list_detran,
         'media_llam'       : list_mediallam,
         'media_vol'        : list_mediavol,
         'media_tran'       : list_mediatran,
         'cvmax_tran'       : list_cvmax_tran,
         'cvmaxporce_tran'  : list_cvmaxporce_tran,
         'res_te'           : vector_res_te, 
         'reiteratividad_te': vector_reiteratividad_te,
         'cvmax_llam'       : list_cvmax_llam,
         'cvmaxporce_llam'  : list_cvmaxporce_llam,         
         'res_le'           : vector_res_le, 
         'reiteratividad_le': vector_reiteratividad_le,
         'cvmin_llam'       : list_cvmin_llam,
         'cvminporce_llam'  : list_cvminporce_llam,         
         'res_ld'           : vector_res_ld, 
         'reiteratividad_ld': vector_reiteratividad_ld,
         'cvmax_vol'        : list_cvmax_vol,
         'cvmaxporce_vol'   : list_cvmaxporce_vol, 
         'res_ve'           : vector_res_ve, 
         'reiteratividad_ve': vector_reiteratividad_ve,        
         'vector_alerta'    : vector_alerta,
         'vector_env_param' : vector_env_param,
         'vector_semaforo'  : vector_semaforo,
         'componente'       : vector_componente,
         'correo'           : vector_correo
         }
    
    df_ = pd.DataFrame(data=d)
    """
    metodo = 'replace'
    name_table = 'df_pandas_vol'
    insert_bd(database_ip,database_username,database_password,database_name,name_table,df_,metodo)
    """    
    return df_,vector_tran_afectadas


def generar_alertas(df,database_ip,database_username,database_password,database_name,data_correo):    
    
    metodo = 'replace'
    name_table = 'df_pandas_vol'
    insert_bd(database_ip,database_username,database_password,database_name,name_table,df,metodo)
    
    i = 0
    while df.iloc[i,0] != 'none':
        i = i + 1
    i = i - 1
    lim = 1  # cantidad de intervalos a rescatar en el envio de correo alerta
    
    if i >= lim:
        fila = i - lim
    else:
        fila = 0
    
    while fila < i:
        if df.iloc[fila, 32] != "" and df.iloc[fila, 34] == 0:

            #param_cliente =              df.iloc[fila, 0]
            param_intervalo =            df.iloc[fila, 1]        
            param_componente =           df.iloc[fila, 33]
            
            #query_rutacorreo = "C:/Users/leonardo.patino/Desktop/tareas programadas/alertas_vru_v3_0.1/alertas_vru_v3/alertas_vru_v3_run.bat"
            query_rutacorreo = data_correo.iloc[0,0]
            ruta = '"'+ query_rutacorreo + '" --context_param componente=' + str(param_componente) + ' --context_param intervalo=' + param_intervalo + ''            
            #os.system(ruta)
            subprocess.call(ruta, shell=True)
            
            df.iloc[fila, 34] = 1 #si se envia el correo entonces marcar 1
            
        fila = fila + 1
    
    #df = np.concatenate((df1, df2), axis=1)
    metodo = 'replace'
    name_table = 'df_pandas_vol'
    insert_bd(database_ip,database_username,database_password,database_name,name_table,df,metodo)    
    
    return "ok"
