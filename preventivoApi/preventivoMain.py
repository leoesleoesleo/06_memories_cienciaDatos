# -*- coding: utf-8 -*-
"""
Created on Mon May 27 16:00:44 2019

@author: leonardo.patino
"""
import preventivoModel
#import argparse

 
class IvrPreventivo:
    
    def inicializar(self,fecha,intervalofin,nom_dia,dia_pago,train,cliente):
        """
        #parametros recibidos desde cmd
        parser = argparse.ArgumentParser()
        parser.add_argument("--fecha",        help="fecha a procesar")
        parser.add_argument("--intervalofin", help="intervalofin a procesar")
        parser.add_argument("--nom_dia",      help="nom_dia a procesar")
        parser.add_argument("--dia_pago",     help="dia_pago a procesar")
        parser.add_argument("--train",        help="train a procesar")
        parser.add_argument("--cliente",      help="cliente a procesar")
        args = parser.parse_args()  
        self.fecha               = args.fecha
        self.intervalofin        = args.intervalofin
        self.nom_dia             = args.nom_dia
        self.dia_pago            = int(args.dia_pago)
        self.train               = int(args.train)
        self.cliente             = args.cliente
        """
        self.fecha               = fecha
        self.intervalofin        = intervalofin
        self.nom_dia             = nom_dia
        self.dia_pago            = int(dia_pago)
        self.train               = int(train)
        self.cliente             = cliente        
        self.intervaloini        = '00:10:00'
        self.intervalofinestim   = '23:50:00'
            
        #string conexión
        self.database_username = 'dllo'
        self.database_password = 'dllo'
        self.database_ip       = '172.20.73.124'
        self.database_name     = 'ivr_preventivo_'

    def proceso(self,debug=False):
        if debug==True:
            print("Parametros Recibidos: ",self.fecha," ",self.nom_dia," ",self.dia_pago," ",self.train," ",self.cliente)
        
        query_vol        = preventivoModel.insumo(self.train,self.cliente,self.fecha)[0]
        query_oper       = preventivoModel.insumo(self.train,self.cliente,self.fecha)[1]
        query_estim      = preventivoModel.insumo(self.train,self.cliente,self.fecha)[2]
        query_estim_oper = preventivoModel.insumo(self.train,self.cliente,self.fecha)[3]
        query_tuneo      = preventivoModel.insumo(self.train,self.cliente,self.fecha)[4]
        query_correo     = preventivoModel.insumo(self.train,self.cliente,self.fecha)[5]
        
        #EJECUTAR QUERYS
        data_vol        = preventivoModel.ejecutar_querys(self.train,self.database_ip,self.database_username,self.database_password,self.database_name,query_vol,query_oper,query_estim,query_estim_oper,query_tuneo,query_correo)[0]
        data_oper       = preventivoModel.ejecutar_querys(self.train,self.database_ip,self.database_username,self.database_password,self.database_name,query_vol,query_oper,query_estim,query_estim_oper,query_tuneo,query_correo)[1]
        data_tuneo      = preventivoModel.ejecutar_querys(self.train,self.database_ip,self.database_username,self.database_password,self.database_name,query_vol,query_oper,query_estim,query_estim_oper,query_tuneo,query_correo)[2]
        data_estim      = preventivoModel.ejecutar_querys(self.train,self.database_ip,self.database_username,self.database_password,self.database_name,query_vol,query_oper,query_estim,query_estim_oper,query_tuneo,query_correo)[3]
        #data_estim_oper = preventivoModel.ejecutar_querys(self.train,self.database_ip,self.database_username,self.database_password,self.database_name,query_vol,query_oper,query_estim,query_estim_oper,query_tuneo,query_correo)[4]
        data_correo     = preventivoModel.ejecutar_querys(self.train,self.database_ip,self.database_username,self.database_password,self.database_name,query_vol,query_oper,query_estim,query_estim_oper,query_tuneo,query_correo)[5]
        
        #TUNEO
        factor1                     = data_tuneo.iloc[0,1]
        factor2                     = data_tuneo.iloc[0,2]
        factor3                     = data_tuneo.iloc[0,3]
        factor_estacional           = data_tuneo.iloc[0,4]
        factor_estacionalQ          = data_tuneo.iloc[0,5]
        inter_deslizante            = data_tuneo.iloc[0,6]
        incremento                  = data_tuneo.iloc[0,7]
        incremen_estimadorTran      = data_tuneo.iloc[0,8]
        incremen_estimadorLlam      = data_tuneo.iloc[0,9]
        incremen_estimadorLlamMin   = data_tuneo.iloc[0,10]
        incremen_estimadorVol       = data_tuneo.iloc[0,11]
        
        if self.dia_pago == 0:
            factor_estacional = factor_estacional
        elif self.dia_pago == 1:
            factor_estacional = factor_estacionalQ
        
        if debug==True:
            print("factor1: ",factor1," ")
            print("factor2: ",factor2," ")
            print("factor3: ",factor3," ")
            print("factor_estacional: ",factor_estacional," ")
            print("inter_deslizante: ",inter_deslizante," ")
            print("incremento: ",incremento," ")
            print("incremen_estimadorTran: ",incremen_estimadorTran," ")
            print("incremen_estimadorLlam: ",incremen_estimadorLlam," ")
            print("incremen_estimadorLlamMin: ",incremen_estimadorLlamMin," ")
            print("incremen_estimadorVol: ",incremen_estimadorVol," ")
            
            print("calcularestimadores_vol ...")
        df = preventivoModel.calcularestimadores_vol(data_vol,data_estim,self.intervaloini,incremento,self.intervalofin,self.intervalofinestim,self.nom_dia,self.dia_pago,self.fecha,self.train,incremen_estimadorLlam,incremen_estimadorVol,incremen_estimadorTran,incremen_estimadorLlamMin,self.database_ip,self.database_username,self.database_password,self.database_name)
        
        if debug==True:
            print("algoritmo_preventivo...")
        res = preventivoModel.algoritmo_preventivo(df,data_oper,factor1,factor2,factor3,inter_deslizante,factor_estacional,self.fecha,self.database_ip,self.database_username,self.database_password,self.database_name,self.intervaloini,incremento,self.intervalofin,self.nom_dia,self.dia_pago,self.train)
        df_data = res[0]  
        
        if debug==True:
            print("Generando Alertas ...")
        preventivoModel.generar_alertas(df_data,self.database_ip,self.database_username,self.database_password,self.database_name,data_correo)
        
        return df_data

#TEST
"""
IvrPreventivo = IvrPreventivo()
fecha               = '2019-08-08'
nom_dia             = 'JUEVES'
intervalofin        = '09:50:00'
dia_pago            = 0
train               = 1
cliente             = 'Bancolombia' 
        
IvrPreventivo.inicializar(fecha,intervalofin,nom_dia,dia_pago,train,cliente) 
IvrPreventivo.proceso() 
"""

#PENDIENTES:
#Validar estimadores correctos OPER
#Control de Horario
#segùn estacionalidad afectar las tran