import preventivoModel

fecha               = '2019-07-24'
nom_dia             = 'MIERCOLES'
intervalofin        = '16:00:00'
dia_pago            = 0
train               = 1
cliente             = 'Bancolombia' 
intervaloini        = '00:00:00'
intervalofinestim   = '23:50:00'

database_username = 'dllo'
database_password = 'dllo'
database_ip       = '172.20.73.124'
database_name     = 'ivr_preventivo_'

query_vol        = preventivoModel.insumo(train,cliente,fecha)[0]
query_oper       = preventivoModel.insumo(train,cliente,fecha)[1]
query_estim      = preventivoModel.insumo(train,cliente,fecha)[2]
query_estim_oper = preventivoModel.insumo(train,cliente,fecha)[3]
query_tuneo      = preventivoModel.insumo(train,cliente,fecha)[4]
query_correo     = preventivoModel.insumo(train,cliente,fecha)[5]

data_vol        = preventivoModel.ejecutar_querys(train,database_ip,database_username,database_password,database_name,query_vol,query_oper,query_estim,query_estim_oper,query_tuneo,query_correo)[0]
data_oper       = preventivoModel.ejecutar_querys(train,database_ip,database_username,database_password,database_name,query_vol,query_oper,query_estim,query_estim_oper,query_tuneo,query_correo)[1]
data_tuneo      = preventivoModel.ejecutar_querys(train,database_ip,database_username,database_password,database_name,query_vol,query_oper,query_estim,query_estim_oper,query_tuneo,query_correo)[2]
data_estim      = preventivoModel.ejecutar_querys(train,database_ip,database_username,database_password,database_name,query_vol,query_oper,query_estim,query_estim_oper,query_tuneo,query_correo)[3]
data_estim_oper = preventivoModel.ejecutar_querys(train,database_ip,database_username,database_password,database_name,query_vol,query_oper,query_estim,query_estim_oper,query_tuneo,query_correo)[4]
data_correo     = preventivoModel.ejecutar_querys(train,database_ip,database_username,database_password,database_name,query_vol,query_oper,query_estim,query_estim_oper,query_tuneo,query_correo)[5]

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

if dia_pago == 0:
    factor_estacional = factor_estacional
elif dia_pago == 1:
    factor_estacional = factor_estacionalQ
	
df = preventivoModel.calcularestimadores_vol(data_vol,data_estim,intervaloini,incremento,intervalofin,intervalofinestim,nom_dia,dia_pago,fecha,train,incremen_estimadorLlam,incremen_estimadorVol,incremen_estimadorTran,incremen_estimadorLlamMin,database_ip,database_username,database_password,database_name)	
			
res = preventivoModel.algoritmo_preventivo(df,data_oper,factor1,factor2,factor3,inter_deslizante,factor_estacional,fecha,database_ip,database_username,database_password,database_name,intervaloini,incremento,intervalofin,nom_dia,dia_pago,train)
df_data = res[0]

preventivoModel.generar_alertas(df_data,database_ip,database_username,database_password,database_name,data_correo)
		
	