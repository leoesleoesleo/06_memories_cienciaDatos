
#CARGAR DATA
dataframe <- read.csv("C:/Users/leonardo.patino/Desktop/mineria datos/bank-additional-full.csv", sep=";")


#Para evitar la introducción de un sesgo SE HACE ANTES
#Para simular un conjunto de pruebas y trenes, 
#dividiremos aleatoriamente estos datos en 80% de prueba y 20% de prueba.

# Random sample indexes
train_index = sample(1:nrow(dataframe), 0.8 * nrow(dataframe))
test_index = setdiff(1:nrow(dataframe), train_index)

# Build X_train, y_train, X_test, y_test
X_train = dataframe[train_index, -21]
y_train = dataframe[train_index, "y"]

X_test = dataframe[test_index, -21]
y_test = dataframe[test_index, "y"]

# Guardar CSV file
write.csv(X_train, "C:/Users/leonardo.patino/Desktop/mineria datos/X_train.csv")
write.csv(X_test, "C:/Users/leonardo.patino/Desktop/mineria datos/X_test.csv")


# ANALISIS

#Conociendo los clientes

pie(table(dataframe$y),main="clientes que se han suscrito un depósito a plazo")

tabla<- as.data.frame(table(dataframe$y)) #pongamos la tabla de frecuencia en formato de bd.
freq_Acum <- cumsum(tabla$Freq) #la frecuencia acumulada
freq_relat<- prop.table(tabla$Freq)*100 #La frecuencia relativa en porcentaje
Freq_relat_acum<-cumsum(freq_relat) #La frecuencia relativa acumulada
tablafinal<-cbind(tabla,freq_Acum,freq_relat,Freq_relat_acum ) #juntamos todo
tablafinal # Allí está el resultado


#TABLAS DE FRECUENCIA CRUZADA

#library(foreign)
tabla<- as.data.frame(table(dataframe$job,dataframe$y)) #pongamos la tabla de frecuencia en formato de bd.
freq_Acum <- cumsum(tabla$Freq) #la frecuencia acumulada
freq_relat<- prop.table(tabla$Freq)*100 #La frecuencia relativa en porcentaje
Freq_relat_acum<-cumsum(freq_relat) #La frecuencia relativa acumulada
tablafinal<-cbind(tabla,freq_Acum,freq_relat,Freq_relat_acum ) #juntamos todo
tablafinal # Allí está el resultado

t=table(dataframe$job,dataframe$y)
prop.table(t)
prop.table(t,1) #Por filas
prop.table(t,2) #Por columnas

t=table(dataframe$education,dataframe$y)
prop.table(t)
prop.table(t,1) #Por filas
prop.table(t,2) #Por columnas

t=table(dataframe$loan,dataframe$y)
prop.table(t)
prop.table(t,1) #Por filas
prop.table(t,2) #Por columnas



#Resúmenes de datos por grupos
#previous and y 
aggregate(dataframe$previous,by=list(dataframe$y),mean)

library(psych)
#describe(dataframe$age)
describeBy(dataframe$previous, dataframe$y)

boxplot(dataframe$previous~dataframe$y,ylab="Contactos",
        main="Duración último contacto del cliente por suscripción a depósito.")

#duracion and y 
aggregate(dataframe$previous,by=list(dataframe$y),mean)


#Agrupamiento
# Model Based Clustering
library(mclust)
fit <- Mclust(X_test$job,X_test$education,X_test$loan)
plot(fit) # plot results 
summary(fit) # display the best model


#Matriz Correlación
new_datos = cbind(dataframe[1],dataframe[11:14],dataframe[16:21])
y_new_datos = new_datos[new_datos$y == "yes",]
corrplot(cor(y_new_datos[1:10]), method = "number") # Display the correlation coefficient

n_new_datos = new_datos[new_datos$y == "no",]
corrplot(cor(n_new_datos[1:10]), method = "number") # Display the correlation coefficient








