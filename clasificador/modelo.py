# -*- coding: utf-8 -*-
"""
fuente: https://archive.ics.uci.edu/ml/datasets/Bank+Marketing#
Created on Wed May 22 10:16:40 2019
@author: Leonardo
"""
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from sklearn.preprocessing import Normalizer
from sklearn.preprocessing import LabelEncoder 
from sklearn.linear_model import LinearRegression
from sklearn.neural_network import MLPClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.metrics import confusion_matrix

data_train=pd.read_csv('X_train.csv',delimiter=';') # csv train
data_test=pd.read_csv('X_test.csv',delimiter=';') # csv test

#______________TRAIN___________________
#normalizar campos cuantitativos eje X_train
num = data_train.loc[:,'age':'nr.employed']
transformer = Normalizer().fit(num)
t=transformer.transform(num)
#print(t)

#normalizar campos cualitativos eje X_train
cat = data_train.loc[:,'job':'poutcome']
cam = pd.get_dummies(cat)
#print(cam)

#Concatenar eje X_train
X = np.concatenate((num, cam), axis=1)
#print(X)

#eje y_train
t_y = data_train['y']
y=t_y

#______________TEST___________________

#normalizar campos cuantitativos eje X_test
num_test = data_test.loc[:,'age':'nr.employed']
transformer_test = Normalizer().fit(num_test)
t_test=transformer_test.transform(num_test)
#print(t_test)

#normalizar campos cualitativos eje X_test
cat_test = data_test.loc[:,'job':'poutcome']
cam_test = pd.get_dummies(cat_test)
#print(cam_test)

#Concatenar eje X_test
X_test = np.concatenate((num_test, cam_test), axis=1)

#eje y_test
y_test = data_test['y']

#______________MODELADO___________________

#RANDOM FOREST
clf_fores = RandomForestClassifier(n_estimators=100, max_depth=2,random_state=0)
model= clf_fores.fit(X, y)
score = model.score(X,y)
print(model)
print(score)

#REDES NEURONALES
clf = MLPClassifier(verbose=False, warm_start=False,hidden_layer_sizes=(50,50,50,50),activation='identity')
model = clf.fit(X, y)
score = model.score(X,y)
print(model)
print(score)

#REGRESIÓN LOGISTICA
clf_logis = LogisticRegression(random_state=0, solver='lbfgs',
                         multi_class='multinomial').fit(X, y)
model = clf_logis.fit(X, y)
score = model.score(X,y)
print(model)
print(score)

#______________PREDECIR DATOS NO VISTOS___________________


itera = len(y_test)
y_traget = []
y_testt = []
win = []
loose = []

i = 0
while i < itera:
    X_test_ = X_test[i]
    predict = model.predict([X_test_])[0]
    y_test_ = y_test[i] 
    
    if predict == y_test_:
        r="win"
        win.append(1)        
    else:
        r="loose"  
        loose.append(1)
    
    y_traget.append(predict) # no/si
    y_testt.append(y_test_)    
     
    i = i + 1    
    
    print("Predicción: ",predict," Se esperaba: ",y_test_, "Resultado: ",r) 
    
mx = confusion_matrix(y_testt, y_traget)
print(mx)
print("ganadas ",len(win))
print("perdidas ",len(loose))
print("Score: ",score)
print("Ganadas vs Perdidas: %",(len(win)*100/(len(win)+len(loose))))
acu = accuracy_score(y_testt, y_traget)*100
print("Acuracy: ",acu)
    
#print("Predicción ",y_traget)
#print("Se Esperaba ",y_testt)





