# -*- coding: utf-8 -*-
"""
Created on Thu Sep  5 15:03:23 2019

@author: leonardo.patino
"""
# estructura de un decorador
# 3 funciones (A,B y C) donde A recibe como parametro a B para devolver C
# Decorador devuelve una funciòn

#ejemplo 1
def decorador(func): #funciòn A y recibe parametro función B
    def new_function(*args, **kwargs): #funciòn C (el decorador devuelve el parametro de esta función)
        print("iniciando juego triqui ...")
        res = func(*args, **kwargs)        
        print("finalizando juego triqui ...")
        return res
    return new_function

@decorador
def triqui(x,o):
    if x:
        x = "win "
        o = "loose "
    elif o:
        o = "win "
        x = "loose "
    else:
        o = "tie "
        x = "tie "
    return x,o    

res = triqui(1,0)
print(res)        


#ejemplo 2

def a_funcion_decoradora(b_funcion):
    def c_funcion(*args):
        #acciones adicionales que decoran
        print("vamos a realizar un calculo: ")        
        b_funcion(*args)        
        #acciones adicionales que decoran
        print("Hemos terminado el calulo")
    return c_funcion  

@a_funcion_decoradora
def suma(num1,num2):
    print(num1+num2)
    
@a_funcion_decoradora
def resta(num1,num2):
    print(num1-num2)
    

suma(7,5)
resta(7,5)    

    
        



