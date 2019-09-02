from flask import Flask
from flask_restful import Resource, Api
import preventivoMain

app = Flask(__name__)
api = Api(app)

class PreventivoApi(Resource):
        
    def get(self,fecha,intervalofin,nom_dia,dia_pago,train,cliente):
        
        IvrPreventivo = preventivoMain.IvrPreventivo()
        IvrPreventivo.inicializar(fecha,intervalofin,nom_dia,dia_pago,train,cliente) 
        IvrPreventivo.proceso() 
        
        json = [{"fecha":fecha},
                {"nom_dia":nom_dia},
                {"intervalofin":intervalofin},
                {"dia_pago":dia_pago},
                {"train":train},
                {"cliente":cliente}
                ]
        return "PROCESO OK"

api.add_resource(PreventivoApi, '/fecha/<fecha>/intervalofin/<intervalofin>/nom_dia/<nom_dia>/dia_pago/<dia_pago>/train/<train>/cliente/<cliente>')

if __name__ == '__main__':
    app.run(debug=True)
  
#http://localhost:5000/fecha/2019-07-10/intervalofin/14:00:00/nom_dia/MIERCOLES/dia_pago/0/train/0/cliente/Bancolombia    