import json
from server.database import collection ,database_mongo
from bson import regex
from datetime import datetime, timedelta, timezone
import requests
from dataclasses import dataclass

# Zona horaria GMT-5 para fechas de telemetría
GMT5 = timezone(timedelta(hours=-5))


#def bd_gene(suffix: str) -> str:
    #"""Nombre de colección genérica por mes/año (ej: TK_PROCESO_03_2025)."""
    #now = datetime.now()
    #return f"TK_{suffix.upper()}_{now.month:02d}_{now.year}"

def bd_gene(imei):
    fet =datetime.now()
    part = fet.strftime('_%m_%Y')
    colect ="TK_"+imei+part
    return colect

def bd_gene_mes_año(mes_actual, año_actual):
    colect ="TK_dispositivos_"+str(mes_actual)+"_"+str(año_actual)
    return colect


def bd_oficial(imei: str) -> str:
    """Nombre de colección oficial por IMEI y año (ej: TK_867858039011138_2025)."""
    return f"TK_{imei}_{datetime.now().year}"
import random
from bson import ObjectId

from typing import Dict, List, Optional, Any
from statistics import median
#from server.functions.ProcesadorDatosChart import ProcesadorDatosChart

def _ahora_gmt5() -> datetime:
    """Hora actual en GMT-5 (misma zona que los datos almacenados)."""
    return datetime.now(GMT5)

def _fecha_ultima_como_gmt5(fecha_ultima: Optional[datetime]) -> Optional[datetime]:
    """Interpreta la fecha almacenada (naive) como GMT-5 para comparar con ahora en GMT-5."""
    if fecha_ultima is None:
        return None
    if fecha_ultima.tzinfo is not None:
        return fecha_ultima
    return fecha_ultima.replace(tzinfo=GMT5)

def _calcular_estado_conexion(fecha_ultima: Optional[datetime]) -> str:
    """online <= 30 min, wait 30 min - 24 h, offline > 24 h. Fechas en GMT-5."""
    fecha_gmt5 = _fecha_ultima_como_gmt5(fecha_ultima)
    if fecha_gmt5 is None:
        return "offline"
    ahora = _ahora_gmt5()
    delta = ahora - fecha_gmt5
    minutos = delta.total_seconds() / 60
    if minutos <= 30:
        return "online"
    if minutos <= 24 * 60:
        return "wait"
    return "offline"

def _filtrar_campos_elementales(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Deja solo los campos elementales; fechas a ISO string."""
    out = {}
    for k in _CAMPOS_ULTIMO_ESTADO:
        v = doc.get(k)
        if isinstance(v, datetime):
            v = v.isoformat()
        out[k] = v
    return out

# Campos elementales para el endpoint de último estado (tabla)
_CAMPOS_ULTIMO_ESTADO = [
    "temp_supply_1", "return_air", "evaporation_coil", "condensation_coil", "compress_coil_1",
    "co2_reading", "o2_reading", "cargo_1_temp", "cargo_2_temp", "cargo_3_temp", "cargo_4_temp",
    "power_kwh", "numero_alarma", "sp_ethyleno", "set_point_o2", "set_point_co2",
    "power_state", "capacity_load", "telemetria_id", "created_at", "longitud", "latitud", "set_point",
]


def _calcular_en_rango(set_point_val: Any, return_air_val: Any) -> Optional[bool]:
    """True si return_air está en rango ±5 respecto a set_point; None si no aplica."""
    try:
        sp = float(set_point_val)
        ra = float(return_air_val)
    except (TypeError, ValueError):
        return None
    return abs(ra - sp) <= 5

def _calcular_en_defrost(
    set_point_val: Any,
    temp_supply_1_val: Any,
    return_air_val: Any,
    evaporation_coil_val: Any,
) -> Optional[bool]:
    """
    Defrost: uno de temp_supply_1 o return_air cerca de set_point,
    y evaporation_coil (resistencia de deshielo) elevada en rango ~12 a 30 °C.
    """
    try:
        evap = float(evaporation_coil_val)
    except (TypeError, ValueError):
        return None
    if evap < 12 or evap > 30:
        return False
    sp = set_point_val
    cerca = _cerca_set_point(temp_supply_1_val, sp) or _cerca_set_point(return_air_val, sp)
    return cerca

def _cerca_set_point(val: Any, set_point_val: Any, margen: float = 5.0) -> bool:
    """True si val está dentro de ±margen respecto a set_point."""
    try:
        v = float(val)
        sp = float(set_point_val)
        return abs(v - sp) <= margen
    except (TypeError, ValueError):
        return False

green_box =["CC:DB:A7:9D:F3:E8","0C:B8:15:F2:C7:A0","28:05:A5:2B:E9:88","28:05:A5:2A:FA:04","28:05:A5:2C:A9:E4",]
descrip = ["GREENBOX ZGRU9803515","GREENBOX ZGRU0048736","GREENBOX ZGRU9803691","GREENBOX ZGRU1040025","GREENBOX ZGRU003388"]
#{ d08: { $nin: [null, ""] } }
from datetime import datetime

def obtener_meses_creados(db):

    now = datetime.now()
    mes = now.month
    anio = now.year

    meses_creados = []

    while True:

        nombre_coleccion = bd_gene_mes_año(f"{mes:02d}", f"{anio:04d}")

        # verificar si la colección existe
        if nombre_coleccion in db.list_collection_names():

            #meses_creados.append(f"{anio:04d}-{mes:02d}")
            meses_creados.append(nombre_coleccion)
            # retroceder un mes
            mes -= 1

            if mes == 0:
                mes = 12
                anio -= 1

        else:
            break

    return meses_creados

def es_hexadecimal(cadena):
    try:
        int(cadena, 16)
        return True
    except ValueError:
        return False

def encontrar_parte_en_texto(text: str, parte: str):
    text_lower = text.lower()
    sequence_lower = parte.lower()
    index = text_lower.find(sequence_lower)
    if index != -1:
        part1 = text[:index + len(parte)]
        part2 = text[index + len(parte):]
        return part2
    else:
        return None

def cortar_texto_1B04(texto, marcador):
    indice = texto.find(marcador)
    if indice != -1:
        texto_cortado = texto[:indice]
    else:
        texto_cortado = texto  # Si no se encuentra, se devuelve igual
    cantidad_letras = len(texto_cortado)
    return [texto_cortado, cantidad_letras]

def texto_error(number: int) -> str:
    return f"E{number:02}"

def invertir_texto_en_par(text: str) -> str:
    pairs = [text[i:i+2] for i in range(0, len(text), 2)]
    inverted_text = ''.join(pairs[::-1]) 
    return inverted_text

def convert_number(valve: int, divisor: int) -> float:
    val = 0.0
    if 0x7FEF <= valve <= 0x7FFF:
        val = float(valve)
    else:
        if valve > 0x7FFF:
            valve = 0xFFFF - valve
            valve += 1
            val = float(valve)
            val = -val
        else:
            val = float(valve)
        val =val/divisor
    return float(val)

A_sensor_eventos = ["Sensor is initializing" ,"Value not applicable (N/A)" ,"Sensor error","Sensor open","Sensor short","Sensor above",
                    "Sensor below","Sensor no comm","Sensor warm up","This is the max value that a sensor can have","Values not represent a reading"]
A_sensor_codigo = ["7FFF","7FFE","7FFD","7FFC","7FFB","7FFA","7FF9","7FF8","7FF7","7FEF","7FF0"]


A_sensor_readout_dataset = ["temp_supply_1","temp_supply_2","return_air","evaporation_coil","condensation_coil","compress_coil_1","compress_coil_2",
                          "ambient_air","cargo_1_temp","cargo_2_temp","cargo_3_temp","cargo_4_temp","relative_humidity","avl","suction_pressure",
                          "discharge_pressure","line_voltage","line_frequency","consumption_ph_1","consumption_ph_2","consumption_ph_3","co2_reading",
                          "o2_reading","evaporator_speed","condenser_speed","battery_voltage","power_kwh","power_trip_reading","power_trip_duration",
                          "suction_temp","discharge_temp","supply_air_temp","return_air_temp","dl_battery_temp","dl_battery_charge","power_consumption",
                          "power_consumption_avg","suction_pressure_2","suction_temp_2"]
A_sensor_divisor = [10,10,10,10,10,10,10,10,10,10,10,10,1,1,100,100,1,1,10,10,10,10,10,1,1,10,10,10,1,10,10,10,10,100,100,100,100,100,10]
A_sensor_caracter = [4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,8,8,8,4,4,4,4,4,4,4,4,4,4]

def transformar_d02(valor):
    resultado = {}
    etiquetas = A_sensor_readout_dataset
    divisores = A_sensor_divisor
    lista_caracter = A_sensor_caracter
    print(valor)
    print("--------------------------------")
    cadena_ok = encontrar_parte_en_texto(valor,"82A700")
    print(cadena_ok)
    print("--------------------------------")
    cadena_ok =cortar_texto_1B04(cadena_ok,"1B04FF") 
    print(cadena_ok)
    print("--------------------------------")
    cadena_ok =cadena_ok[0]
    print(cadena_ok)
    print("--------------------------------")
    #validar que sea hexadecimal
    if  es_hexadecimal(cadena_ok):
        cadena_general = cadena_ok
        current_position_set = 0
        for idx ,num_chars in enumerate(lista_caracter):
            substring = cadena_general[current_position_set:current_position_set + num_chars]    
            if num_chars==2 and len(substring)==2 :  
                try:
                    transformado = int(substring, 16)
                except ValueError:
                    print("Valor no hexadecimal:", substring)
                    transformado = None  #       
                if substring =='FF' or substring =='FE' :
                    res_trans = texto_error(98)
                else :
                    if idx ==3 :
                        transformado = (transformado >> 1) & 1
                    res_trans = transformado
            elif num_chars==4 and len(substring)==4 :
                inverso =invertir_texto_en_par(substring)
                try:
                    index_error = A_sensor_codigo.index(inverso)
                except ValueError:
                    index_error = -1  
                if index_error == -1 :
                    transformado = convert_number(int(invertir_texto_en_par(substring), 16),divisores[idx])
                    res_trans = transformado if transformado or transformado ==0 else texto_error(98)
                else :
                    res_trans = texto_error(index_error)
            elif num_chars==8 and len(substring)==8 :
                transformado = convert_number(int(invertir_texto_en_par(substring), 16),divisores[idx])
                res_trans = round(transformado/divisores[idx],1)
            else :
                res_trans = texto_error(99)   
            resultado[etiquetas[idx]]=res_trans
            current_position_set += num_chars
        
    return resultado


A_sensor_readout_set = ["alarm_present","set_point","capacity_load","power_state","controlling_mode","humidity_control","humidity_set_point",
                          "fresh_air_ex_mode","fresh_air_ex_rate","fresh_air_ex_delay","set_point_o2","set_point_co2","defrost_term_temp","defrost_interval","water_cooled_conde",
                          "usda_trip","evaporator_exp_valve","suction_mod_valve","hot_gas_valve","economizer_valve"]
A_sensor_caracter_set = [4,4,4,2,2,2,2,2,4,4,4,4,4,2,2,2,2,2,2,2]
A_sensor_divisor_set = [1,100,1,1,1,1,1,1,1,10,10,10,100,1,1,1,1,1,1,1]

def transformar_d03(valor):
    resultado = {}
    etiquetas = A_sensor_readout_set
    divisores = A_sensor_divisor_set
    lista_caracter = A_sensor_caracter_set
    cadena_ok = encontrar_parte_en_texto(valor,"82A701")
    cadena_ok =cortar_texto_1B04(cadena_ok,"1B04FF") 
    cadena_ok =cadena_ok[0]
    #validar que sea hexadecimal
    if  es_hexadecimal(cadena_ok):
        cadena_general = cadena_ok
        current_position_set = 0
        for idx ,num_chars in enumerate(lista_caracter):
            substring = cadena_general[current_position_set:current_position_set + num_chars]    
            if num_chars==2 and len(substring)==2 :  
                try:
                    transformado = int(substring, 16)
                except ValueError:
                    print("Valor no hexadecimal:", substring)
                    transformado = None  #       
                if substring =='FF' or substring =='FE' :
                    res_trans = texto_error(98)
                else :
                    if idx ==3 :
                        transformado = (transformado >> 1) & 1
                    res_trans = transformado
            elif num_chars==4 and len(substring)==4 :
                inverso =invertir_texto_en_par(substring)
                try:
                    index_error = A_sensor_codigo.index(inverso)
                except ValueError:
                    index_error = -1  
                if index_error == -1 :
                    transformado = convert_number(int(invertir_texto_en_par(substring), 16),divisores[idx])
                    res_trans = transformado if transformado or transformado ==0 else texto_error(98)
                else :
                    res_trans = texto_error(index_error)
            elif num_chars==8 and len(substring)==8 :
                transformado = convert_number(int(invertir_texto_en_par(substring), 16),divisores[idx])
                res_trans = round(transformado/divisores[idx],1)
            else :
                res_trans = texto_error(99)   
            resultado[etiquetas[idx]]=res_trans
            current_position_set += num_chars

    return resultado


A_sensor_readout_alarma = ["numero_alarma","alarma_01","alarma_02","alarma_03","alarma_04","alarma_05","alarma_06","alarma_07","alarma_08","alarma_09","alarma_10"]
A_sensor_caracter_alarma = [4,4,4,4,4,4,4,4,4,4,4]
A_sensor_divisor_alarma =  [1,1,1,1,1,1,1,1,1,1,1]

def transformar_d08(valor):
    resultado = {}
    etiquetas = A_sensor_readout_alarma
    divisores = A_sensor_divisor_alarma
    lista_caracter = A_sensor_caracter_alarma
    cadena_ok = encontrar_parte_en_texto(valor,"82A706")
    cadena_ok =cortar_texto_1B04(cadena_ok,"1B04FF") 
    cadena_ok =cadena_ok[0]
    #validar que sea hexadecimal
    if  es_hexadecimal(cadena_ok):
        cadena_general = cadena_ok
        current_position_set = 0
        for idx ,num_chars in enumerate(lista_caracter):
            substring = cadena_general[current_position_set:current_position_set + num_chars]    
            if num_chars==2 and len(substring)==2 :
                try:
                    transformado = int(substring, 16)
                except ValueError:
                    print("Valor no hexadecimal:", substring)
                    transformado = None  #       
                res_trans = transformado
            elif num_chars==4 and len(substring)==4 :
                inverso =invertir_texto_en_par(substring)
                try:
                    index_error = A_sensor_codigo.index(inverso)
                except ValueError:
                    index_error = -1  
                if index_error == -1 :
                    transformado = convert_number(int(invertir_texto_en_par(substring), 16),divisores[idx])
                    res_trans = transformado if transformado or transformado ==0 else texto_error(98)
                else :
                    res_trans = texto_error(index_error)
            elif num_chars==8 and len(substring)==8 :
                transformado = convert_number(int(invertir_texto_en_par(substring), 16),divisores[idx])
                res_trans = round(transformado/divisores[idx],1)
            else :
                res_trans = texto_error(99)   
            resultado[etiquetas[idx]]=res_trans
            current_position_set += num_chars

    return resultado

# en d04: "0 1 1 150 0 64 152.4" procesar datos segun class MaduradorV1  , 7 campos separados por espacio  , si solo hay 6 campos, el ultimo campo es null y sucesivamente 

def transformar_d04(valor):
    campos = [
        "iRspRip",
        "iOptRip",
        "iCtrlRip",
        "SP_PPM",
        "iDlyRip",
        "iDrtRip",
        "PPM_Sensor"
    ]

    if not valor:
        return {k: None for k in campos}

    partes = (valor.split() + [None] * 7)[:7]

    resultado = {}

    for i, campo in enumerate(campos):
        v = partes[i]

        try:
            if i == 6 and v is not None:  # último campo float
                resultado[campo] = float(v)
            else:
                resultado[campo] = int(v) if v is not None else None
        except:
            resultado[campo] = None

    return resultado


def procesar_documento(doc):
    resultado = {
        "i": doc.get("i"),
        "ip": doc.get("ip"),
        "estado": doc.get("estado"),
        "fecha": doc.get("fecha"),
        "tramas_procesadas": []
    }

    mapa_tramas = {
        #"c": transformar_c,
        #"d00": transformar_d00,
        #"d01": transformar_d01,
        "d02": transformar_d02,
        "d03": transformar_d03,
        "d04": transformar_d04,
        #"d05": transformar_d05,
        #"d06": transformar_d06,
        #"d07": transformar_d07,
        "d08": transformar_d08,
        #"d1": transformar_d1,
        #"d2": transformar_d2,
        #"d3": transformar_d3,
        #"d4": transformar_d4,
        #"gps": transformar_gps,
        #"val": transformar_val,
        #"rs": transformar_rs,
        #"r": transformar_r,
    }

    for campo, funcion in mapa_tramas.items():
        valor = doc.get(campo)

        if valor is not None and valor != "":
            datos = funcion(valor)   # la función devuelve solo datos
            resultado.update(datos)  # se agregan al resultado
            resultado["tramas_procesadas"].append(campo)

    return resultado

def reconstruccion_green_box():

    meses_creados = obtener_meses_creados(database_mongo)

    json_data = imeis_en_colecciones(meses_creados, green_box, collection)
    

    return json_data            






def bd_gene_imei(nombre_coleccion, imei):
    try:
        _, _, mes, anio = nombre_coleccion.split("_")
        return f"TK_{imei}_{mes}_{anio}"
    except ValueError:
        raise ValueError(f"Formato de colección inválido: {nombre_coleccion}")

import time



def imeis_en_colecciones(meses_creados, green_box, collection):
    # resultado: {"imei1": ["TK_dispositivos_03_2026", ...], "imei2": [...], ...}
    #vacear las colecciones que tengan el nombre "TRATADO_green_box[i]
    # vaciar TRATADO_<imei>
    tiempo_inicio = time.time()
    for imei in green_box:
        nombre = f"TRATADO_{imei}"
        collection(nombre).delete_many({})
        resultado = {imei: [] for imei in green_box}

    for nombre_coleccion in meses_creados:
        col = collection(nombre_coleccion)

        # Trae solo los imeis que existen en esa colección (1 consulta por mes)
        for doc in col.find({"imei": {"$in": green_box}}, {"_id": 0, "imei": 1}):
            
            imei = doc.get("imei")
            if not imei:
                continue

            col_imei = collection(bd_gene_imei(nombre_coleccion, imei))
            col_tratado = collection(f"TRATADO_{imei}")   # <- AQUÍ
            
            #procesar 10 documentos de cada coleccion y guardar en una lista de documentos
            documentos = []
            #for doc in col_imei.find({}).limit(10):
            for doc in col_imei.find({}):

                documentos.append(doc)
            #procesar los documentos de la lista con la funcion procesar_documento
            for doc in documentos:
                resultado1 = procesar_documento(doc)
                print(resultado1)
                print("--------------------------------")
                validado = estructura_termoking(resultado1)
                validado['ethylene'] = validado.get('PPM_Sensor')
                validado['sp_ethyleno'] = validado.get('SP_PPM')
                #validado['stateProcess'] = validado.get('stateProcess')
                validado['inyeccion_hora'] = validado.get('iDrtRip')
                print(validado)
                print("--------------------------------")
                col_tratado.insert_one(validado)
            cantidad_datos = col_imei.count_documents({})
            #calcular el tiempo de ejecucion de la funcion
            tiempo_ejecucion = time.time() - tiempo_inicio
            print(f"Tiempo de ejecución: {tiempo_ejecucion:.2f} segundos")
            resultado.setdefault(imei, []).append({
                "Coleccion": nombre_coleccion,
                "cantidad": cantidad_datos,
                "tiempo_ejecucion": tiempo_ejecucion
            })
            

    return resultado               

 





def  lista_imeis_termoking():
    data_proceso_actual = collection(bd_gene("dispositivos"))
    dispositivos: List[Dict[str, Any]] = []
    ahora_gmt5 = _ahora_gmt5()
    print(data_proceso_actual)
    print("--------------------------------*!d")
    print("luis estuvo aqui")
    #capturas los datos de los dispositivos 
    dispositivos = []
    for notificacion in data_proceso_actual.find({"estado": 1}, {"_id": 0}):
        #imei = notificacion.get("imei")
        #dispositivos.append({
        #    "imei": imei,
        #})
        #print(imei)
        #print("--------------------------------")
        dispositivos.append(notificacion)
    return dispositivos

def detectar_errores(etiquetas, datos):
    resultado = {}
    for etiqueta in etiquetas:
        valor = datos.get(etiqueta)
        if isinstance(valor, str) and "E" in valor:
            resultado[etiqueta] = valor
        elif valor is None:
            resultado[etiqueta] = "E100"
    return resultado

def validar_valor(json_data, clave,min_temp, max_temp=1000000000):

    valor = json_data.get(clave, None)
    if valor is None:
        return None
    try:
        min_temp = float(min_temp)
        max_temp = float(max_temp)
        valor = float(valor)
    except (ValueError, TypeError):
        return None
    if min_temp <= valor <= max_temp:
        return valor
    else:
        return None

conjunto_etiquetas = A_sensor_readout_dataset + A_sensor_readout_set + A_sensor_readout_alarma 

def estructura_termoking(json_validar):
    json_ok = {
        "temp_supply_1": validar_valor(json_validar,"temp_supply_1",-50,130),
        "temp_supply_2": validar_valor(json_validar,"temp_supply_2",-50,130),
        "return_air": validar_valor(json_validar,"return_air",-50,130),
        "evaporation_coil": validar_valor(json_validar,"evaporation_coil",-50,130),
        "condensation_coil": validar_valor(json_validar,"condensation_coil",-50,130),
        "compress_coil_1": validar_valor(json_validar,"compress_coil_1",-50,130),
        "compress_coil_2": validar_valor(json_validar,"compress_coil_2",-50,130),
        "ambient_air": validar_valor(json_validar,"ambient_air",-50,130),
        "cargo_1_temp": validar_valor(json_validar,"cargo_1_temp",-50,130),
        "cargo_2_temp": validar_valor(json_validar,"cargo_2_temp",-50,130),
        "cargo_3_temp": validar_valor(json_validar,"cargo_3_temp",-50,130),
        "cargo_4_temp": validar_valor(json_validar,"cargo_4_temp",-50,130),
        "relative_humidity": validar_valor(json_validar,"relative_humidity",0,100),
        "avl": validar_valor(json_validar,"avl",0,250),
        "suction_pressure": validar_valor(json_validar,"suction_pressure",-0),
        "discharge_pressure": validar_valor(json_validar,"discharge_pressure",0),
        "line_voltage": validar_valor(json_validar,"line_voltage",0,500),
        "line_frequency": validar_valor(json_validar,"line_frequency",0,100),
        "consumption_ph_1": validar_valor(json_validar,"consumption_ph_1",0,100),
        "consumption_ph_2": validar_valor(json_validar,"consumption_ph_2",0,100),
        "consumption_ph_3": validar_valor(json_validar,"consumption_ph_3",0,100),
        "co2_reading": validar_valor(json_validar,"co2_reading",0,22),
        "o2_reading": validar_valor(json_validar,"o2_reading",0,22),
        "evaporator_speed": validar_valor(json_validar,"evaporator_speed",-50,130),
        "condenser_speed": validar_valor(json_validar,"condenser_speed",-50,130),
        "battery_voltage": validar_valor(json_validar,"battery_voltage",-50,130),
        "power_kwh":validar_valor(json_validar,"power_kwh",0),
        "power_trip_reading": validar_valor(json_validar,"power_trip_reading",0),
        "power_trip_duration": validar_valor(json_validar,"power_trip_duration",0),
        "suction_temp": validar_valor(json_validar,"suction_temp",-50,130),
        "discharge_temp": validar_valor(json_validar,"discharge_temp",-50,130),
        "supply_air_temp": validar_valor(json_validar,"supply_air_temp",-50,130),
        "return_air_temp": validar_valor(json_validar,"return_air_temp",-50,130),
        "dl_battery_temp": validar_valor(json_validar,"dl_battery_temp",-50,130),
        "dl_battery_charge": validar_valor(json_validar,"dl_battery_charge",0),
        "power_consumption": validar_valor(json_validar,"power_consumption",0),
        "power_consumption_avg": validar_valor(json_validar,"power_consumption_avg",0),
        "suction_pressure_2": validar_valor(json_validar,"suction_pressure_2",0),
        "suction_temp_2": validar_valor(json_validar,"suction_temp_2",-50,130),
        "alarm_present": validar_valor(json_validar,"alarm_present",0,100),
        "set_point": validar_valor(json_validar,"set_point",-50,130),
        "capacity_load": validar_valor(json_validar,"capacity_load",0,100),
        "power_state": validar_valor(json_validar,"power_state",0,2),
        "controlling_mode": validar_valor(json_validar,"controlling_mode",0,10),
        "humidity_control": validar_valor(json_validar,"humidity_control",0,2),
        "humidity_set_point": validar_valor(json_validar,"humidity_set_point",0,100),
        "fresh_air_ex_mode": validar_valor(json_validar,"fresh_air_ex_mode",0,3),
        "fresh_air_ex_rate": validar_valor(json_validar,"fresh_air_ex_rate",0),
        "fresh_air_ex_delay": validar_valor(json_validar,"fresh_air_ex_delay",0),
        "set_point_o2": validar_valor(json_validar,"set_point_o2",0,22),
        "set_point_co2": validar_valor(json_validar,"set_point_co2",0,22),
        "defrost_term_temp": validar_valor(json_validar,"defrost_term_temp",-50,130),
        "defrost_interval": validar_valor(json_validar,"defrost_interval",0,24),
        "water_cooled_conde": validar_valor(json_validar,"water_cooled_conde",0,2),
        "usda_trip": validar_valor(json_validar,"usda_trip",0,2),
        "evaporator_exp_valve": validar_valor(json_validar,"evaporator_exp_valve",0),
        "suction_mod_valve": validar_valor(json_validar,"suction_mod_valve",0),
        "hot_gas_valve": validar_valor(json_validar,"hot_gas_valve",0),
        "economizer_valve": validar_valor(json_validar,"economizer_valve",0),
        "sp_ethyleno": validar_valor(json_validar,"sp_ethyleno",0,300),
        "stateProcess": str(validar_valor(json_validar,"stateProcess",0,300)),
        "inyeccion_hora": validar_valor(json_validar,"inyeccion_hora",0,300),
        "ethylene": validar_valor(json_validar,"ethylene",0,350),
        "numero_alarma": validar_valor(json_validar,"numero_alarma",0,11),
        "alarma_01": validar_valor(json_validar,"alarma_01",0,300),
        "alarma_02": validar_valor(json_validar,"alarma_02",0,300),
        "alarma_03": validar_valor(json_validar,"alarma_03",0,300),
        "alarma_04": validar_valor(json_validar,"alarma_04",0,300),
        "alarma_05": validar_valor(json_validar,"alarma_05",0,300),
        "alarma_06": validar_valor(json_validar,"alarma_06",0,300),
        "alarma_07": validar_valor(json_validar,"alarma_07",0,300),
        "alarma_08": validar_valor(json_validar,"alarma_08",0,300),
        "alarma_09": validar_valor(json_validar,"alarma_09",0,300),
        "alarma_10": validar_valor(json_validar,"alarma_10",0,300),
        "lecturas_erradas" : detectar_errores(conjunto_etiquetas,json_validar),

        "iRspRip": validar_valor(json_validar,"iRspRip",0,10),
        "iOptRip": validar_valor(json_validar,"iOptRip",0,10),
        "iCtrlRip": validar_valor(json_validar,"iCtrlRip",0,10),
        "SP_PPM": validar_valor(json_validar,"SP_PPM",0,300),
        "iDlyRip": validar_valor(json_validar,"iDlyRip",0,10),
        "iDrtRip": validar_valor(json_validar,"iDrtRip",0,100),
        "PPM_Sensor": validar_valor(json_validar,"PPM_Sensor",0,350),

        "imei": json_validar['i'],
        "ip": json_validar['ip'],
        "device": json_validar['i'],
        "fecha": json_validar['fecha']
    }
    return json_ok




def ultimo_estado_dispositivos_termoking() -> Dict[str, Any]:
    """
    Último estado de cada dispositivo según TK_PROCESO_MES_AÑO e IMEI_OFICIAL_AÑO.
    Incluye resumen por estado (online/wait/offline), campos elementales, power_state on/off
    y en_rango (return_air ±5 respecto a set_point). Pensado para mostrar en tabla.
    """
    data_proceso_actual = collection(bd_gene("proceso"))
    dispositivos: List[Dict[str, Any]] = []
    ahora_gmt5 = _ahora_gmt5()
    print(data_proceso_actual)
    print("--------------------------------*")
    print("luis estuvo aqui")
    for notificacion in data_proceso_actual.find({"estado": 1}, {"_id": 0, "imei": 1}):
        imei = notificacion.get("imei")
        print(imei)
        print("--------------------------------")
        if not imei:
            continue
        coll_oficial = collection(bd_oficial(imei))
        ultimo = coll_oficial.find_one(
            {},
            {"_id": 0},
            sort=[("fecha", -1)]
        )

        fecha_ultima: Optional[datetime] = ultimo.get("fecha") if ultimo else None
        fecha_gmt5 = _fecha_ultima_como_gmt5(fecha_ultima)
        estado_conexion = _calcular_estado_conexion(fecha_ultima)

        if fecha_gmt5 is not None:
            minutos_desde = (ahora_gmt5 - fecha_gmt5).total_seconds() / 60
            ultima_actualizacion = fecha_ultima.isoformat() if fecha_ultima else None
        else:
            minutos_desde = None
            ultima_actualizacion = None

        if ultimo:
            dato = _filtrar_campos_elementales(ultimo)
            # created_at puede venir como fecha; normalizar a ISO
            if dato.get("created_at") is None and fecha_ultima:
                dato["created_at"] = fecha_ultima.isoformat()
            elif isinstance(dato.get("created_at"), datetime):
                dato["created_at"] = dato["created_at"].isoformat()
            power_state = ultimo.get("power_state")
            dato["power_state_texto"] = "on" if power_state == 1 else "off"
            dato["en_rango"] = _calcular_en_rango(ultimo.get("set_point"), ultimo.get("return_air"))
            en_defrost = _calcular_en_defrost(
                ultimo.get("set_point"),
                ultimo.get("temp_supply_1"),
                ultimo.get("return_air"),
                ultimo.get("evaporation_coil"),
            )
        else:
            dato = {k: None for k in _CAMPOS_ULTIMO_ESTADO}
            dato["power_state_texto"] = None
            dato["en_rango"] = None
            en_defrost = None

        dispositivos.append({
            "imei": imei,
            "estado_conexion": estado_conexion,
            "ultima_actualizacion": ultima_actualizacion,
            "minutos_desde_ultimo_dato": round(minutos_desde, 1) if minutos_desde is not None else None,
            "power_state_texto": dato.pop("power_state_texto"),
            "en_rango": dato.pop("en_rango"),
            "en_defrost": en_defrost,
            "ultimo_dato": dato,
        })

    # Resumen para tabla (todas las fechas/horas consideradas en GMT-5)
    online = sum(1 for d in dispositivos if d["estado_conexion"] == "online")
    wait = sum(1 for d in dispositivos if d["estado_conexion"] == "wait")
    offline = sum(1 for d in dispositivos if d["estado_conexion"] == "offline")
    en_defrost_count = sum(1 for d in dispositivos if d.get("en_defrost") is True)
    power_on_count = sum(1 for d in dispositivos if d.get("power_state_texto") == "on")
    power_off_count = sum(1 for d in dispositivos if d.get("power_state_texto") == "off")

    return {
        "resumen": {
            "total_dispositivos": len(dispositivos),
            "online": online,
            "wait": wait,
            "offline": offline,
            "en_defrost": en_defrost_count,
            "power_on": power_on_count,
            "power_off": power_off_count,
            "zona_horaria": "GMT-5",
        },
        "dispositivos": dispositivos,
    }



