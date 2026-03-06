from fastapi import APIRouter, Body
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from server.functions.termoking import (
    lista_imeis_termoking,
    ultimo_estado_dispositivos_termoking,
    reconstruccion_green_box,
)
#Aqui importamos el modelo necesario para la clase 
from server.models.termoking import (
    ErrorResponseModel,
    ResponseModel,
    TermoKingSchema,
    BusquedaSchema,
    BusquedaGeneral,
    ComandoSchema,
    BusquedaSchema_proceso,

)
router = APIRouter()

@router.get("/reconstruccion_green_box/", response_description="Reconstruccion de la coleccion de los dispositivos.")
def reconstruccion_green_box_ok():
    """
    Reconstruccion de la coleccion de los dispositivos.
    """
    data = reconstruccion_green_box()
    return ResponseModel(data, "Reconstruccion de la coleccion de los dispositivos realizada correctamente.")

@router.get("/lista_imeis_termoking/", response_description="Lista de IMEIs de los dispositivos.")
def  lista_imeis_termoking_ok():
    """
    Lista de IMEIs de los dispositivos.
    """
    data =  lista_imeis_termoking()
    return ResponseModel(data, "Lista de IMEIs de los dispositivos recuperada correctamente.")

@router.get("/ultimo_estado_dispositivos/", response_description="Resumen y último estado por dispositivo para tabla.")
def ultimo_estado_dispositivos_ok():
    """
    Resumen: total, online/wait/offline (GMT-5), en_defrost, power_on/power_off.
    Por dispositivo: campos elementales, power_state (on/off), en_rango (±5 vs set_point), en_defrost.
    """
    data =  ultimo_estado_dispositivos_termoking()
    return ResponseModel(data, "Último estado por dispositivo recuperado correctamente.")