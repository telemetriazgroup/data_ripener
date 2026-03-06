from fastapi import APIRouter, Body
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse


from datetime import datetime, timedelta, timezone

from typing import Optional,List

from server.functions.termoking import (
    lista_imeis_termoking,
    ultimo_estado_dispositivos_termoking,
    reconstruccion_green_box,
    estado_general_dispositivos,
    historial_tratado,
    actualizar_incrementalmente,
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

#ruta para actualizar incrementalmente
@router.get("/actualizar_incrementalmente/", response_description="Actualizacion incremental de los datos.")
def actualizar_incrementalmente_ok():
    """
    Actualizacion incremental de los datos.
    """
    data = actualizar_incrementalmente()
    return ResponseModel(data, "Actualizacion incremental de los datos realizada correctamente.")

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

    # ruta
@router.get("/estado_general/", response_description="Estado consolidado desde General_dispositivos.")
def estado_general_ok():
    data = estado_general_dispositivos()
    return ResponseModel(data, "Estado general recuperado correctamente.")

    

@router.get("/historial/{imei}/", response_description="Historial de tramas TRATADO por IMEI.")
def historial_tratado_ok(
    imei: str,
    fecha_inicio: Optional[str] = None,   # ISO: "2026-03-06T00:00:00"
    fecha_fin:    Optional[str] = None,
):
    """
    Últimas 12 horas por defecto, o rango personalizado vía query params.
    Máximo 7 días por consulta.

    Ejemplos:
      GET /termoking/historial/CC:DB:A7:9D:F3:E8/
      GET /termoking/historial/CC:DB:A7:9D:F3:E8/?fecha_inicio=2026-03-06T00:00:00
      GET /termoking/historial/CC:DB:A7:9D:F3:E8/?fecha_inicio=2026-03-05T00:00:00&fecha_fin=2026-03-06T00:00:00
    """
    def _parse(s: Optional[str]) -> Optional[datetime]:
        if not s:
            return None
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            return None

    data = historial_tratado(imei, _parse(fecha_inicio), _parse(fecha_fin))
    return ResponseModel(data, "Historial recuperado correctamente.")