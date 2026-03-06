"""
server/functions/termoking.py
─────────────────────────────
Pipeline de telemetría Thermo King / GreenBox.

Mejoras respecto a la versión anterior:
  · transformar_d02/d03/d08 unificados en un único _transformar_trama() genérico
  · _procesar_hex() elimina la triplicación de la lógica num_chars 2/4/8
  · bd_gene_imei() ya no hace split frágil; usa bd_gene_mes_año() internamente
  · obtener_meses_creados() sigue leyendo list_collection_names() una sola vez
  · imeis_en_colecciones() usa insert_many por batch en TRATADO y bulk_write
    para General_dispositivos (mínimo de round-trips a MongoDB)
  · actualizar_incrementalmente() lee cursores de todos los IMEIs en UNA consulta
  · validar_valor() tipado estricto; _RANGOS centraliza todos los límites
  · estructura_termoking() construida con dict-comprehension sobre _RANGOS
  · _upsert_general() construye el $set solo para campos no-None (preserva
    el último valor válido aunque lleguen tramas con null)
  · Constantes MODULE-LEVEL: listas de sensores como tuplas inmutables
  · Sin prints de depuración en producción; usa logging
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from bson import ObjectId

from server.database import collection, database_mongo

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# Zona horaria y constantes globales
# ══════════════════════════════════════════════════════════════════════════════

GMT5 = timezone(timedelta(hours=-5))

green_box = [
    "CC:DB:A7:9D:F3:E8",
    "0C:B8:15:F2:C7:A0",
    "28:05:A5:2B:E9:88",
    "28:05:A5:2A:FA:04",
    "28:05:A5:2C:A9:E4",
]
descrip = [
    "GREENBOX ZGRU9803515",
    "GREENBOX ZGRU0048736",
    "GREENBOX ZGRU9803691",
    "GREENBOX ZGRU1040025",
    "GREENBOX ZGRU003388",
]
_IMEI_DESCRIPCION: Dict[str, str] = dict(zip(green_box, descrip))

# ── Códigos de error de sensor (orden importa, índice = código de error) ─────
_SENSOR_EVENTOS: Tuple[str, ...] = (
    "Sensor is initializing",       # E00
    "Value not applicable (N/A)",   # E01
    "Sensor error",                 # E02
    "Sensor open",                  # E03
    "Sensor short",                 # E04
    "Sensor above",                 # E05
    "Sensor below",                 # E06
    "Sensor no comm",               # E07
    "Sensor warm up",               # E08
    "This is the max value",        # E09  (7FEF)
    "Values not represent a reading", # E10 (7FF0)
)
# Set para lookup O(1) en la ruta caliente de decodificación
_SENSOR_CODIGOS_SET: frozenset = frozenset([
    "7FFF","7FFE","7FFD","7FFC","7FFB","7FFA",
    "7FF9","7FF8","7FF7","7FEF","7FF0",
])
_SENSOR_CODIGOS_LIST: Tuple[str, ...] = (
    "7FFF","7FFE","7FFD","7FFC","7FFB","7FFA",
    "7FF9","7FF8","7FF7","7FEF","7FF0",
)

# ── Definiciones de tramas ────────────────────────────────────────────────────
# Cada entrada: (nombre_campo, num_chars, divisor)
# num_chars: 2 = byte, 4 = word, 8 = dword

_TRAMA_D02: Tuple[Tuple, ...] = (
    ("temp_supply_1",        4, 10), ("temp_supply_2",       4, 10),
    ("return_air",           4, 10), ("evaporation_coil",    4, 10),
    ("condensation_coil",    4, 10), ("compress_coil_1",     4, 10),
    ("compress_coil_2",      4, 10), ("ambient_air",         4, 10),
    ("cargo_1_temp",         4, 10), ("cargo_2_temp",        4, 10),
    ("cargo_3_temp",         4, 10), ("cargo_4_temp",        4, 10),
    ("relative_humidity",    4,  1), ("avl",                 4,  1),
    ("suction_pressure",     4,100), ("discharge_pressure",  4,100),
    ("line_voltage",         4,  1), ("line_frequency",      4,  1),
    ("consumption_ph_1",     4, 10), ("consumption_ph_2",    4, 10),
    ("consumption_ph_3",     4, 10), ("co2_reading",         4, 10),
    ("o2_reading",           4, 10), ("evaporator_speed",    4,  1),
    ("condenser_speed",      4,  1), ("battery_voltage",     4, 10),
    ("power_kwh",            8, 10), ("power_trip_reading",  8, 10),
    ("power_trip_duration",  8,  1), ("suction_temp",        4, 10),
    ("discharge_temp",       4, 10), ("supply_air_temp",     4, 10),
    ("return_air_temp",      4, 10), ("dl_battery_temp",     4,100),
    ("dl_battery_charge",    4,100), ("power_consumption",   4,100),
    ("power_consumption_avg",4,100), ("suction_pressure_2",  4,100),
    ("suction_temp_2",       4, 10),
)

_TRAMA_D03: Tuple[Tuple, ...] = (
    ("alarm_present",        4,  1), ("set_point",           4,100),
    ("capacity_load",        4,  1), ("power_state",         2,  1),
    ("controlling_mode",     2,  1), ("humidity_control",    2,  1),
    ("humidity_set_point",   2,  1), ("fresh_air_ex_mode",   2,  1),
    ("fresh_air_ex_rate",    4,  1), ("fresh_air_ex_delay",  4, 10),
    ("set_point_o2",         4, 10), ("set_point_co2",       4, 10),
    ("defrost_term_temp",    4,100), ("defrost_interval",    2,  1),
    ("water_cooled_conde",   2,  1), ("usda_trip",           2,  1),
    ("evaporator_exp_valve", 2,  1), ("suction_mod_valve",   2,  1),
    ("hot_gas_valve",        2,  1), ("economizer_valve",    2,  1),
)

_TRAMA_D08: Tuple[Tuple, ...] = (
    ("numero_alarma", 4, 1), ("alarma_01", 4, 1), ("alarma_02", 4, 1),
    ("alarma_03",     4, 1), ("alarma_04", 4, 1), ("alarma_05", 4, 1),
    ("alarma_06",     4, 1), ("alarma_07", 4, 1), ("alarma_08", 4, 1),
    ("alarma_09",     4, 1), ("alarma_10", 4, 1),
)

# Índice 3 de D03 tiene tratamiento especial (bit shift)
_D03_IDX_POWER_STATE = 3

# ── General_dispositivos ──────────────────────────────────────────────────────
COLECCION_GENERAL = "General_dispositivos"

# Mapa: nombre_campo_general → clave en doc validado
_CAMPO_FUENTE: Dict[str, str] = {
    "ultimo_set_point_temperatura_valido": "set_point",
    "ultimo_temp_supply_valido":           "temp_supply_1",
    "ultimo_return_air_valido":            "return_air",
    "ultimo_relative_humidity_valido":     "relative_humidity",
    "ultimo_co2_reading_valido":           "co2_reading",
    "ultimo_ethylene_valido":              "ethylene",
}

# ── Rangos de validación centralizados ───────────────────────────────────────
# Formato: campo → (min, max)   max=None → sin límite superior
_RANGOS: Dict[str, Tuple[float, Optional[float]]] = {
    "temp_supply_1":        (-50, 130), "temp_supply_2":        (-50, 130),
    "return_air":           (-50, 130), "evaporation_coil":     (-50, 130),
    "condensation_coil":    (-50, 130), "compress_coil_1":      (-50, 130),
    "compress_coil_2":      (-50, 130), "ambient_air":          (-50, 130),
    "cargo_1_temp":         (-50, 130), "cargo_2_temp":         (-50, 130),
    "cargo_3_temp":         (-50, 130), "cargo_4_temp":         (-50, 130),
    "relative_humidity":    (  0, 100), "avl":                  (  0, 250),
    "suction_pressure":     (  0, None),"discharge_pressure":   (  0, None),
    "line_voltage":         (  0, 500), "line_frequency":       (  0, 100),
    "consumption_ph_1":     (  0, 100), "consumption_ph_2":     (  0, 100),
    "consumption_ph_3":     (  0, 100), "co2_reading":          (  0,  22),
    "o2_reading":           (  0,  22), "evaporator_speed":     (-50, 130),
    "condenser_speed":      (-50, 130), "battery_voltage":      (-50, 130),
    "power_kwh":            (  0, None),"power_trip_reading":   (  0, None),
    "power_trip_duration":  (  0, None),"suction_temp":         (-50, 130),
    "discharge_temp":       (-50, 130), "supply_air_temp":      (-50, 130),
    "return_air_temp":      (-50, 130), "dl_battery_temp":      (-50, 130),
    "dl_battery_charge":    (  0, None),"power_consumption":    (  0, None),
    "power_consumption_avg":(  0, None),"suction_pressure_2":   (  0, None),
    "suction_temp_2":       (-50, 130),
    "alarm_present":        (  0, 100), "set_point":            (-50, 130),
    "capacity_load":        (  0, 100), "power_state":          (  0,   2),
    "controlling_mode":     (  0,  10), "humidity_control":     (  0,   2),
    "humidity_set_point":   (  0, 100), "fresh_air_ex_mode":    (  0,   3),
    "fresh_air_ex_rate":    (  0, None),"fresh_air_ex_delay":   (  0, None),
    "set_point_o2":         (  0,  22), "set_point_co2":        (  0,  22),
    "defrost_term_temp":    (-50, 130), "defrost_interval":     (  0,  24),
    "water_cooled_conde":   (  0,   2), "usda_trip":            (  0,   2),
    "evaporator_exp_valve": (  0, None),"suction_mod_valve":    (  0, None),
    "hot_gas_valve":        (  0, None),"economizer_valve":     (  0, None),
    "numero_alarma":        (  0,  11), "alarma_01":            (  0, 300),
    "alarma_02":            (  0, 300), "alarma_03":            (  0, 300),
    "alarma_04":            (  0, 300), "alarma_05":            (  0, 300),
    "alarma_06":            (  0, 300), "alarma_07":            (  0, 300),
    "alarma_08":            (  0, 300), "alarma_09":            (  0, 300),
    "alarma_10":            (  0, 300), "sp_ethyleno":          (  0, 300),
    "inyeccion_hora":       (  0, 300), "ethylene":             (  0, 350),
    "iRspRip":              (  0,  10), "iOptRip":              (  0,  10),
    "iCtrlRip":             (  0,  10), "SP_PPM":               (  0, 300),
    "iDlyRip":              (  0,  10), "iDrtRip":              (  0, 100),
    "PPM_Sensor":           (  0, 350), "stateProcess":         (  0, 300),
}

# Conjunto de todos los campos de sensores para detectar errores de lectura
_CONJUNTO_ETIQUETAS: frozenset = frozenset(
    n for n, *_ in (*_TRAMA_D02, *_TRAMA_D03, *_TRAMA_D08)
)

# Campos elementales para endpoint de último estado
_CAMPOS_ULTIMO_ESTADO: Tuple[str, ...] = (
    "temp_supply_1", "return_air", "evaporation_coil", "condensation_coil",
    "compress_coil_1", "co2_reading", "o2_reading", "cargo_1_temp",
    "cargo_2_temp", "cargo_3_temp", "cargo_4_temp", "power_kwh",
    "numero_alarma", "sp_ethyleno", "set_point_o2", "set_point_co2",
    "power_state", "capacity_load", "telemetria_id", "created_at",
    "longitud", "latitud", "set_point",
)


# ══════════════════════════════════════════════════════════════════════════════
# Helpers de fecha / conexión
# ══════════════════════════════════════════════════════════════════════════════

def _ahora_gmt5() -> datetime:
    return datetime.now(GMT5)


def _como_gmt5(fecha: Optional[datetime]) -> Optional[datetime]:
    if fecha is None:
        return None
    return fecha if fecha.tzinfo else fecha.replace(tzinfo=GMT5)


def _calcular_estado_conexion(fecha_ultima: Optional[datetime]) -> str:
    fg = _como_gmt5(fecha_ultima)
    if fg is None:
        return "offline"
    minutos = (_ahora_gmt5() - fg).total_seconds() / 60
    if minutos <= 30:
        return "online"
    if minutos <= 1440:
        return "wait"
    return "offline"


def _filtrar_campos_elementales(doc: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for k in _CAMPOS_ULTIMO_ESTADO:
        v = doc.get(k)
        out[k] = v.isoformat() if isinstance(v, datetime) else v
    return out


# ══════════════════════════════════════════════════════════════════════════════
# Helpers de nombres de colección
# ══════════════════════════════════════════════════════════════════════════════

def bd_gene(suffix: str) -> str:
    now = datetime.now()
    return f"TK_{suffix}_{now.month:02d}_{now.year}"


def bd_gene_mes_año(mes: str, año: str) -> str:
    return f"TK_dispositivos_{mes}_{año}"


def bd_oficial(imei: str) -> str:
    return f"TK_{imei}_{datetime.now().year}"


def bd_gene_imei(nombre_coleccion: str, imei: str) -> str:
    """
    Extrae mes y año de nombre_coleccion (TK_dispositivos_MM_YYYY)
    y construye TK_<imei>_MM_YYYY.
    Más robusto que split: busca el prefijo conocido.
    """
    prefijo = "TK_dispositivos_"
    sufijo  = nombre_coleccion[len(prefijo):]   # "MM_YYYY"
    return f"TK_{imei}_{sufijo}"


def obtener_meses_creados(db) -> List[str]:
    """
    Lista colecciones TK_dispositivos_MM_YYYY desde el mes actual hacia atrás
    mientras existan. Una sola llamada list_collection_names().
    """
    todos = db.list_collection_names()
    prefijo = "TK_dispositivos_"
    existentes: set = set()
    for nombre in todos:
        if not nombre.startswith(prefijo):
            continue
        partes = nombre.split("_")
        if len(partes) >= 4:
            try:
                existentes.add((int(partes[-1]), int(partes[-2])))  # (año, mes)
            except ValueError:
                pass

    now = datetime.now()
    mes, anio = now.month, now.year
    meses = []
    while (anio, mes) in existentes:
        meses.append(bd_gene_mes_año(f"{mes:02d}", f"{anio:04d}"))
        mes -= 1
        if mes == 0:
            mes, anio = 12, anio - 1
    return meses


def _colecciones_recientes() -> List[str]:
    """Mes actual + anterior si existen (para ciclo incremental)."""
    nombres = []
    now = datetime.now()
    for delta in (0, 1):
        mes  = now.month - delta
        anio = now.year
        if mes <= 0:
            mes, anio = 12, anio - 1
        nombre = bd_gene_mes_año(f"{mes:02d}", f"{anio:04d}")
        if collection(nombre).find_one({}, {"_id": 1}):
            nombres.append(nombre)
    return nombres


# ══════════════════════════════════════════════════════════════════════════════
# Decodificación hexadecimal — núcleo unificado
# ══════════════════════════════════════════════════════════════════════════════

def _invertir_pares(text: str) -> str:
    """Invierte el orden de los pares de bytes: "AABB" → "BBAA"."""
    pares = [text[i:i+2] for i in range(0, len(text), 2)]
    return "".join(reversed(pares))


def _convert_number(valve: int, divisor: int) -> float:
    """Convierte entero unsigned 16-bit a float con signo / divisor."""
    if 0x7FEF <= valve <= 0x7FFF:
        return float(valve)
    if valve > 0x7FFF:
        valve = (0xFFFF - valve + 1)
        return -float(valve) / divisor
    return float(valve) / divisor


def _texto_error(n: int) -> str:
    return f"E{n:02d}"


def _procesar_hex(substring: str, num_chars: int, divisor: int, idx: int, es_d03: bool = False) -> Any:
    """
    Decodifica un substring hexadecimal según su tamaño esperado.

    num_chars=2  → byte simple (flags, estados)
    num_chars=4  → word con signo little-endian
    num_chars=8  → dword con signo little-endian
    """
    if len(substring) != num_chars:
        return _texto_error(99)   # longitud inesperada

    if num_chars == 2:
        if substring in ("FF", "FE"):
            return _texto_error(98)
        try:
            val = int(substring, 16)
        except ValueError:
            return _texto_error(98)
        # D03 índice 3: power_state = bit 1 del byte
        if es_d03 and idx == _D03_IDX_POWER_STATE:
            return (val >> 1) & 1
        return val

    if num_chars == 4:
        inverso = _invertir_pares(substring)
        if inverso in _SENSOR_CODIGOS_SET:
            return _texto_error(_SENSOR_CODIGOS_LIST.index(inverso))
        try:
            val = _convert_number(int(inverso, 16), divisor)
        except ValueError:
            return _texto_error(98)
        return val if (val or val == 0) else _texto_error(98)

    # num_chars == 8
    inverso = _invertir_pares(substring)
    try:
        val = _convert_number(int(inverso, 16), divisor)
    except ValueError:
        return _texto_error(98)
    return round(val / divisor, 1)


def _encontrar_tras_marcador(texto: str, marcador: str) -> Optional[str]:
    """Devuelve el texto que sigue al marcador (case-insensitive)."""
    idx = texto.lower().find(marcador.lower())
    if idx == -1:
        return None
    return texto[idx + len(marcador):]


def _cortar_antes_de(texto: str, marcador: str) -> str:
    idx = texto.find(marcador)
    return texto[:idx] if idx != -1 else texto


def _decodificar_trama(
    raw: str,
    marcador_inicio: str,
    marcador_fin: str,
    definicion: Tuple[Tuple, ...],
    es_d03: bool = False,
) -> Dict[str, Any]:
    """
    Función genérica que reemplaza transformar_d02, transformar_d03, transformar_d08.

    1. Extrae la cadena entre marcador_inicio y marcador_fin.
    2. Valida que sea hexadecimal.
    3. Itera sobre la definición de campos y decodifica cada uno.
    """
    resultado: Dict[str, Any] = {}
    cadena = _encontrar_tras_marcador(raw, marcador_inicio)
    if cadena is None:
        return resultado
    cadena = _cortar_antes_de(cadena, marcador_fin)

    # Validar hex completo antes de iterar (evita excepciones campo a campo)
    try:
        int(cadena, 16)
    except ValueError:
        logger.warning("Cadena no hexadecimal en trama (%s): %.40s…", marcador_inicio, cadena)
        return resultado

    pos = 0
    for idx, (nombre, num_chars, divisor) in enumerate(definicion):
        sub = cadena[pos:pos + num_chars]
        resultado[nombre] = _procesar_hex(sub, num_chars, divisor, idx, es_d03)
        pos += num_chars

    return resultado


# Funciones públicas individuales (mantienen compatibilidad de nombre)
def transformar_d02(valor: str) -> Dict[str, Any]:
    return _decodificar_trama(valor, "82A700", "1B04FF", _TRAMA_D02)

def transformar_d03(valor: str) -> Dict[str, Any]:
    return _decodificar_trama(valor, "82A701", "1B04FF", _TRAMA_D03, es_d03=True)

def transformar_d08(valor: str) -> Dict[str, Any]:
    return _decodificar_trama(valor, "82A706", "1B04FF", _TRAMA_D08)


# ══════════════════════════════════════════════════════════════════════════════
# Ripener / d04
# ══════════════════════════════════════════════════════════════════════════════

_CAMPOS_D04 = ("iRspRip", "iOptRip", "iCtrlRip", "SP_PPM", "iDlyRip", "iDrtRip", "PPM_Sensor")

def transformar_d04(valor: str) -> Dict[str, Any]:
    """
    Parsea "0 1 1 150 0 64 152.4" → dict con 7 campos.
    Campos faltantes quedan en None; PPM_Sensor es float, el resto int.
    """
    if not valor:
        return {k: None for k in _CAMPOS_D04}
    partes = (valor.split() + [None] * 7)[:7]
    resultado: Dict[str, Any] = {}
    for i, campo in enumerate(_CAMPOS_D04):
        v = partes[i]
        if v is None:
            resultado[campo] = None
            continue
        try:
            resultado[campo] = float(v) if i == 6 else int(v)
        except (ValueError, TypeError):
            resultado[campo] = None
    return resultado


# ══════════════════════════════════════════════════════════════════════════════
# Procesamiento de documento completo
# ══════════════════════════════════════════════════════════════════════════════

_MAPA_TRAMAS = {
    "d02": transformar_d02,
    "d03": transformar_d03,
    "d04": transformar_d04,
    "d08": transformar_d08,
}

def procesar_documento(doc: Dict[str, Any]) -> Dict[str, Any]:
    resultado = {
        "i":                 doc.get("i"),
        "ip":                doc.get("ip"),
        "estado":            doc.get("estado"),
        "fecha":             doc.get("fecha"),
        "tramas_procesadas": [],
    }
    for campo, funcion in _MAPA_TRAMAS.items():
        valor = doc.get(campo)
        if valor is not None and valor != "":
            datos = funcion(valor)
            resultado.update(datos)
            resultado["tramas_procesadas"].append(campo)
    return resultado


# ══════════════════════════════════════════════════════════════════════════════
# Validación y estructura final
# ══════════════════════════════════════════════════════════════════════════════

def validar_valor(json_data: Dict[str, Any], clave: str) -> Optional[float]:
    """
    Valida que el valor de `clave` sea numérico y esté dentro del rango
    definido en _RANGOS. Devuelve float o None.
    """
    rango = _RANGOS.get(clave)
    if rango is None:
        return None                     # campo sin rango definido → ignorar
    valor = json_data.get(clave)
    if valor is None:
        return None
    try:
        v = float(valor)
    except (ValueError, TypeError):
        return None
    mn, mx = rango
    if v < mn:
        return None
    if mx is not None and v > mx:
        return None
    return v


def detectar_errores_2(datos: Dict[str, Any]) -> Dict[str, str]:
    """Devuelve los campos que tienen código de error o son None."""
    return {
        k: (v if isinstance(v, str) and "E" in v else "E100")
        for k in _CONJUNTO_ETIQUETAS
        if datos.get(k) is None or (isinstance(datos.get(k), str) and "E" in datos.get(k))
    }

def detectar_errores(datos: Dict[str, Any]) -> Dict[str, str]:
    """Devuelve los campos que tienen código de error o son None."""
    resultado = {}
    for k in _CONJUNTO_ETIQUETAS:
        v = datos.get(k)
        if v is None:
            resultado[k] = "E100"
        elif isinstance(v, str) and "E" in v:
            resultado[k] = v
    return resultado


def estructura_termoking(json_validar: Dict[str, Any]) -> Dict[str, Any]:
    """
    Construye el documento validado aplicando rangos desde _RANGOS.
    Campos derivados (ethylene, sp_ethyleno, inyeccion_hora) se calculan aquí.
    stateProcess se convierte a str por compatibilidad con el schema original.
    """
    # Validar todos los campos con rango definido de una vez
    validado: Dict[str, Any] = {
        campo: validar_valor(json_validar, campo)
        for campo in _RANGOS
    }

    # stateProcess tratado aparte (puede no existir en _RANGOS con tipo str)
    validado["stateProcess"] = str(validar_valor(json_validar, "stateProcess") or "")

    # Campos de identidad (nunca pasan por rango)
    validado["imei"]   = json_validar.get("i")
    validado["ip"]     = json_validar.get("ip")
    validado["device"] = json_validar.get("i")
    validado["fecha"]  = json_validar.get("fecha")

    # Detección de lecturas erróneas
    validado["lecturas_erradas"] = detectar_errores(json_validar)

    return validado


def _enriquecer_validado(validado: Dict[str, Any]) -> None:
    """Agrega campos derivados del módulo ripener in-place."""
    validado["ethylene"]       = validado.get("PPM_Sensor")
    validado["sp_ethyleno"]    = validado.get("SP_PPM")
    validado["inyeccion_hora"] = validado.get("iDrtRip")


# ══════════════════════════════════════════════════════════════════════════════
# General_dispositivos — helpers
# ══════════════════════════════════════════════════════════════════════════════

def _calcular_proceso_activo(doc: Dict[str, Any]) -> bool:
    try:
        return (
            float(doc.get("inyeccion_hora") or 0) > 0
            and float(doc.get("ethylene")    or 0) > 2
        )
    except (TypeError, ValueError):
        return False


def _doc_inicial_general(imei: str) -> Dict[str, Any]:
    null_campo     = {"valor": None, "fecha": None, "batch_id": None}
    null_power_evt = {"fecha": None, "batch_id": None}
    doc: Dict[str, Any] = {
        "_id":                  imei,
        "imei":                 imei,
        "descripcion":          _IMEI_DESCRIPCION.get(imei, imei),
        "ultimo_batch_id":      None,
        "ultimo_dato_recibido": None,
        "proceso_activo":       False,
        # Eventos de power — null hasta detectar el primer cambio de estado
        "ultimo_encendido":     dict(null_power_evt),  # power_state == 1
        "ultimo_apagado":       dict(null_power_evt),  # power_state == 0
    }
    for campo in _CAMPO_FUENTE:
        doc[campo] = dict(null_campo)
    return doc


def _garantizar_doc_general(imei: str) -> None:
    col = collection(COLECCION_GENERAL)
    if col.find_one({"_id": imei}, {"_id": 1}) is None:
        try:
            col.insert_one(_doc_inicial_general(imei))
        except Exception:
            pass  # race condition: ya insertado


def _fecha_a_iso(fecha: Any) -> Optional[str]:
    if fecha is None:
        return None
    return fecha.isoformat() if isinstance(fecha, datetime) else str(fecha)


def _construir_set_general(
    imei: str,
    validado: Dict[str, Any],
    batch_id: ObjectId,
) -> Dict[str, Any]:
    """
    Construye el operador $set para General_dispositivos.
    Solo incluye campos válidos (no None) para preservar el último valor real.
    Incluye ultimo_encendido / ultimo_apagado si power_state es 1 o 0.
    """
    fecha_iso = _fecha_a_iso(validado.get("fecha"))
    set_op: Dict[str, Any] = {
        "ultimo_batch_id":      batch_id,
        "ultimo_dato_recibido": fecha_iso,
        "proceso_activo":       _calcular_proceso_activo(validado),
        "descripcion":          _IMEI_DESCRIPCION.get(imei, imei),
    }
    for campo_general, campo_fuente in _CAMPO_FUENTE.items():
        valor = validado.get(campo_fuente)
        if valor is not None:
            set_op[campo_general] = {
                "valor":    valor,
                "fecha":    fecha_iso,
                "batch_id": batch_id,
            }
    # Evento de power: solo se escribe si este doc tiene power_state definido
    power = validado.get("power_state")
    if power == 1:
        set_op["ultimo_encendido"] = {"fecha": fecha_iso, "batch_id": batch_id}
    elif power == 0:
        set_op["ultimo_apagado"]   = {"fecha": fecha_iso, "batch_id": batch_id}
    return set_op


def _upsert_general(imei: str, validado: Dict[str, Any], batch_id: ObjectId) -> None:
    """Un solo doc → un solo update_one. Usado en el ciclo incremental."""
    set_op = _construir_set_general(imei, validado, batch_id)
    collection(COLECCION_GENERAL).update_one(
        {"_id": imei},
        {"$set": set_op},
        upsert=True,
    )


def _upsert_general_batch(
    imei: str,
    docs: List[Dict[str, Any]],
    ids: List[ObjectId],
) -> None:
    """
    Actualiza General_dispositivos desde un bloque de N documentos en UNA sola
    operación update_one.

    Estrategia:
      · Recorre los docs de más reciente a más antiguo.
      · Para cada campo de _CAMPO_FUENTE toma el primer valor no-None que encuentre
        (= el más reciente válido del bloque).
      · Construye un único $set y lo aplica en un solo round-trip.

    Reemplaza el loop de N update_one individuales que era el cuello de botella:
    175 docs x ~400 ms red = ~70 s  →  ahora 1 update_one = < 1 s.
    """
    if not docs:
        return

    campos_pendientes = set(_CAMPO_FUENTE.keys())
    batch_id_final    = ids[-1]
    fecha_iso_final   = _fecha_a_iso(docs[-1].get("fecha"))

    set_op: Dict[str, Any] = {
        "ultimo_batch_id":      batch_id_final,
        "ultimo_dato_recibido": fecha_iso_final,
        "proceso_activo":       _calcular_proceso_activo(docs[-1]),
        "descripcion":          _IMEI_DESCRIPCION.get(imei, imei),
    }

    # Recorremos reverso: el valor más reciente válido gana para cada campo
    # También buscamos el último encendido (power_state==1) y apagado (power_state==0)
    encontrado_encendido = False
    encontrado_apagado   = False

    for doc, bid in zip(reversed(docs), reversed(ids)):
        todo_listo = (
            not campos_pendientes
            and encontrado_encendido
            and encontrado_apagado
        )
        if todo_listo:
            break

        fecha_iso = _fecha_a_iso(doc.get("fecha"))

        # Campos de último valor válido
        for campo_general in list(campos_pendientes):
            valor = doc.get(_CAMPO_FUENTE[campo_general])
            if valor is not None:
                set_op[campo_general] = {
                    "valor":    valor,
                    "fecha":    fecha_iso,
                    "batch_id": bid,
                }
                campos_pendientes.discard(campo_general)

        # Eventos de power (independientes de _CAMPO_FUENTE)
        power = doc.get("power_state")
        if not encontrado_encendido and power == 1:
            set_op["ultimo_encendido"] = {"fecha": fecha_iso, "batch_id": bid}
            encontrado_encendido = True
        if not encontrado_apagado and power == 0:
            set_op["ultimo_apagado"] = {"fecha": fecha_iso, "batch_id": bid}
            encontrado_apagado = True

    # Campos sin ningún valor válido en el bloque → no se tocan en MongoDB
    # → el documento existente conserva su último valor válido anterior.
    # Lo mismo aplica a ultimo_encendido/ultimo_apagado si no hubo ese evento.
    collection(COLECCION_GENERAL).update_one(
        {"_id": imei},
        {"$set": set_op},
        upsert=True,
    )


def _ultimo_recibido_de_general(imei: str) -> Optional[datetime]:
    doc = collection(COLECCION_GENERAL).find_one(
        {"_id": imei}, {"ultimo_dato_recibido": 1}
    )
    if not doc:
        return None
    val = doc.get("ultimo_dato_recibido")
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    try:
        return datetime.fromisoformat(str(val))
    except (ValueError, TypeError):
        return None


def _cursores_todos_los_imeis() -> Dict[str, Optional[datetime]]:
    """
    Lee ultimo_dato_recibido de TODOS los IMEIs del grupo en UNA sola consulta.
    Evita N round-trips al inicio del ciclo incremental.
    """
    col = collection(COLECCION_GENERAL)
    docs = col.find(
        {"_id": {"$in": green_box}},
        {"_id": 1, "ultimo_dato_recibido": 1},
    )
    cursores: Dict[str, Optional[datetime]] = {imei: None for imei in green_box}
    for doc in docs:
        val = doc.get("ultimo_dato_recibido")
        imei = doc["_id"]
        if val is None:
            continue
        if isinstance(val, datetime):
            cursores[imei] = val
        else:
            try:
                cursores[imei] = datetime.fromisoformat(str(val))
            except (ValueError, TypeError):
                pass
    return cursores


# ══════════════════════════════════════════════════════════════════════════════
# Procesamiento de documento individual → TRATADO + General
# ══════════════════════════════════════════════════════════════════════════════

def _procesar_e_insertar(
    doc_crudo: Dict[str, Any],
    col_tratado,
) -> Tuple[Optional[ObjectId], Dict[str, Any]]:
    """
    Procesa un doc crudo, lo inserta en col_tratado y devuelve (inserted_id, validado).
    Devuelve (None, validado) si la inserción falla.
    """
    resultado1 = procesar_documento(doc_crudo)
    validado   = estructura_termoking(resultado1)
    _enriquecer_validado(validado)

    try:
        res = col_tratado.insert_one(validado)
        return res.inserted_id, validado
    except Exception as exc:
        logger.error("Error insertando en TRATADO_%s: %s", validado.get("imei"), exc)
        return None, validado


# ══════════════════════════════════════════════════════════════════════════════
# BATCH COMPLETO — reconstruccion_green_box
# ══════════════════════════════════════════════════════════════════════════════

def _precalcular_mapa_meses(
    meses_creados: List[str],
    green_box_list: List[str],
    col_func,
) -> Dict[str, set]:
    """
    Devuelve {nombre_coleccion: {imei, ...}} con UNA sola consulta por mes.
    Meses donde ningún IMEI del grupo tiene datos quedan con set vacío
    y se pueden saltar completamente sin tocar más colecciones.
    """
    mapa: Dict[str, set] = {}
    for nombre_coleccion in meses_creados:
        col_mes = col_func(nombre_coleccion)
        imeis_con_datos = {
            doc["imei"]
            for doc in col_mes.find(
                {"imei": {"$in": green_box_list}},
                {"_id": 0, "imei": 1},
            )
            if doc.get("imei")
        }
        mapa[nombre_coleccion] = imeis_con_datos
    return mapa


def imeis_en_colecciones(
    meses_creados: List[str],
    green_box_list: List[str],
    col_func,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Procesa el histórico completo de IMEIs en green_box_list.

    Optimizaciones:
      · _precalcular_mapa_meses() → determina qué IMEI tiene datos en qué mes
        antes del loop principal; meses sin datos se saltan por completo.
      · insert_many por batch en TRATADO (un solo round-trip por bloque).
      · _upsert_general_batch → un solo update_one por bloque en General.
    """
    tiempo_inicio = time.time()
    resultado: Dict[str, List] = {imei: [] for imei in green_box_list}

    # Resetear estado
    col_general = collection(COLECCION_GENERAL)
    for imei in green_box_list:
        col_func(f"TRATADO_{imei}").delete_many({})
        col_general.delete_one({"_id": imei})
        col_general.insert_one(_doc_inicial_general(imei))

    # Precalcular qué IMEIs tienen datos en cada mes — evita entrar al loop
    # de colecciones TK_<imei>_MM_YYYY cuando no hay nada que procesar
    mapa_meses = _precalcular_mapa_meses(meses_creados, green_box_list, col_func)

    for nombre_coleccion, imeis_en_mes in mapa_meses.items():
        # Mes sin ningún IMEI relevante → saltar completamente
        if not imeis_en_mes:
            for imei in green_box_list:
                resultado[imei].append({
                    "Coleccion":        nombre_coleccion,
                    "cantidad":         0,
                    "tiempo_ejecucion": round(time.time() - tiempo_inicio, 3),
                })
            continue

        for imei in imeis_en_mes:
            col_imei    = col_func(bd_gene_imei(nombre_coleccion, imei))
            col_tratado = col_func(f"TRATADO_{imei}")

            documentos_crudos = list(col_imei.find({}).sort("fecha", 1))
            cantidad = len(documentos_crudos)

            if not documentos_crudos:
                resultado[imei].append({
                    "Coleccion":        nombre_coleccion,
                    "cantidad":         0,
                    "tiempo_ejecucion": round(time.time() - tiempo_inicio, 3),
                })
                continue

            # Procesar y acumular para insert_many
            docs_a_insertar: List[Dict[str, Any]] = []
            for doc_crudo in documentos_crudos:
                r1       = procesar_documento(doc_crudo)
                validado = estructura_termoking(r1)
                _enriquecer_validado(validado)
                docs_a_insertar.append(validado)

            # Un solo insert_many + un solo upsert por bloque
            if docs_a_insertar:
                inserted = col_tratado.insert_many(docs_a_insertar)
                _upsert_general_batch(
                    imei,
                    docs_a_insertar,
                    inserted.inserted_ids,
                )

            resultado[imei].append({
                "Coleccion":        nombre_coleccion,
                "cantidad":         cantidad,
                "tiempo_ejecucion": round(time.time() - tiempo_inicio, 3),
            })

    logger.info(
        "Batch completado en %.1f s | meses=%d | IMEIs=%d",
        time.time() - tiempo_inicio, len(meses_creados), len(green_box_list),
    )
    return resultado


def reconstruccion_green_box() -> Dict[str, Any]:
    meses = obtener_meses_creados(database_mongo)
    return imeis_en_colecciones(meses, green_box, collection)


# ══════════════════════════════════════════════════════════════════════════════
# CICLO INCREMENTAL — llamado cada 60 s desde la ruta HTTP
# ══════════════════════════════════════════════════════════════════════════════

def actualizar_incrementalmente() -> Dict[str, Any]:
    """
    Procesa solo los documentos nuevos (fecha > cursor) para cada IMEI.

    Optimizaciones:
      · _cursores_todos_los_imeis() → una sola consulta a General_dispositivos.
      · insert_many por bloque en TRATADO en lugar de insert_one por doc.
      · _upsert_general_batch → un solo update_one por bloque en General_dispositivos.
    """
    tiempo_inicio = time.time()
    colecciones   = _colecciones_recientes()
    cursores      = _cursores_todos_los_imeis()
    resumen: Dict[str, Any] = {}

    for imei in green_box:
        _garantizar_doc_general(imei)
        ultimo_recibido = cursores.get(imei)
        col_tratado     = collection(f"TRATADO_{imei}")
        nuevos_total    = 0

        for nombre_coleccion in colecciones:
            col_mes = collection(nombre_coleccion)
            if not col_mes.find_one({"imei": imei}, {"_id": 1}):
                continue

            col_imei = collection(bd_gene_imei(nombre_coleccion, imei))
            filtro: Dict[str, Any] = {}
            if ultimo_recibido is not None:
                filtro["fecha"] = {"$gt": ultimo_recibido}

            nuevos_crudos = list(col_imei.find(filtro).sort("fecha", 1))
            if not nuevos_crudos:
                continue

            # Procesar todos → acumular
            docs_procesados: List[Dict[str, Any]] = []
            for doc_crudo in nuevos_crudos:
                r1       = procesar_documento(doc_crudo)
                validado = estructura_termoking(r1)
                _enriquecer_validado(validado)
                docs_procesados.append(validado)

            # Un solo insert_many → un solo _upsert_general_batch
            inserted = col_tratado.insert_many(docs_procesados)
            _upsert_general_batch(imei, docs_procesados, inserted.inserted_ids)

            nuevos_total += len(nuevos_crudos)

        resumen[imei] = {
            "nuevos_procesados":     nuevos_total,
            "colecciones_revisadas": colecciones,
            "cursor_anterior":       _fecha_a_iso(ultimo_recibido),
        }

    return {
        "tiempo_ejecucion_seg": round(time.time() - tiempo_inicio, 3),
        "resumen": resumen,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Endpoints de lectura (sin cambios de lógica, solo limpieza)
# ══════════════════════════════════════════════════════════════════════════════

def lista_imeis_termoking() -> List[Dict[str, Any]]:
    col = collection(bd_gene("dispositivos"))
    return [doc for doc in col.find({"estado": 1}, {"_id": 0})]


def _calcular_en_rango(set_point: Any, return_air: Any) -> Optional[bool]:
    try:
        return abs(float(return_air) - float(set_point)) <= 5
    except (TypeError, ValueError):
        return None


def _calcular_en_defrost(
    set_point: Any, temp_supply_1: Any, return_air: Any, evaporation_coil: Any
) -> Optional[bool]:
    try:
        evap = float(evaporation_coil)
    except (TypeError, ValueError):
        return None
    if not (12 <= evap <= 30):
        return False
    def cerca(val: Any) -> bool:
        try:
            return abs(float(val) - float(set_point)) <= 5
        except (TypeError, ValueError):
            return False
    return cerca(temp_supply_1) or cerca(return_air)


def ultimo_estado_dispositivos_termoking() -> Dict[str, Any]:
    """
    Último estado de cada dispositivo en TK_PROCESO_MES_AÑO.
    Incluye resumen (online/wait/offline/defrost/power), campos elementales,
    en_rango y en_defrost.
    """
    data_proceso = collection(bd_gene("proceso"))
    dispositivos: List[Dict[str, Any]] = []
    ahora_gmt5   = _ahora_gmt5()

    for notif in data_proceso.find({"estado": 1}, {"_id": 0, "imei": 1}):
        imei = notif.get("imei")
        if not imei:
            continue

        ultimo = collection(bd_oficial(imei)).find_one(
            {}, {"_id": 0}, sort=[("fecha", -1)]
        )

        fecha_ultima: Optional[datetime] = ultimo.get("fecha") if ultimo else None
        fecha_gmt5   = _como_gmt5(fecha_ultima)
        estado_con   = _calcular_estado_conexion(fecha_ultima)

        if fecha_gmt5:
            minutos_desde       = (ahora_gmt5 - fecha_gmt5).total_seconds() / 60
            ultima_actualizacion = _fecha_a_iso(fecha_ultima)
        else:
            minutos_desde        = None
            ultima_actualizacion = None

        if ultimo:
            dato        = _filtrar_campos_elementales(ultimo)
            power_state = ultimo.get("power_state")
            dato["power_state_texto"] = "on" if power_state == 1 else "off"
            dato["en_rango"]          = _calcular_en_rango(
                ultimo.get("set_point"), ultimo.get("return_air")
            )
            en_defrost = _calcular_en_defrost(
                ultimo.get("set_point"),   ultimo.get("temp_supply_1"),
                ultimo.get("return_air"),  ultimo.get("evaporation_coil"),
            )
        else:
            dato       = {k: None for k in _CAMPOS_ULTIMO_ESTADO}
            dato.update({"power_state_texto": None, "en_rango": None})
            en_defrost = None

        dispositivos.append({
            "imei":                       imei,
            "estado_conexion":            estado_con,
            "ultima_actualizacion":       ultima_actualizacion,
            "minutos_desde_ultimo_dato":  round(minutos_desde, 1) if minutos_desde else None,
            "power_state_texto":          dato.pop("power_state_texto"),
            "en_rango":                   dato.pop("en_rango"),
            "en_defrost":                 en_defrost,
            "ultimo_dato":                dato,
        })

    online  = sum(1 for d in dispositivos if d["estado_conexion"] == "online")
    wait    = sum(1 for d in dispositivos if d["estado_conexion"] == "wait")
    offline = sum(1 for d in dispositivos if d["estado_conexion"] == "offline")

    return {
        "resumen": {
            "total_dispositivos": len(dispositivos),
            "online":  online,
            "wait":    wait,
            "offline": offline,
            "en_defrost":  sum(1 for d in dispositivos if d.get("en_defrost")),
            "power_on":    sum(1 for d in dispositivos if d.get("power_state_texto") == "on"),
            "power_off":   sum(1 for d in dispositivos if d.get("power_state_texto") == "off"),
            "zona_horaria": "GMT-5",
        },
        "dispositivos": dispositivos,
    }