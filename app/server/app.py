from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse

from server.routes.termoking import router as TermoKingRouter

# Carpeta static (gráfica histórico, etc.)
STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

app = FastAPI(
    title="ZTRACK API FullRest x",
    summary="Modulos de datos bidireccional",
    version="0.0.1",
)

app.add_middleware(
    CORSMiddleware,
    #allow_origins=origins,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
#añadir el conjunto de rutas de notificaciones

app.include_router(TermoKingRouter, tags=["TermoKing"], prefix="/TermoKing")

if STATIC_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", tags=["Root"])
async def read_root():
    return {"message": "Welcome to  app ztrack by 0.1!"}


@app.get("/datos-historico", tags=["Frontend"])
async def datos_historico_page():
    """Redirige a la página de gráfica de Datos Histórico (responsive + zoom)."""
    return RedirectResponse(url="/static/historico.html")


