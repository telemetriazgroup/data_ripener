from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from server.routes.termoking import router as TermoKingRouter


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

app.include_router(TermoKingRouter, tags=["TermoKing"]        , prefix="/TermoKing")


@app.get("/", tags=["Root"])
async def read_root():
    return {"message": "Welcome to  app ztrack by 0.1!"}


