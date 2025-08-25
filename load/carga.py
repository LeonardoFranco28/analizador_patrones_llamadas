
import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from logging import INFO

from dotenv import load_dotenv
from graphiti_core import Graphiti
from graphiti_core.nodes import EpisodeType

# ------------------- CONFIG -------------------
logging.basicConfig(
    level=INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
load_dotenv()

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "password")
DATA_PATH = "data/interacciones_clientes_sort.json"

# ------------------- FUNCIONES AUXILIARES -------------------
async def add_episode(graphiti, name, data, summary=None, group_id="default"):
    """Crea un episodio en Graphiti con group_id y todo dentro de episode_body JSON."""
    episode_content = {
        "data": data,
        "group_id": group_id
    }
    await graphiti.add_episode(
        name=name,
        episode_body=json.dumps(episode_content),
        source=EpisodeType.json,
        source_description=summary or "",
        reference_time=datetime.now(timezone.utc)
    )

# ------------------- CREACIÓN DE NODOS -------------------
async def crear_clientes(graphiti, clientes):
    for cliente in clientes:
        await add_episode(
            graphiti,
            name=cliente["id"],
            data=cliente,
            summary=f"{cliente['nombre']} con deuda inicial {cliente['monto_deuda_inicial']} tipo {cliente['tipo_deuda']}",
            group_id="clientes"
        )

async def crear_interacciones(graphiti, interacciones):
    interacciones = sorted(interacciones, key=lambda x: x["timestamp"])
    prev_id = None

    for inter in interacciones:
        inter_copy = inter.copy()
        inter_copy["links"] = [{"target_name": inter["cliente_id"], "type": "REALIZA_INTERACCION"}]
        if "agente_id" in inter:
            inter_copy["links"].append({"target_name": inter["agente_id"], "type": "GESTIONADA_POR"})

        await add_episode(
            graphiti,
            name=inter["id"],
            data=inter_copy,
            summary=f"Interacción {inter['tipo']} resultado: {inter.get('resultado', 'n/a')}",
            group_id="interacciones"
        )

        # Relación temporal
        if prev_id:
            temporal_content = {
                "from": prev_id,
                "to": inter["id"],
                "type": "SIGUIENTE_INTERACCION"
            }
            await add_episode(
                graphiti,
                name=f"rel_{prev_id}_{inter['id']}",
                data=temporal_content,
                summary=f"Interacción temporal entre {prev_id} y {inter['id']}",
                group_id="temporales"
            )
        prev_id = inter["id"]

async def crear_promesas_pagos(graphiti, interacciones):
    for inter in interacciones:
        if inter.get("resultado") == "promesa_pago":
            promesa_content = {
                "monto_prometido": inter["monto_prometido"],
                "fecha_promesa": inter["fecha_promesa"],
                "interaccion_id": inter["id"]
            }
            await add_episode(
                graphiti,
                name=f"promesa_{inter['id']}",
                data=promesa_content,
                summary=f"Promesa de pago {inter['monto_prometido']} para {inter['fecha_promesa']}",
                group_id="promesas_pagos"
            )
        if inter["tipo"] == "pago_recibido":
            pago_content = {
                "monto": inter.get("monto"),
                "metodo_pago": inter.get("metodo_pago"),
                "pago_completo": inter.get("pago_completo"),
                "interaccion_id": inter["id"]
            }
            await add_episode(
                graphiti,
                name=f"pago_{inter['id']}",
                data=pago_content,
                summary=f"Pago recibido {inter.get('monto')} por método {inter.get('metodo_pago')}",
                group_id="promesas_pagos"
            )

# ------------------- MAIN -------------------
async def main():
    graphiti = Graphiti(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    try:
        await graphiti.build_indices_and_constraints()

        # Cargar datos
        with open(DATA_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)

        await crear_clientes(graphiti, data.get("clientes", []))
        await crear_interacciones(graphiti, data.get("interacciones", []))
        await crear_promesas_pagos(graphiti, data.get("interacciones", []))

        logger.info("✅ Todos los episodios y relaciones fueron creados en Neo4j cloud con Graphiti.")

    finally:
        await graphiti.close()
        logger.info("Conexión cerrada.")

if __name__ == "__main__":
    asyncio.run(main())
