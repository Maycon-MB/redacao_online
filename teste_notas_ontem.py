import asyncio
import platform
from datetime import datetime, timedelta
import aiohttp

import importar_notas_sae as sae

if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

DATA_ALVO = (datetime.now() - timedelta(days=1)).date()


def data_essay(essay):
    bruto = essay.get('updated_at') or essay.get('created_at')
    dt_local = sae.parse_data_utc_para_local(bruto)
    return dt_local.date() if dt_local else None


CONCORRENCIA = 8


async def fetch_page(session, page, semaforo):
    async with semaforo:
        url = f"{sae.base_url}&page={page}"
        async with session.get(url, headers=sae.headers) as response:
            if response.status != 200:
                print(f"[ERRO] Falha na página {page}: {response.status} - {await response.text()}", flush=True)
                return page, None
            return page, await response.json()


async def main():
    async with aiohttp.ClientSession() as session:
        if not await sae.test_token(session):
            print("[ERRO] Token inválido.", flush=True)
            return

        print(f"Buscando todas as notas corrigidas em {DATA_ALVO.strftime('%d/%m/%Y')}...", flush=True)

        page1, data1 = await fetch_page(session, 1, asyncio.Semaphore(1))
        if not data1 or not data1.get('data'):
            print("[AVISO] Nenhum dado retornado na página 1.", flush=True)
            return

        last_page = data1.get('pagination', {}).get('last_page', 1)
        print(f"Total de páginas: {last_page}. Varrendo TODAS (sem corte por ordem) com {CONCORRENCIA} requisições simultâneas...", flush=True)

        total_processadas = 0

        async def processa_pagina(dados_pagina, num_pagina):
            nonlocal total_processadas
            print(f"Verificando página {num_pagina}...", flush=True)
            for essay in dados_pagina.get('data', []):
                if data_essay(essay) != DATA_ALVO:
                    continue
                total_processadas += 1
                await sae.process_essay(session, essay, total_processadas)

        await processa_pagina(data1, 1)

        if last_page > 1:
            semaforo = asyncio.Semaphore(CONCORRENCIA)
            tarefas = [asyncio.create_task(fetch_page(session, p, semaforo)) for p in range(2, last_page + 1)]
            for tarefa in asyncio.as_completed(tarefas):
                num_pagina, dados_pagina = await tarefa
                if dados_pagina:
                    await processa_pagina(dados_pagina, num_pagina)

        print(f"Total de notas processadas de {DATA_ALVO.strftime('%d/%m/%Y')}: {total_processadas}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
