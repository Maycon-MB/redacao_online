import asyncio
import platform
import aiohttp

import importar_notas_sae as sae

if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

NOME_ALUNO = "JHENIFFER DANTAS MONTECHIARI SILVA"
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

        page, data = await fetch_page(session, 1, asyncio.Semaphore(1))
        if not data or not data.get('data'):
            print("[AVISO] Nenhum dado retornado na página 1.", flush=True)
            return

        last_page = data.get('pagination', {}).get('last_page', 1)
        print(f"Total de páginas: {last_page}. Buscando '{NOME_ALUNO}' com {CONCORRENCIA} requisições simultâneas...", flush=True)

        encontrado = False

        async def processa_pagina(dados_pagina, num_pagina):
            nonlocal encontrado
            for essay in dados_pagina.get('data', []):
                student = essay.get('student') or {}
                nome = (student.get('name') or '').strip().upper()
                if nome == NOME_ALUNO.upper():
                    print(f"Redação encontrada para {nome} (página {num_pagina}). Processando...", flush=True)
                    await sae.process_essay(session, essay, 1)
                    encontrado = True

        await processa_pagina(data, 1)

        if last_page > 1 and not encontrado:
            semaforo = asyncio.Semaphore(CONCORRENCIA)
            tarefas = [asyncio.create_task(fetch_page(session, p, semaforo)) for p in range(2, last_page + 1)]

            for tarefa in asyncio.as_completed(tarefas):
                num_pagina, dados_pagina = await tarefa
                if encontrado:
                    continue
                if dados_pagina:
                    await processa_pagina(dados_pagina, num_pagina)

            for t in tarefas:
                if not t.done():
                    t.cancel()

        if not encontrado:
            print(f"[AVISO] Nenhuma redação encontrada para '{NOME_ALUNO}'.", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
