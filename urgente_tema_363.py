import aiohttp
import asyncio
import json
import psycopg2
from datetime import datetime, timedelta
import csv
import sys
import platform

if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

ANO_SUFIXO = str(datetime.now().year)[2:]
TABELA_ALUNOS = f"alunos_{ANO_SUFIXO}_geral"

base_url = "https://app.redacaonline.com.br/api/essays?limit=100"
theme_text_url = "https://app.redacaonline.com.br/api/themes/texts/{}"
theme_url = "https://app.redacaonline.com.br/api/themes/{}"
headers = {
    'Accept': 'application/json',
    'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxMzIiLCJqdGkiOiI1MTNhMDdhMDdmMmY4OTI5MWUyMzQ5MTVmMDY5ODIyYWZhNjkxZDAwZGQ2YzZiNjVkM2IyMmY1N2QwNzNmZTEzN2NhMjMxZDc1MmQwMTg3YSIsImlhdCI6MTczNzA1Mjk4Ny40MTIwNDgsIm5iZiI6MTczNzA1Mjk4Ny40MTIwNTIsImV4cCI6MTg5NDgxOTM4Ny40MDM0MDcsInN1YiI6IjMxOSIsInNjb3BlcyI6WyIqIl19.UP9ThFiYOvtjr2JOQuu0nSWzroOz6WmWCnfNK4yhU-j_02pcDT1hukXrgV_FnsCqy37kGgAZVXT-uk2TLr8RWxMww4JXtqLDCdH6uSmQvO2uK1HAutxkoJNFDurIjTcfX2RmbQ4_TD0QUc2pyyHZ9lB9C4nOlOvRLkJIOoyGAa3gUlTHgX8GN5x2ZS_2bxrYPy4ioFDMNTwCi5UG_fXbApmVuXvhc35muacC1TVmuExvGQLpliDsZqNNJgZSVZsqdhFQS4ZrqYMz-pkd0_W0AuCSYxjMKyEAjcd6aEQfkLiGfVzI0EJ19e1Yc-vBG1SxC4Iv7FfhL6H9hahMZ-sQK07Ilbt9vD7k-I-93vvHBmlYIT4A4wC9QqH-z-I0hg64JjYsPLBezqmZOUEmcsan30OFZSlSp2iRkgd4iEnvia41KdkllERkoPPu5o4fq8hQ6dfVvGsqSQ0ZdMRWy0ukWTdAvgExFCh3i7Q6hadvdePrSzNUIrESp4tKpTg5qtVtbaseZNz7IkzpuYbk8tJUolNIterF20nc0leBbk2Qx0cjrrw6Cyzu7AElUxG-vALI5FRmR9jsWT_knrSQaWZaRo1tHrCF0yG19odOjS8Scd-97JTzN-gStji6XyDl5fTQV0uljHS3CqyTFDcWl9ZRk3wI5ITjFgYaiVEIkZ7U5p4'
}

db_config = {
    "dbname": "BOLETOS",
    "user": "postgres",
    "password": "teste",
    "host": "192.168.1.163",
    "port": "5432"
}

theme_cache = {}
alunos_cache = {}

async def test_token(session):
    try:
        async with session.get(base_url, headers=headers) as response:
            return response.status == 200
    except Exception:
        return False

def preload_alunos(conn):
    print("Carregando alunos do banco local para cruzar matrículas...")
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT matricula, unidade, turma FROM {TABELA_ALUNOS}")
        for row in cursor.fetchall():
            if row[0]:
                alunos_cache[str(row[0])] = (row[1].strip().zfill(2) if row[1] else '', row[2])
        cursor.close()
    except Exception as e:
        print("Erro ao carregar alunos do banco:", e)

def get_aluno_info(matricula):
    return alunos_cache.get(str(matricula), (None, None))

async def get_theme_name(session, theme_text_id):
    if theme_text_id in theme_cache:
        return theme_cache[theme_text_id]
    try:
        url = theme_text_url.format(theme_text_id)
        async with session.get(url, headers=headers) as response:
            if response.status != 200: return None
            text_data = await response.json()
            theme_id = text_data.get('theme_id')
            if not theme_id: return None

        url = theme_url.format(theme_id)
        async with session.get(url, headers=headers) as response:
            if response.status != 200: return None
            theme_data = await response.json()
            theme_title = theme_data.get('title', None)
            theme_cache[theme_text_id] = theme_title
            return theme_title
    except Exception:
        return None

def convert_nota(nota):
    try:
        nota_float = float(nota)
        if nota_float > 10:
            nota_float = nota_float / 100
        return f"{nota_float:.1f}".replace('.', ',')
    except (ValueError, TypeError):
        return nota

async def process_essay(session, essay, results_list, theme_title):
    if not essay.get('is_corrected'):
        return

    student = essay.get('student') or {}
    matricula = student.get('external_id')
    if not matricula: return

    corrections = essay.get('corrections', [])
    if not corrections: return

    nota_bruta = corrections[0].get('grade') or corrections[0].get('nota')
    if nota_bruta is None: return

    nota = convert_nota(nota_bruta)

    unidade, turma = get_aluno_info(matricula)
    
    codigo = theme_title[:6] if len(theme_title) >= 6 else theme_title
    disciplina, avaliacao = codigo[:3], codigo[3:]
    
    created_at_str = essay.get('created_at', '')[:10]

    results_list.append({
        'Data': created_at_str,
        'Unidade': unidade or 'Desconhecida',
        'Turma': turma or 'Desconhecida',
        'Matrícula': matricula,
        'Nome Aluno': student.get('name', ''),
        'Tema': theme_title,
        'Disciplina': disciplina,
        'Avaliação': avaliacao,
        'Nota': nota
    })

async def main():
    target_theme = "363AV2"
    print(f"\nIniciando busca URGENTE OTIMIZADA para o tema: {target_theme}")
    
    try:
        db_conn = psycopg2.connect(**db_config)
        preload_alunos(db_conn)
        db_conn.close()
    except Exception as e:
        print(f"Aviso: Não foi possível conectar ao banco de dados: {e}")
    
    results = []
    
    async with aiohttp.ClientSession() as session:
        if not await test_token(session):
            print("Token da API inválido.")
            return

        # 1. Buscar IDs dos temas que contém o termo
        print("Buscando temas correspondentes...")
        theme_search_url = f"https://app.redacaonline.com.br/api/themes?search={target_theme}"
        async with session.get(theme_search_url, headers=headers) as response:
            if response.status != 200:
                print("Erro ao buscar temas.")
                return
            theme_data = await response.json()
            themes = theme_data.get('data', [])
        
        # Filtra para ter certeza que começa com target_theme
        valid_themes = [t for t in themes if t.get('title', '').upper().startswith(target_theme.upper())]
        print(f"Encontrados {len(valid_themes)} temas que começam com '{target_theme}'.")

        # 2. Buscar as redações para cada tema encontrado
        for theme in valid_themes:
            theme_id = theme['id']
            theme_title = theme['title']
            print(f"Buscando redações para o tema ID {theme_id}...")
            
            page = 1
            while True:
                url = f"{base_url}&theme_id={theme_id}&page={page}"
                async with session.get(url, headers=headers) as response:
                    if response.status != 200: break
                    data = await response.json()
                    if not data.get('data'): break

                    for essay in data['data']:
                        await process_essay(session, essay, results, theme_title)

                    pagination = data.get('pagination', {})
                    if page >= pagination.get('last_page', 1): break
                    page += 1

    print(f"\n\nTotal de notas encontradas para o tema {target_theme}: {len(results)}")
    
    if results:
        arquivo_csv = f'notas_tema_{target_theme}.csv'
        with open(arquivo_csv, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=['Data', 'Unidade', 'Turma', 'Matrícula', 'Nome Aluno', 'Tema', 'Disciplina', 'Avaliação', 'Nota'])
            writer.writeheader()
            writer.writerows(results)
            
        print(f"Arquivo gerado com SUCESSO: {arquivo_csv}")

if __name__ == "__main__":
    asyncio.run(main())
