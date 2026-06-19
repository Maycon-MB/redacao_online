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

async def test_token(session):
    try:
        async with session.get(base_url, headers=headers) as response:
            return response.status == 200
    except Exception:
        return False

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

def extract_disciplina_avaliacao(tema):
    if not tema: return 'Não disponível', 'Não disponível'
    try:
        codigo = tema[:6]
        return codigo[:3], codigo[3:]
    except Exception:
        return 'Não disponível', 'Não disponível'

def convert_nota(nota):
    try:
        nota_float = float(nota)
        if nota_float > 10:
            nota_float = nota_float / 100
        return f"{nota_float:.1f}".replace('.', ',')
    except (ValueError, TypeError):
        return nota

alunos_cache = {}

def preload_alunos(conn):
    print("Carregando alunos do banco de dados para a memória...")
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT matricula, unidade, turma FROM {TABELA_ALUNOS}")
        for row in cursor.fetchall():
            if row[0]:
                alunos_cache[str(row[0])] = (row[1].strip().zfill(2) if row[1] else '', row[2])
        cursor.close()
        print(f"Alunos carregados: {len(alunos_cache)}")
    except Exception as e:
        print("Erro ao carregar alunos do banco:", e)

def get_aluno_info(matricula):
    return alunos_cache.get(str(matricula), (None, None))

def extract_gra_ser(turma):
    try:
        turma_str = str(turma)
        gra = turma_str[0] if len(turma_str) >= 1 else None
        ser = turma_str[2] if len(turma_str) >= 3 else None
        return gra, ser
    except Exception:
        return None, None

async def process_essay(session, essay, results_list, three_months_ago):
    if not essay.get('is_corrected'):
        return

    created_at_str = essay.get('created_at')
    if not created_at_str: return
    try:
        created_at = datetime.strptime(created_at_str[:10], '%Y-%m-%d')
        if created_at < three_months_ago:
            return
    except Exception:
        pass

    student = essay.get('student') or {}
    matricula = student.get('external_id')
    if not matricula: return

    corrections = essay.get('corrections', [])
    if not corrections: return

    nota_bruta = corrections[0].get('grade') or corrections[0].get('nota')
    if nota_bruta is None: return

    nota = convert_nota(nota_bruta)

    unidade, turma = get_aluno_info(matricula)
    if not unidade or not turma: return

    gra, ser = extract_gra_ser(turma)
    
    if gra == '3':
        theme_text_id = essay.get('theme_text_id')
        tema = await get_theme_name(session, theme_text_id) if theme_text_id else None
        disciplina, avaliacao = extract_disciplina_avaliacao(tema)

        results_list.append({
            'Data': created_at_str[:10],
            'Unidade': unidade,
            'Turma': turma,
            'Matrícula': matricula,
            'Nome Aluno': student.get('name', ''),
            'Tema': tema,
            'Disciplina': disciplina,
            'Avaliação': avaliacao,
            'Nota': nota
        })

async def main():
    three_months_ago = datetime.now() - timedelta(days=90)
    results = []
    
    db_conn = psycopg2.connect(**db_config)
    preload_alunos(db_conn)
    db_conn.close()
    
    async with aiohttp.ClientSession() as session:
        if not await test_token(session):
            print("Token inválido.")
            return

        page = 1
        print("Buscando redações da API...")
        while True:
            sys.stdout.write(f"\rProcessando Página {page}...")
            sys.stdout.flush()
            url = f"{base_url}&page={page}"
            async with session.get(url, headers=headers) as response:
                if response.status != 200: break
                data = await response.json()
                if not data.get('data'): break

                # Processa sequencialmente
                for essay in data['data']:
                    await process_essay(session, essay, results, three_months_ago)
                    if len(results) >= 20:
                        break

                if len(results) >= 20:
                    print("\nEncontrou amostra suficiente (20 redações).")
                    break

                pagination = data.get('pagination', {})
                if page >= pagination.get('last_page', 1): break
                
                first_essay_date_str = data['data'][0].get('created_at')
                if first_essay_date_str:
                    try:
                        dt = datetime.strptime(first_essay_date_str[:10], '%Y-%m-%d')
                        if dt < three_months_ago:
                            print("\nAlcançou redações mais antigas que 3 meses. Parando busca.")
                            break
                    except: pass
                
                page += 1

    print(f"\nTotal de redações de 3ª série encontradas nos últimos 3 meses: {len(results)}")
    
    if results:
        unidades_presentes = set(r['Unidade'] for r in results)
        unidade_escolhida = None
        
        for u in ['14', '01', '03', '04', '05', '06', '09']:
            if u in unidades_presentes:
                unidade_escolhida = u
                break
        
        if not unidade_escolhida and unidades_presentes:
            unidade_escolhida = list(unidades_presentes)[0]
            
        print(f"Filtrando para a unidade {unidade_escolhida}")
        
        resultados_finais = [r for r in results if r['Unidade'] == unidade_escolhida]
        
        arquivo_csv = 'exportacao_thiago_3serie.csv'
        with open(arquivo_csv, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=['Data', 'Unidade', 'Turma', 'Matrícula', 'Nome Aluno', 'Tema', 'Disciplina', 'Avaliação', 'Nota'])
            writer.writeheader()
            writer.writerows(resultados_finais)
            
        print(f"Arquivo gerado: {arquivo_csv} com {len(resultados_finais)} linhas.")

if __name__ == "__main__":
    asyncio.run(main())
