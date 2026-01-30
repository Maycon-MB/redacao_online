import aiohttp
import asyncio
import json
import psycopg2
from datetime import datetime

ANO_SUFIXO = str(datetime.now().year)[2:]
TABELA_ALUNOS = f"alunos_{ANO_SUFIXO}_geral"

base_url = "https://app.redacaonline.com.br/api/essays?limit=100"
theme_text_url = "https://app.redacaonline.com.br/api/themes/texts/{}"
theme_url = "https://app.redacaonline.com.br/api/themes/{}"
headers = {
    'Accept': 'application/json',
    'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxMzIiLCJqdGkiOiI1MTNhMDdhMDdmMmY4OTI5MWUyMzQ5MTVmMDY5ODIyYWZhNjkxZDAwZGQ2YzZiNjVkM2IyMmY1N2QwNzNmZTEzN2NhMjMxZDc1MmQwMTg3YSIsImlhdCI6MTczNzA1Mjk4Ny40MTIwNDgsIm5iZiI6MTczNzA1Mjk4Ny40MTIwNTIsImV4cCI6MTg5NDgxOTM4Ny40MDM0MDcsInN1YiI6IjMxOSIsInNjb3BlcyI6WyIqIl19.UP9ThFiYOvtjr2JOQuu0nSWzroOz6WmWCnfNK4yhU-j_02pcDT1hukXrgV_FnsCqy37kGgAZVXT-uk2TLr8RWxMww4JXtqLDCdH6uSmQvO2uK1HAutxkoJNFDurIjTcfX2RmbQ4_TD0QUc2pyyHZ9lB9C4nOlOvRLkJIOoyGAa3gUlTHgX8GN5x2ZS_2bxrYPy4ioFDMNTwCi5UG_fXbApmVuXvhc35muacC1TVmuExvGQLpliDsZqNNJgZSVZsqdhFQS4ZrqYMz-pkd0_W0AuCSYxjMKyEAjcd6aEQfkLiGfVzI0EJ19e1Yc-vBG1SxC4Iv7FfhL6H9hahMZ-sQK07Ilbt9vD7k-I-93vvHBmlYIT4A4wC9QqH-z-I0hg64JjYsPLBezqmZOUEmcsan30OFZSlSp2iRkgd4iEnvia41KdkllERkoPPu5o4fq8hQ6dfVvGsqSQ0ZdMRWy0ukWTdAvgExFCh3i7Q6hadvdePrSzNUIrESp4tKpTg5qtVtbaseZNz7IkzpuYbk8tJUolNIterF20nc0leBbk2Qx0cjrrw6Cyzu7AElUxG-vALI5FRmR9jsWT_knrSQaWZaRo1tHrCF0yG19odOjS8Scd-97JTzN-gStji6XyDl5fTQV0uljHS3CqyTFDcWl9ZRk3wI5ITjFgYaiVEIkZ7U5p4'
}

# Configuração do banco de dados PostgreSQL (banco principal)
db_config = {
    "dbname": "BOLETOS",
    "user": "postgres",
    "password": "teste",
    "host": "192.168.1.163",
    "port": "5432"
}

# Mapeamento de servidores
servidores = {
    "01": {"nome": "Bento Ribeiro", "ip": "10.3.1.2"},
    "02": {"nome": "Madureira", "ip": "10.4.1.2"},
    "03": {"nome": "Santa Cruz", "ip": "10.6.1.2"},
    "04": {"nome": "Cascadura", "ip": "10.5.1.2"},
    "05": {"nome": "Taquara", "ip": "10.14.1.2"},
    "06": {"nome": "Nilópolis", "ip": "10.17.1.2"},
    "09": {"nome": "Seropédica", "ip": "10.13.1.2"},
    "10": {"nome": "Barra", "ip": "10.8.1.2"},
    "11": {"nome": "Campo Grande", "ip": "10.7.1.2"},
    "14": {"nome": "Maricá", "ip": "10.9.1.2"},
    "15": {"nome": "Ilha do Governador", "ip": "10.10.1.2"},
    "16": {"nome": "Freguesia", "ip": "10.11.1.2"},
    "17": {"nome": "Recreio", "ip": "10.12.1.2"}
}

# Cache para armazenar temas já buscados
theme_cache = {}

async def test_token(session):
    """Testa se o token é válido."""
    print("Verificando validade do token...", flush=True)
    try:
        async with session.get(base_url, headers=headers) as response:
            if response.status == 200:
                print("Token válido!", flush=True)
                return True
            else:
                print(f"[ERRO] Token inválido: {response.status} - {await response.text()}", flush=True)
                return False
    except Exception as e:
        print(f"[ERRO] Falha ao verificar token: {e}", flush=True)
        return False

async def get_theme_name(session, theme_text_id):
    """Obtém o nome do tema a partir do theme_text_id, usando cache."""
    if theme_text_id in theme_cache:
        print(f"Usando tema em cache para theme_text_id {theme_text_id}: {theme_cache[theme_text_id]}", flush=True)
        return theme_cache[theme_text_id]

    print(f"Buscando tema para theme_text_id {theme_text_id}...", flush=True)
    try:
        url = theme_text_url.format(theme_text_id)
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                print(f"[ERRO] Falha ao buscar texto motivador {theme_text_id}: {response.status} - {await response.text()}", flush=True)
                return None
            text_data = await response.json()
            theme_id = text_data.get('theme_id')
            if not theme_id:
                print(f"[ERRO] theme_id não encontrado no texto motivador {theme_text_id}: {json.dumps(text_data, indent=2)}", flush=True)
                return None

        url = theme_url.format(theme_id)
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                print(f"[ERRO] Falha ao buscar tema {theme_id}: {response.status} - {await response.text()}", flush=True)
                return None
            theme_data = await response.json()
            theme_title = theme_data.get('title', None)
            theme_cache[theme_text_id] = theme_title
            print(f"Tema encontrado: {theme_title}", flush=True)
            return theme_title
    except Exception as e:
        print(f"[ERRO] Erro ao buscar tema para theme_text_id {theme_text_id}: {e}", flush=True)
        return None

def extract_disciplina_avaliacao(tema):
    """Extrai disciplina (código numérico) e avaliação do título do tema."""
    if not tema:
        return 'Disciplina não disponível', 'Avaliação não disponível'
    
    try:
        codigo = tema[:6]
        disciplina_codigo = codigo[:3]
        avaliacao = codigo[3:]
        print(f"Disciplina: {disciplina_codigo}, Avaliação: {avaliacao}", flush=True)
        return disciplina_codigo, avaliacao
    except Exception as e:
        print(f"[ERRO] Erro ao extrair disciplina/avaliação de '{tema}': {e}", flush=True)
        return 'Disciplina não disponível', 'Avaliação não disponível'

def convert_nota(nota):
    """Converte nota para formato com vírgula."""
    try:
        nota_float = float(nota)
        if nota_float > 10:
            nota_float = nota_float / 100
        return f"{nota_float:.1f}".replace('.', ',')
    except (ValueError, TypeError):
        return nota

def get_aluno_info(matricula):
    f"""Obtém unidade e turma do aluno a partir da tabela {TABELA_ALUNOS}."""
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT unidade, turma 
            FROM {TABELA_ALUNOS}
            WHERE matricula = %s
        """, (matricula,))
        result = cursor.fetchone()
        conn.close()
        if result:
            unidade, turma = result
            return unidade.strip().zfill(2), turma
        else:
            print(f"[ERRO] Aluno com matrícula {matricula} não encontrado na tabela {TABELA_ALUNOS}.", flush=True)
            return None, None
    except Exception as e:
        print(f"[ERRO] Erro ao buscar aluno {matricula}: {e}", flush=True)
        return None, None

def extract_gra_ser(turma):
    """Extrai gra (1º dígito) e ser (3º dígito) da turma."""
    try:
        turma_str = str(turma)
        gra = turma_str[0] if len(turma_str) >= 1 else None
        ser = turma_str[2] if len(turma_str) >= 3 else None
        return gra, ser
    except Exception as e:
        print(f"[ERRO] Erro ao extrair gra/ser da turma {turma}: {e}", flush=True)
        return None, None

async def grava_nota_db(matricula, disciplina, avaliacao, nota, gra, ser, unidade):
    """Grava a nota no banco da unidade correspondente via dblink."""
    if not all([matricula, disciplina, avaliacao, nota, gra, ser, unidade]):
        print(f"[ERRO] Dados incompletos para gravação: matrícula={matricula}, disciplina={disciplina}, avaliacao={avaliacao}, nota={nota}, gra={gra}, ser={ser}, unidade={unidade}", flush=True)
        return False

    servidor = servidores.get(unidade)
    if not servidor:
        print(f"[ERRO] Unidade {unidade} não encontrada nos servidores.", flush=True)
        return False

    ip = servidor['ip']
    ano = datetime.now().strftime('%y')  # Ex.: "25" para 2025
    dbname = f"sae{unidade}{ano}"

    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        query = """
            SELECT dados1.retorno 
            FROM dblink('dbname=%s hostaddr=%s user=postgres password=teste port=5432', 
                        'select grava_nota(%s, %s, (select avaliacao_id from matriz_avaliacao where gra=%s and ser=%s and sig=%s), %s)') 
            dados1(retorno boolean)
        """
        cursor.execute(query, (dbname, ip, matricula, disciplina, gra, ser, avaliacao, nota))
        result = cursor.fetchone()
        conn.commit()
        conn.close()
        if result and result[0]:
            print(f"Nota gravada com sucesso para matrícula {matricula}, disciplina {disciplina}, avaliação {avaliacao}, nota {nota}", flush=True)
            return True
        else:
            print(f"[ERRO] Falha ao gravar nota para matrícula {matricula}, disciplina {disciplina}, avaliação {-avaliacao}", flush=True)
            return False
    except Exception as e:
        print(f"[ERRO] Erro ao gravar nota via dblink para {matricula}: {e}", flush=True)
        return False

async def process_essay(session, essay, total_corrected):
    """Processa uma redação individualmente."""
    if not (essay.get('is_corrected') and essay.get('corrections')):
        return None

    print(f"Processando redação {total_corrected}...", flush=True)
    student = essay.get('student', {})
    matricula = student.get('external_id', 'Matrícula não disponível')
    nota = essay['corrections'][0].get('nota', 'Nota não disponível')
    nota = convert_nota(nota)
    theme_text_id = essay.get('theme_text_id')
    updated_at = essay.get('updated_at', 'Data não disponível')

    tema = await get_theme_name(session, theme_text_id) if theme_text_id else None
    disciplina, avaliacao = extract_disciplina_avaliacao(tema)

    unidade, turma = get_aluno_info(matricula)
    if not (unidade and turma):
        return None

    gra, ser = extract_gra_ser(turma)
    if not (gra and ser):
        return None

    success = await grava_nota_db(matricula, disciplina, avaliacao, nota, gra, ser, unidade)
    if not success:
        print(f"[AVISO] Falha ao gravar nota para matrícula {matricula}.", flush=True)

async def main():
    async with aiohttp.ClientSession() as session:
        if not await test_token(session):
            print("[ERRO] Token inválido ou sem permissão. Insira um token válido no campo 'Authorization'.", flush=True)
            return

        page = 1
        total_corrected = 0

        print("Iniciando busca de redações...", flush=True)
        while True:
            print(f"Processando página {page}...", flush=True)
            url = f"{base_url}&page={page}"
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    print(f"[ERRO] Falha na página {page}: {response.status} - {await response.text()}", flush=True)
                    break

                data = await response.json()
                if not data.get('data'):
                    print("Nenhum dado encontrado na página. Finalizando busca.", flush=True)
                    break

                tasks = []
                for essay in data['data']:
                    total_corrected += 1
                    tasks.append(process_essay(session, essay, total_corrected))
                
                await asyncio.gather(*tasks, return_exceptions=True)

                pagination = data.get('pagination', {})
                if page >= pagination.get('last_page', 1):
                    print("Última página alcançada.", flush=True)
                    break
                page += 1

        print(f"Total de redações corrigidas: {total_corrected}", flush=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"[ERRO] Erro inesperado: {e}", flush=True)