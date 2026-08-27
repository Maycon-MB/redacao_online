"""Reimporta notas corrigidas que nunca chegaram aos bancos das unidades.

Consome o JSON produzido pela auditoria (matricula, unidade, ano, disciplina,
avaliacao_id, nota_bruta) e grava via dblink no banco do ano letivo de cada
redação — diferente do importar_notas_sae.py, que sempre mira o ano corrente.

Por padrão roda em DRY-RUN: mostra o que faria e não escreve nada. Para gravar
de fato é preciso passar --apply explicitamente.

Uso:
    python reimportar_faltando.py faltando.json                    # dry-run de tudo
    python reimportar_faltando.py faltando.json --unidade 09       # dry-run de uma unidade
    python reimportar_faltando.py faltando.json --unidade 09 --apply
    python reimportar_faltando.py faltando.json --apply --limite 50
"""
import argparse
import collections
import json
import sys

import psycopg2

from importar_notas_sae import (
    convert_nota,
    db_config,
    servidores,
)

CAMPOS = ('matricula', 'unidade', 'ano', 'disciplina', 'avaliacao_id', 'nota_bruta')


def conn_str(unidade, ano):
    ip = servidores[unidade]['ip']
    return f"dbname=sae{unidade}{ano} hostaddr={ip} user=postgres password=teste port=5432"


def valida(reg):
    """Retorna o motivo da rejeição, ou None se o registro está apto."""
    for c in CAMPOS:
        if reg.get(c) in (None, ''):
            return f"campo ausente: {c}"
    if reg['unidade'] not in servidores:
        return f"unidade desconhecida: {reg['unidade']}"
    if str(reg['ano']) not in ('25', '26'):
        return f"ano fora de escopo: {reg['ano']}"
    if convert_nota(reg['nota_bruta']) == reg['nota_bruta'] and not isinstance(reg['nota_bruta'], str):
        return f"nota não conversível: {reg['nota_bruta']!r}"
    return None


def grava(cur, reg, nota):
    """Grava uma nota via dblink. Retorna o valor booleano devolvido pela função."""
    # os quatro parâmetros de grava_nota são character varying; passar
    # avaliacao_id como int não casa com a assinatura
    inner = cur.mogrify(
        "select grava_nota(%s, %s, %s, %s)",
        (str(reg['matricula']), str(reg['disciplina']),
         str(reg['avaliacao_id']), str(nota))
    ).decode('utf-8')
    cur.execute(
        "SELECT d.retorno FROM dblink(%s, %s) AS d(retorno boolean)",
        (conn_str(reg['unidade'], reg['ano']), inner)
    )
    row = cur.fetchone()
    return bool(row and row[0])


def confere(cur, regs_gravados):
    """Lê de volta as notas gravadas e compara. grava_nota retorna true
    incondicionalmente, então o retorno da função não prova persistência."""
    print("\n--- conferindo no destino ---", flush=True)
    ok = diverge = ausente = 0
    por_destino = collections.defaultdict(list)
    for reg, nota in regs_gravados:
        por_destino[(reg['unidade'], str(reg['ano']))].append((reg, nota))

    for (unidade, ano), itens in sorted(por_destino.items()):
        mats = sorted({str(r['matricula']) for r, _ in itens})
        aids = sorted({str(r['avaliacao_id']) for r, _ in itens})
        inner = cur.mogrify(
            "select mat::text, dis::text, avaliacao_id::text, nota::text"
            " from nota where mat = any(%s) and avaliacao_id = any(%s)",
            (mats, aids)).decode('utf-8')
        cur.execute(
            "SELECT * FROM dblink(%s, %s) AS d(mat text, dis text, aid text, nota text)",
            (conn_str(unidade, ano), inner))
        idx = {(m.strip(), d.strip(), a): n for m, d, a, n in cur.fetchall()}

        for reg, nota in itens:
            chave = (str(reg['matricula']).strip(), reg['disciplina'], str(reg['avaliacao_id']))
            achado = idx.get(chave)
            if achado is None:
                ausente += 1
                print(f"  AUSENTE  {reg['matricula']} aval={reg['avaliacao_id']}", flush=True)
            elif achado.replace(',', '.').lstrip('0') == nota.replace(',', '.').lstrip('0'):
                ok += 1
            else:
                diverge += 1
                print(f"  DIVERGE  {reg['matricula']} aval={reg['avaliacao_id']} "
                      f"esperado={nota} banco={achado}", flush=True)
    print(f"confere={ok} diverge={diverge} ausente={ausente}", flush=True)
    return ok, diverge, ausente


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('json', help='arquivo com os registros a reimportar')
    ap.add_argument('--apply', action='store_true',
                    help='grava de fato (sem isso é dry-run e nada é escrito)')
    ap.add_argument('--unidade', help='processa apenas esta unidade (ex.: 09)')
    ap.add_argument('--ano', help='processa apenas este ano letivo (25 ou 26)')
    ap.add_argument('--limite', type=int, help='processa no máximo N registros')
    ap.add_argument('--registro', default='gravadas_log.json',
                    help='arquivo onde registrar o que foi gravado (para rollback)')
    args = ap.parse_args()

    with open(args.json, encoding='utf-8') as f:
        regs = json.load(f)

    if args.unidade:
        regs = [r for r in regs if r.get('unidade') == args.unidade]
    if args.ano:
        regs = [r for r in regs if str(r.get('ano')) == args.ano]
    if args.limite:
        regs = regs[:args.limite]

    modo = 'APLICANDO (grava no banco)' if args.apply else 'DRY-RUN (nada será escrito)'
    print(f"=== {modo} ===")
    print(f"registros selecionados: {len(regs)}\n")
    if not regs:
        return 0

    aptos, rejeitados = [], []
    for r in regs:
        motivo = valida(r)
        if motivo:
            rejeitados.append((r, motivo))
        else:
            aptos.append((r, convert_nota(r['nota_bruta'])))

    porun = collections.Counter(f"{r['unidade']} {servidores[r['unidade']]['nome']}"
                                for r, _ in aptos)
    print("aptos por unidade:")
    for k, v in sorted(porun.items()):
        print(f"  {k:<26} {v:>5}")
    if rejeitados:
        print(f"\nrejeitados: {len(rejeitados)}")
        for r, motivo in rejeitados[:10]:
            print(f"  {r.get('matricula')}: {motivo}")

    if not args.apply:
        print(f"\n[DRY-RUN] {len(aptos)} notas seriam gravadas. Exemplos:")
        for r, nota in aptos[:5]:
            print(f"  sae{r['unidade']}{r['ano']} mat={r['matricula']} dis={r['disciplina']} "
                  f"aval_id={r['avaliacao_id']} nota={nota}")
        print("\nNada foi escrito. Use --apply para gravar.")
        return 0

    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    gravados, falhas = [], []
    try:
        for i, (r, nota) in enumerate(aptos, 1):
            try:
                if grava(cur, r, nota):
                    gravados.append((r, nota))
                else:
                    falhas.append((r, 'grava_nota retornou false'))
            except Exception as e:
                conn.rollback()
                falhas.append((r, str(e).splitlines()[0]))
                continue
            if i % 100 == 0:
                conn.commit()
                print(f"  {i}/{len(aptos)}...", flush=True)
        conn.commit()
        print(f"\nchamadas ok: {len(gravados)} | falhas: {len(falhas)}")
        for r, motivo in falhas[:10]:
            print(f"  FALHA {r['matricula']}: {motivo}")

        # Registra as chaves gravadas para viabilizar rollback preciso. Sem isso
        # seria preciso inferir o que foi escrito a partir de data/usuario.
        if gravados:
            with open(args.registro, 'w', encoding='utf-8') as f:
                json.dump([{'matricula': r['matricula'], 'unidade': r['unidade'],
                            'ano': r['ano'], 'disciplina': r['disciplina'],
                            'avaliacao_id': r['avaliacao_id'], 'nota': nota}
                           for r, nota in gravados], f, ensure_ascii=False)
            print(f"registro para rollback: {args.registro} ({len(gravados)})")
            confere(cur, gravados)
    finally:
        conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
