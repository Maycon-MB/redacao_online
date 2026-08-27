"""Desfaz uma carga feita pelo reimportar_faltando.py.

Consome o gravadas_log.json produzido pela carga e remove as linhas
correspondentes da tabela nota das unidades.

Só apaga uma linha quando ela ainda está exatamente como a carga a deixou:
usuario='Nota_web', a nota igual à gravada e nota_ant vazia (ou seja, a carga
inseriu a linha e não sobrescreveu nada). Se alguém alterou a nota depois, a
linha é preservada e reportada — apagá-la destruiria trabalho de terceiro.

Por padrão roda em DRY-RUN; apaga apenas com --apply.

Uso:
    python rollback_reimportacao.py gravadas_log.json
    python rollback_reimportacao.py gravadas_log.json --unidade 09 --apply
"""
import argparse
import collections
import json
import sys

import psycopg2

from importar_notas_sae import db_config, servidores


def conn_str(unidade, ano):
    ip = servidores[unidade]['ip']
    return f"dbname=sae{unidade}{ano} hostaddr={ip} user=postgres password=teste port=5432"


def norm(v):
    return (v or '').replace(',', '.').lstrip('0') or '0'


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('log', help='gravadas_log.json produzido pela carga')
    ap.add_argument('--apply', action='store_true', help='apaga de fato')
    ap.add_argument('--unidade', help='restringe a uma unidade')
    args = ap.parse_args()

    with open(args.log, encoding='utf-8') as f:
        regs = json.load(f)
    if args.unidade:
        regs = [r for r in regs if r['unidade'] == args.unidade]

    modo = 'APLICANDO (apaga do banco)' if args.apply else 'DRY-RUN (nada será apagado)'
    print(f"=== {modo} ===\nregistros no log: {len(regs)}\n")
    if not regs:
        return 0

    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    v = collections.Counter()
    preservados = []
    try:
        por_destino = collections.defaultdict(list)
        for r in regs:
            por_destino[(r['unidade'], str(r['ano']))].append(r)

        for (unidade, ano), itens in sorted(por_destino.items()):
            mats = sorted({str(r['matricula']) for r in itens})
            aids = sorted({str(r['avaliacao_id']) for r in itens})
            inner = cur.mogrify(
                "select mat::text, dis::text, avaliacao_id::text, nota::text,"
                " coalesce(nota_ant,'')::text, usuario::text from nota"
                " where mat = any(%s) and avaliacao_id = any(%s)", (mats, aids)).decode()
            cur.execute("SELECT * FROM dblink(%s, %s) AS d(mat text, dis text, aid text,"
                        " nota text, ant text, usuario text)", (conn_str(unidade, ano), inner))
            atual = {(m.strip(), d.strip(), a): (n, ant, u) for m, d, a, n, ant, u in cur.fetchall()}

            apagar = []
            for r in itens:
                chave = (str(r['matricula']).strip(), r['disciplina'], str(r['avaliacao_id']))
                got = atual.get(chave)
                if got is None:
                    v['ja_ausente'] += 1
                    continue
                nota_atual, ant, usuario = got
                if usuario != 'Nota_web' or norm(nota_atual) != norm(r['nota']) or ant.strip():
                    v['PRESERVADO (alterado depois)'] += 1
                    preservados.append((unidade, r['matricula'], r['nota'], nota_atual, usuario, ant))
                    continue
                apagar.append(r)

            print(f"sae{unidade}{ano} {servidores[unidade]['nome']:<18} "
                  f"a apagar={len(apagar)} de {len(itens)}")

            if apagar and args.apply:
                for r in apagar:
                    inner = cur.mogrify(
                        "delete from nota where mat=%s and dis=%s and avaliacao_id=%s"
                        " and usuario='Nota_web' and coalesce(nota_ant,'')=''",
                        (str(r['matricula']), str(r['disciplina']),
                         str(r['avaliacao_id']))).decode()
                    # dblink() exige query que retorna linhas; DELETE vai por dblink_exec
                    cur.execute("SELECT dblink_exec(%s, %s)", (conn_str(unidade, ano), inner))
                    v['APAGADO'] += 1
                conn.commit()
            elif apagar:
                v['seria_apagado'] += len(apagar)
        conn.commit()
    finally:
        conn.close()

    print("\n" + "=" * 60)
    for k, n in v.most_common():
        print(f"  {k:<32} {n}")
    if preservados:
        print("\n  preservados (unidade, matricula, nota_da_carga, nota_atual, usuario, nota_ant):")
        for p in preservados[:20]:
            print("   ", p)
    if not args.apply:
        print("\nNada foi apagado. Use --apply para executar.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
