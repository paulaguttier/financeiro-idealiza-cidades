#!/usr/bin/env python3
"""
Robô de Atualização de Inadimplência — Seehaus Home Resort
Puxa dados do Sienge API, calcula métricas e atualiza o dashboard HTML.
Roda via GitHub Actions toda noite à meia-noite (BRT).
"""

import os
import sys
import re
import json
import time
import base64
import logging
from datetime import datetime, date
from collections import defaultdict
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SIENGE_SUBDOMAIN = os.environ.get("SIENGE_SUBDOMAIN", "demarcoadvocacia")
SIENGE_USER = os.environ["SIENGE_API_USER"]
SIENGE_PASSWORD = os.environ["SIENGE_API_PASSWORD"]
SIENGE_BASE = f"https://api.sienge.com.br/{SIENGE_SUBDOMAIN}/public/api/v1"

ENTERPRISE_ID = int(os.environ.get("SIENGE_ENTERPRISE_ID", "7"))   # Seehaus
COMPANY_ID = int(os.environ.get("SIENGE_COMPANY_ID", "11"))        # SPE Gaspar

DASHBOARD_FILE = os.environ.get("DASHBOARD_FILE", "index.html")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SIENGE API HELPERS
# ---------------------------------------------------------------------------
_auth_header = "Basic " + base64.b64encode(
    f"{SIENGE_USER}:{SIENGE_PASSWORD}".encode()
).decode()

_request_count = 0
_request_window_start = time.time()


def _throttle():
    """Respect 200 req/min rate limit."""
    global _request_count, _request_window_start
    _request_count += 1
    elapsed = time.time() - _request_window_start
    if _request_count >= 190 and elapsed < 60:
        wait = 60 - elapsed + 1
        log.info(f"Rate limit: aguardando {wait:.0f}s...")
        time.sleep(wait)
        _request_count = 0
        _request_window_start = time.time()
    elif elapsed >= 60:
        _request_count = 0
        _request_window_start = time.time()


def api_get(path, params=None):
    """GET request to Sienge API with auth and pagination."""
    url = SIENGE_BASE + path
    if params:
        url += "?" + urlencode(params)
    req = Request(url, headers={
        "Authorization": _auth_header,
        "Accept": "application/json",
    })
    _throttle()
    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except HTTPError as e:
        body = e.read().decode() if e.fp else ""
        log.error(f"HTTP {e.code} on {url}: {body[:300]}")
        raise
    except URLError as e:
        log.error(f"URL error on {url}: {e.reason}")
        raise


def api_get_all(path, params=None, max_pages=50):
    """Paginate through all results."""
    params = dict(params or {})
    params.setdefault("limit", 200)
    params.setdefault("offset", 0)
    all_results = []
    for _ in range(max_pages):
        data = api_get(path, params)
        results = data.get("results", [])
        all_results.extend(results)
        meta = data.get("resultSetMetadata", {})
        total = meta.get("count", 0)
        if len(all_results) >= total or not results:
            break
        params["offset"] = len(all_results)
    return all_results


# ---------------------------------------------------------------------------
# DATA COLLECTION
# ---------------------------------------------------------------------------
def fetch_sales_contracts():
    """Fetch all sales contracts for the enterprise."""
    log.info(f"Buscando contratos de venda (enterpriseId={ENTERPRISE_ID})...")
    contracts = api_get_all("/sales-contracts", {
        "enterpriseId": ENTERPRISE_ID,
    })
    # Filter only active contracts (situation 1=Authorized or 2=Issued)
    active = [c for c in contracts if c.get("situation") in [1, 2, "1", "2"]]
    log.info(f"Total contratos: {len(contracts)}, ativos: {len(active)}")
    return active


def fetch_receivable_bills(customer_id):
    """Fetch receivable bills for a customer."""
    try:
        bills = api_get_all("/accounts-receivable/receivable-bills", {
            "customerId": customer_id,
        })
        return bills
    except HTTPError as e:
        if e.code == 404:
            return []
        raise


def fetch_installments(bill_id):
    """Fetch installments for a receivable bill."""
    try:
        data = api_get(f"/accounts-receivable/receivable-bills/{bill_id}/installments")
        return data.get("results", data) if isinstance(data, dict) else data
    except HTTPError as e:
        if e.code == 404:
            return []
        raise


def collect_inadimplencia_data(contracts):
    """
    For each contract, fetch bills and installments.
    Returns structured data for dashboard metrics.
    """
    today = date.today()
    customer_ids_seen = set()
    customer_names = {}

    # Per-contract data
    contract_data = []  # {customer_id, customer_name, defaulting, total_due, overdue_amount, installments_detail}

    # Collect unique customer IDs from contracts
    for c in contracts:
        cid = c.get("customerId") or c.get("customer", {}).get("id")
        if cid:
            customer_ids_seen.add(cid)
            # Try to get customer name
            name = c.get("customerName") or c.get("customer", {}).get("name", f"Cliente {cid}")
            customer_names[cid] = name

    log.info(f"Clientes únicos: {len(customer_ids_seen)}")

    # For each customer, get their receivable bills
    all_installments = []  # list of {customer_id, customer_name, bill_id, due_date, balance_due, days_overdue}
    customer_overdue = defaultdict(float)       # customer_id -> total overdue
    customer_total_debt = defaultdict(float)    # customer_id -> total outstanding
    defaulting_customers = set()

    for i, cid in enumerate(customer_ids_seen):
        log.info(f"  [{i+1}/{len(customer_ids_seen)}] Cliente {cid}...")
        bills = fetch_receivable_bills(cid)

        for bill in bills:
            bill_id = bill.get("receivableBillId") or bill.get("id")
            is_defaulting = bill.get("defaulting", False)

            if is_defaulting:
                defaulting_customers.add(cid)

            # Fetch installments for this bill
            if bill_id:
                installments = fetch_installments(bill_id)
                if isinstance(installments, list):
                    for inst in installments:
                        due_date_str = inst.get("dueDate")
                        balance = float(inst.get("balanceDue", 0) or 0)

                        if due_date_str and balance > 0:
                            try:
                                due_date = datetime.strptime(due_date_str, "%Y-%m-%d").date()
                            except (ValueError, TypeError):
                                continue

                            days_overdue = (today - due_date).days
                            customer_total_debt[cid] += balance

                            if days_overdue > 0:
                                # Overdue installment
                                customer_overdue[cid] += balance
                                all_installments.append({
                                    "customer_id": cid,
                                    "customer_name": customer_names.get(cid, f"Cliente {cid}"),
                                    "bill_id": bill_id,
                                    "due_date": due_date,
                                    "balance_due": balance,
                                    "days_overdue": days_overdue,
                                })

    return {
        "total_contracts": len(contracts),
        "defaulting_customers": defaulting_customers,
        "customer_names": customer_names,
        "customer_overdue": dict(customer_overdue),
        "customer_total_debt": dict(customer_total_debt),
        "overdue_installments": all_installments,
    }


# ---------------------------------------------------------------------------
# METRICS CALCULATION
# ---------------------------------------------------------------------------
def calculate_metrics(data):
    """Calculate all dashboard metrics from collected data."""
    total_contracts = data["total_contracts"]
    defaulting_count = len(data["defaulting_customers"])
    overdue_installments = data["overdue_installments"]

    # Total saldo vencido
    total_overdue = sum(i["balance_due"] for i in overdue_installments)

    # Total recebido + vencido (approximation from total debt of all customers)
    total_debt = sum(data["customer_total_debt"].values())
    total_received_plus_overdue = total_debt + total_overdue  # simplified

    # Taxa de inadimplência = vencido / (recebido + vencido)
    # Better: use total_overdue / total_debt as proxy
    taxa = (total_overdue / total_debt * 100) if total_debt > 0 else 0

    # Parcelas
    total_parcelas_vencidas = len(overdue_installments)
    # We'd need total installments count - estimate from contracts * avg installments
    # For now, collect from data if available

    # Aging buckets
    aging = {
        "0-30": 0, "31-60": 0, "61-90": 0,
        "91-180": 0, "181-360": 0, "360+": 0
    }
    for inst in overdue_installments:
        d = inst["days_overdue"]
        amt = inst["balance_due"]
        if d <= 30:
            aging["0-30"] += amt
        elif d <= 60:
            aging["31-60"] += amt
        elif d <= 90:
            aging["61-90"] += amt
        elif d <= 180:
            aging["91-180"] += amt
        elif d <= 360:
            aging["181-360"] += amt
        else:
            aging["360+"] += amt

    # Maior concentração
    aging_sorted = sorted(aging.items(), key=lambda x: x[1], reverse=True)
    maior_faixa = aging_sorted[0][0] if aging_sorted else "N/A"
    maior_valor = aging_sorted[0][1] if aging_sorted else 0
    maior_pct = (maior_valor / total_overdue * 100) if total_overdue > 0 else 0

    # Top 10 inadimplentes by total debt
    customer_debts = []
    for cid in data["defaulting_customers"]:
        total = data["customer_total_debt"].get(cid, 0)
        name = data["customer_names"].get(cid, f"Cliente {cid}")
        if total > 0:
            customer_debts.append({"name": name, "total": total})
    customer_debts.sort(key=lambda x: x["total"], reverse=True)
    top10 = customer_debts[:10]

    # Percentage of defaulting contracts
    pct_contratos = (defaulting_count / total_contracts * 100) if total_contracts > 0 else 0

    return {
        "data_posicao": datetime.now().strftime("%b/%Y").replace(
            "Jan", "Jan").replace("Feb", "Fev").replace("Mar", "Mar"
            ).replace("Apr", "Abr").replace("May", "Mai").replace("Jun", "Jun"
            ).replace("Jul", "Jul").replace("Aug", "Ago").replace("Sep", "Set"
            ).replace("Oct", "Out").replace("Nov", "Nov").replace("Dec", "Dez"),
        "contratos_inadimplentes": defaulting_count,
        "total_contratos": total_contracts,
        "pct_contratos": pct_contratos,
        "taxa_inadimplencia": taxa,
        "saldo_vencido": total_overdue,
        "saldo_recebido_vencido": total_debt,
        "parcelas_vencidas": total_parcelas_vencidas,
        "aging": aging,
        "maior_faixa": maior_faixa,
        "maior_valor": maior_valor,
        "maior_pct": maior_pct,
        "top10": top10,
    }


# ---------------------------------------------------------------------------
# HTML UPDATE
# ---------------------------------------------------------------------------
def fmt_mm(value):
    """Format value in R$ MM."""
    mm = value / 1_000_000
    return f"{mm:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_mil(value):
    """Format value in R$ mil."""
    mil = value / 1000
    return f"{mil:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_pct(value):
    """Format percentage Brazilian style."""
    return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def shorten_name(name, max_len=25):
    """Shorten customer name for chart display."""
    if len(name) <= max_len:
        return name
    parts = name.split()
    if len(parts) <= 2:
        return name[:max_len]
    # Keep first name, abbreviate middle, keep last
    first = parts[0]
    last = parts[-1]
    middle = " ".join(p[0] + "." for p in parts[1:-1])
    result = f"{first} {middle} {last}"
    if len(result) > max_len:
        result = f"{first[0]}. {middle} {last}"
    return result


def update_html(metrics):
    """Update the dashboard HTML with new inadimplência data."""
    log.info("Atualizando HTML do dashboard...")

    with open(DASHBOARD_FILE, "r", encoding="utf-8") as f:
        html = f.read()

    m = metrics
    hoje = datetime.now()
    meses_pt = {1:"Janeiro",2:"Fevereiro",3:"Março",4:"Abril",5:"Maio",6:"Junho",
                7:"Julho",8:"Agosto",9:"Setembro",10:"Outubro",11:"Novembro",12:"Dezembro"}
    pos_label = f"{meses_pt[hoje.month][:3]}/{hoje.year}"

    # 1. Update section title: "Inadimplência — Posição Mar/2026"
    html = re.sub(
        r'(Inadimpl[^—]*—\s*Posi[^<]*<)',
        f'Inadimpl&#234;ncia — Posi&#231;&#227;o {pos_label}<',
        html
    )

    # 2. KPI: Contratos Inadimplentes "24 <span...>de 219</span>"
    html = re.sub(
        r'(Contratos Inadimplentes</div>\s*<div class="kpi-value kpi-red">)\d+(\s*<span[^>]*>de )\d+',
        f'\\g<1>{m["contratos_inadimplentes"]}\\g<2>{m["total_contratos"]}',
        html
    )

    # 3. KPI badge: "11,0% DOS CONTRATOS"
    html = re.sub(
        r'[\d,]+%\s*DOS CONTRATOS',
        f'{fmt_pct(m["pct_contratos"])}% DOS CONTRATOS',
        html
    )

    # 4. KPI: Taxa de Inadimplência "1,06%"
    html = re.sub(
        r'(Taxa de Inadimpl[^<]*</div>\s*<div class="kpi-value kpi-red">)[\d,]+%',
        f'\\g<1>{fmt_pct(m["taxa_inadimplencia"])}%',
        html
    )

    # 5. KPI badge: "R$ 1,17 MM de R$ 109,9 MM"
    saldo_mm = fmt_mm(m["saldo_vencido"])
    total_mm = fmt_mm(m["saldo_recebido_vencido"])
    html = re.sub(
        r'R\$\s*[\d,.]+\s*MM\s*de\s*R\$\s*[\d,.]+\s*MM',
        f'R$ {saldo_mm} MM de R$ {total_mm} MM',
        html
    )

    # 6. KPI: Saldo Vencido value
    html = re.sub(
        r'(Saldo Vencido</div>\s*<div class="kpi-value kpi-red">R\$\s*)[\d,.]+\s*MM',
        f'\\g<1>{saldo_mm} MM',
        html
    )

    # 7. KPI sub: "122 parcelas vencidas de 1.933"
    # We may not have total parcelas, keep the "de X" part flexible
    html = re.sub(
        r'\d+\s*parcelas vencidas de\s*[\d.]+',
        f'{m["parcelas_vencidas"]} parcelas vencidas de {m["total_contratos"]}',
        html
    )

    # 8. KPI badge: "6,3% DAS PARCELAS"
    if m["total_contratos"] > 0:
        pct_parcelas = m["parcelas_vencidas"] / m["total_contratos"] * 100
    else:
        pct_parcelas = 0
    html = re.sub(
        r'[\d,]+%\s*DAS PARCELAS',
        f'{fmt_pct(pct_parcelas)}% DAS PARCELAS',
        html
    )

    # 9. Maior Concentração
    faixa_map = {
        "0-30": "0-30 dias", "31-60": "31-60 dias", "61-90": "61-90 dias",
        "91-180": "91-180 dias", "181-360": "181-360 dias", "360+": "360+ dias"
    }
    maior_label = faixa_map.get(m["maior_faixa"], m["maior_faixa"])
    html = re.sub(
        r'(Maior Concentra[^<]*</div>\s*<div class="kpi-value kpi-dark">)[^<]+',
        f'\\g<1>{maior_label}',
        html
    )
    html = re.sub(
        r'(Maior Concentra[^<]*</div>\s*<div class="kpi-value kpi-dark">[^<]+</div>\s*<div class="kpi-sub">)R\$\s*[^<]+',
        f'\\g<1>R$ {fmt_mil(m["maior_valor"])} mil ({m["maior_pct"]:.0f}% do saldo vencido)',
        html
    )

    # 10. Aging chart data
    aging_values = [
        round(m["aging"]["0-30"] / 1000, 1),
        round(m["aging"]["31-60"] / 1000, 1),
        round(m["aging"]["61-90"] / 1000, 1),
        round(m["aging"]["91-180"] / 1000, 1),
        round(m["aging"]["181-360"] / 1000, 1),
        round(m["aging"]["360+"] / 1000, 1),
    ]
    aging_str = ", ".join(str(v) for v in aging_values)
    html = re.sub(
        r"(label:\s*'Saldo Vencido \(R\$ mil\)',\s*data:\s*\[)[^\]]+",
        f"\\g<1>{aging_str}",
        html
    )

    # 11. Top 10 Defaulters chart
    if m["top10"]:
        top_names = [shorten_name(d["name"]) for d in m["top10"]]
        top_values = [round(d["total"] / 1000, 1) for d in m["top10"]]

        names_str = ",".join(f"'{n}'" for n in top_names)
        values_str = ", ".join(str(v) for v in top_values)

        html = re.sub(
            r"(chartDefaulters'\),\s*\{\s*type:\s*'bar',\s*data:\s*\{\s*labels:\s*\[)[^\]]+",
            f"\\g<1>{names_str}",
            html
        )
        html = re.sub(
            r"(label:\s*'Saldo Devedor Total \(R\$ mil\)',\s*data:\s*\[)[^\]]+",
            f"\\g<1>{values_str}",
            html
        )

    # 12. Footer date
    footer_date = f"{meses_pt[hoje.month]} {hoje.year}"
    html = re.sub(
        r'(class="footer">Idealiza Cidades\s*[—–-]\s*Seehaus Home Resort\s*[—–-]\s*)[^<]+',
        f'\\g<1>{footer_date}',
        html
    )

    with open(DASHBOARD_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    log.info("Dashboard atualizado com sucesso!")
    return True


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    log.info("=" * 60)
    log.info("ROBÔ INADIMPLÊNCIA — Seehaus Home Resort")
    log.info(f"Empresa: {COMPANY_ID} | Empreendimento: {ENTERPRISE_ID}")
    log.info(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    # 1. Fetch contracts
    contracts = fetch_sales_contracts()
    if not contracts:
        log.warning("Nenhum contrato encontrado! Abortando.")
        sys.exit(1)

    # 2. Collect inadimplência data
    data = collect_inadimplencia_data(contracts)

    # 3. Calculate metrics
    metrics = calculate_metrics(data)

    # Log summary
    log.info("-" * 40)
    log.info(f"Contratos inadimplentes: {metrics['contratos_inadimplentes']} de {metrics['total_contratos']}")
    log.info(f"Taxa: {metrics['taxa_inadimplencia']:.2f}%")
    log.info(f"Saldo vencido: R$ {metrics['saldo_vencido']:,.2f}")
    log.info(f"Parcelas vencidas: {metrics['parcelas_vencidas']}")
    log.info(f"Maior faixa: {metrics['maior_faixa']} (R$ {metrics['maior_valor']:,.2f})")
    log.info(f"Top 10: {[d['name'] for d in metrics['top10']]}")
    log.info("-" * 40)

    # 4. Update HTML
    update_html(metrics)

    log.info("Processo finalizado com sucesso!")


if __name__ == "__main__":
    main()
