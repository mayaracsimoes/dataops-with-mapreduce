from pymongo import MongoClient
import matplotlib.pyplot as plt
import numpy as np

"""
    Acessando o banco de dados!
"""
client = MongoClient("mongodb://localhost:27017")
db = client["cicids"]

def aggregate_labels():
    """Agregação para contar fluxos por label"""
    pipeline = [
        {"$group": {"_id": "$Label", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$out": "counts_by_label"}
    ]
    db.flows.aggregate(pipeline)

def agregar_ip_periodo():
    """Agregação para contar fluxos por IPs e período do dia"""
    pipeline = [
        {"$addFields": {
            "hour": {"$hour": {"$toDate": "$Timestamp"}},
            "ip_pair": {"$concat": ["$Source IP", " → ", "$Destination IP"]}
        }},
        {"$addFields": {
            "period": {
                "$cond": {
                    "if": {"$and": [{"$gte": ["$hour", 8]}, {"$lt": ["$hour", 18]}]},
                    "then": "day",
                    "else": "night"
                }
            }
        }},
        {"$group": {
            "_id": {"ip_pair": "$ip_pair", "period": "$period"},
            "count": {"$sum": 1}
        }},
        {"$project": {
            "_id": 0,
            "key": {"$concat": ["$_id.ip_pair", " [", "$_id.period", "]"]},
            "count": 1
        }},
        {"$sort": {"count": -1}},
        {"$out": "counts_ip_period"}
    ]
    db.flows.aggregate(pipeline)


def aggregate_label_and_period():
    """Agregação para contar fluxos por label e período do dia"""
    pipeline = [
        {"$addFields": {
            "hour": {"$hour": {"$toDate": "$Timestamp"}}
        }},
        {"$addFields": {
            "period": {
                "$cond": {
                    "if": {"$and": [{"$gte": ["$hour", 8]}, {"$lt": ["$hour", 18]}]},
                    "then": "day",
                    "else": "night"
                }
            }
        }},
        {"$group": {
            "_id": {"label": "$Label", "period": "$period"},
            "count": {"$sum": 1}
        }},
        {"$project": {
            "_id": 0,
            "key": {"$concat": ["$_id.label", " | ", "$_id.period"]},
            "count": 1
        }},
        {"$sort": {"count": -1}},
        {"$out": "mapped_period_and_label"}
    ]
    db.flows.aggregate(pipeline)


def aggregate_monthly():
    """Agregação para contar fluxos mensais"""
    pipeline = [
        {"$addFields": {
            "month": {"$substr": ["$Timestamp", 0, 7]}
        }},
        {"$group": {"_id": "$month", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
        {"$out": "counts_by_month"}
    ]
    db.flows.aggregate(pipeline)


def aggregate_top_ips():
    """Agregação para top IPs de origem"""
    pipeline = [
        {"$group": {"_id": "$Source IP", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 15},
        {"$out": "top_source_ips"}
    ]
    db.flows.aggregate(pipeline)


def aggregate_ports():
    """Agregação para portas mais comuns por tipo de ataque"""
    pipeline = [
        {"$match": {"Label": {"$ne": "BENIGN"}}},
        {"$group": {
            "_id": {
                "port": "$Destination Port",
                "label": "$Label"
            },
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}},
        {"$limit": 20},
        {"$out": "ports_by_attack"}
    ]
    db.flows.aggregate(pipeline)


def aggregate_protocols():
    """Agregação para protocolos mais usados"""
    pipeline = [
        {"$group": {"_id": "$Protocol", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$out": "protocol_counts"}
    ]
    db.flows.aggregate(pipeline)

def plot_bar(data, title, xlabel, ylabel, rotate_x=False, top_n=None, save_as=None, color_mapping=None,
             percentage=False, total_count=None, log_scale=False):
    keys = []
    values = []

    for d in data:
        if '_id' in d:
            keys.append(str(d['_id']))
        elif 'key' in d:
            keys.append(str(d['key']))
        else:
            continue
        values.append(d['count'])

    if percentage and total_count is None:
        total_count = sum(values)

    if top_n and len(keys) > top_n:
        main_keys = keys[:top_n]
        main_values = values[:top_n]
        others_count = sum(values[top_n:])
        main_keys.append('OUTROS')
        main_values.append(others_count)
        keys = main_keys
        values = main_values

    if percentage and total_count > 0:
        values = [(v / total_count) * 100 for v in values]
        ylabel = "Porcentagem (%)"

    plt.figure(figsize=(12, 6))

    if color_mapping:
        colors = []
        for k in keys:
            found = False
            for pattern, color in color_mapping.items():
                if pattern in k:
                    colors.append(color)
                    found = True
                    break
            if not found:
                colors.append('gray')
    else:
        colors = 'steelblue'

    bars = plt.bar(keys, values, color=colors)
    plt.title(title, fontsize=14)
    plt.xlabel(xlabel, fontsize=12)
    plt.ylabel(ylabel, fontsize=12)

    # Adiciona rótulos de valor nas barras
    for bar in bars:
        height = bar.get_height()
        if height > 0:  # Só adiciona rótulo se o valor for maior que zero
            if percentage:
                label = f'{height:.4f}%' if height < 0.01 else f'{height:.2f}%'
            else:
                label = f'{height:,}'
            plt.annotate(
                label,
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3),  # Deslocamento vertical
                textcoords="offset points",
                ha='center', va='bottom',
                fontsize=9
            )

    # Configura escala do eixo Y
    if log_scale:
        plt.yscale('log')
        plt.ylim(bottom=0.001)  # Limite inferior para escala log
    elif percentage:
        plt.ylim(0, 100)
        plt.yticks(np.arange(0, 101, 10))
    else:
        plt.ylim(0, max(values) * 1.15)  # 15% de margem no topo

    if rotate_x:
        plt.xticks(rotation=45, ha='right', fontsize=10)
    else:
        plt.xticks(fontsize=10)

    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()

    if save_as:
        plt.savefig(save_as, dpi=300, bbox_inches='tight')
    else:
        plt.show()
    plt.close()

if __name__ == "__main__":
    print("Processando agregações...")

    aggregation_functions = [
        aggregate_labels,
        agregar_ip_periodo,
        aggregate_label_and_period,
        aggregate_monthly,
        aggregate_top_ips,
        aggregate_ports,
        aggregate_protocols
    ]

    for func in aggregation_functions:
        try:
            print(f"Executando {func.__name__}...")
            func()
        except Exception as e:
            print(f"Erro em {func.__name__}: {str(e)}")

    print("Gerando visualizações...")

    total_flows = db.flows.count_documents({})
    print(f"Total de fluxos na coleção: {total_flows:,}")

    period_colors = {'day': 'steelblue', 'night': 'firebrick'}
    try:
        data_labels = list(db.counts_by_label.find().sort("count", -1))
        plot_bar(
            data_labels,
            title="Distribuição de Tipos de Ataques (%)",
            xlabel="Tipo de Ataque",
            ylabel="Porcentagem",
            rotate_x=True,
            top_n=10,
            save_as="attack_types_percentage.png",
            percentage=True,
            total_count=total_flows
        )
    except Exception as e:
        print(f"Erro ao plotar labels: {str(e)}")

    try:
        data_lp = list(db.mapped_period_and_label.find().sort("count", -1).limit(15))
        plot_bar(
            data_lp,
            title="Ataques por Tipo e Período (%)",
            xlabel="Tipo de Ataque | Período",
            ylabel="Porcentagem",
            rotate_x=True,
            save_as="attacks_by_period_percentage.png",
            color_mapping=period_colors,
            percentage=True,
            total_count=total_flows
        )
    except Exception as e:
        print(f"Erro ao plotar labels por período: {str(e)}")

    try:
        data_month = list(db.counts_by_month.find().sort("_id", 1))
        plot_bar(
            data_month,
            title="Fluxos por Mês",
            xlabel="Mês (YYYY-MM)",
            ylabel="Número de Fluxos",
            rotate_x=True,
            save_as="monthly_flows.png"
        )
    except Exception as e:
        print(f"Erro ao plotar fluxos mensais: {str(e)}")

    try:
        data_ips = list(db.top_source_ips.find().sort("count", -1))
        plot_bar(
            data_ips,
            title="Top IPs de Origem (%)",
            xlabel="Endereço IP",
            ylabel="Porcentagem",
            rotate_x=True,
            save_as="top_source_ips_percentage.png",
            percentage=True,
            total_count=total_flows
        )
    except Exception as e:
        print(f"Erro ao plotar top IPs: {str(e)}")

    try:
        total_attacks = db.flows.count_documents({"Label": {"$ne": "BENIGN"}})

        data_ports = list(db.ports_by_attack.find().sort("count", -1).limit(15))
        for item in data_ports:
            if 'key' in item:
                item['percentage'] = (item['count'] / total_attacks) * 100
            else:
                item['percentage'] = (item['count'] / total_attacks) * 100

        keys = [str(d['_id']) if '_id' in d else str(d['key']) for d in data_ports]
        values = [d['count'] for d in data_ports]
        percentages = [d['percentage'] for d in data_ports]

        plt.figure(figsize=(14, 7))
        bars = plt.bar(keys, values, color='purple')
        plt.title("Portas Mais Atacadas por Tipo de Ameaça", fontsize=16)
        plt.xlabel("Porta | Tipo de Ataque", fontsize=12)
        plt.ylabel("Número de Ocorrências (Escala Log)", fontsize=12)
        plt.yscale('log')

        for bar, percent in zip(bars, percentages):
            height = bar.get_height()
            plt.annotate(
                f'{percent:.4f}%',
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3),
                textcoords="offset points",
                ha='center',
                va='bottom',
                fontsize=9
            )

        plt.xticks(rotation=45, ha='right', fontsize=10)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.savefig("ports_by_attack_log.png", dpi=300, bbox_inches='tight')
        plt.close()

        if data_ports:
            specific_total = sum(d['count'] for d in data_ports)

            plot_bar(
                data_ports,
                title="Portas Mais Atacadas por Tipo de Ameaça",
                xlabel="Porta | Tipo de Ataque",
                ylabel="Porcentagem entre Ataques Selecionados (%)",
                rotate_x=True,
                save_as="ports_by_attack_percentage.png",
                percentage=True,
                total_count=specific_total
            )
    except Exception as e:
        print(f"Erro ao plotar portas por ataque: {str(e)}")

    print("Fim da análise dos dados via MapReduce.")
