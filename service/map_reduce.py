from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["cicids"]

REDUCE_SUM = """
function(key, values) {
  return Array.sum(values);
}
"""


def map_reduce_labels():
    """
        Filtro focado em descobrir a quantidade de cada Label existente dentre os fluxos existestes.
    """

    map_fn = """
    function() {
      if (this.Label) emit(this.Label, 1);
    }
    """
    reduce_fn = """
    function(key, values) {
      return Array.sum(values);
    }
    """
    return db.command({
        "mapReduce": "flows",
        "map": map_fn,
        "reduce": reduce_fn,
        "out": "counts_by_label"
    })

def map_reduce_ip_period():
    """
        Filtro focado em mapear os ips (origem -> destino), em seu respectivo periodo, AM ou PM.
    """

    map_fn = """
    function() {
      var h = parseInt(this.Timestamp.substr(11,2),10);
      var status = (h >= 8 && h < 18) ? "working" : "alert";
      var key = this["Source IP"] + " → " + this["Destination IP"] + "["+ status +"]";
      emit(key, 1);
    }
    """
    reduce_fn = """
    function(key, values) {
      return Array.sum(values);
    }
    """
    return db.command({
        "mapReduce": "flows",
        "map": map_fn,
        "reduce": reduce_fn,
        "out": "counts_ip_period"
    })

def map_reduce_by_label_and_period():
    """
        Filtro focado em mapear se o fluxo ocorreu em horário comercial ou não.
    """

    map_fn = """
        function() {
            var horas = parseInt(this.Timestamp.substr(11,2),10),
            periodo = (horas >= 8 && horas < 18) ? "working" : "alert",
            key =  this.Label + " | " + periodo;
            emit(key, 1); 
        }
    """

    reduce_fn = """
        function(key, values) {
          return Array.sum(values);
        }
    """

    return db.command({
        "mapReduce": "flows",
        "map": map_fn,
        "reduce": reduce_fn,
        "out": "mapped_period_and_label"
    })

def map_reduce_monthly():
    """
        Filtro focado em mostrar os fluxos mensais.
    """

    map_fn = """
    function() {
      var month = this.Timestamp.substr(0,7);
      emit(month, 1);
    }
    """
    return db.command({
        "mapReduce": "flows",
        "map": map_fn,
        "reduce": REDUCE_SUM,
        "out": { "replace": "counts_by_month" }
    })

if __name__ == "__main__":
    map_reduce_labels()
    for doc in db["counts_by_label"].find().sort("value", -1):
        print(f"{doc['_id']}: {doc['value']}")
    print()

    print("Rodando MapReduce #2 (IPs AM/PM)…")
    map_reduce_ip_period()
    for doc in db["counts_ip_period"].find().sort("_id", 1):
        print(f"{doc['_id']}: {doc['value']}")
    print()

    print("Rodando MapReduce #3 (Periodo e Label)...")
    map_reduce_by_label_and_period()
    for doc in db["mapped_period_and_label"].find().sort("_id", 1):
        print(f"{doc['_id']}: {doc['value']}")
    print("Rodando MapReduce #1 (Labels)…")

    print("Rodando MapReduce #4 (Periodo e Label)...")
    map_reduce_monthly()
    all_months = list(db["counts_by_month"].find().sort("_id", 1))
    if all_months:
        print(all_months)
        last = all_months[-1]
        print(f"Fluxos em {last['_id']}: {last['value']}")
    else:
        print("Nenhum dado mensal encontrado.")