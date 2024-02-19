import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import sys

def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month

start_date = datetime.datetime(2024, 2, 7)

pay_dates = [start_date + relativedelta(months=i, days=20) for i in range(20)]
for date in pay_dates:
    print(date)

cohorts = {}
for i in range(1, 16):
    cohorts[str(i)] = {
        "fecha_entrada": start_date + relativedelta(months=i - 1),
        "nro_cuotas": 1000,
        "usd_x_cuota": 7,
        "desembolso_acum": 0,
    }

for k, v in cohorts.items():
    print(k, v)

pd.set_option('display.precision', 2)
pd.options.display.float_format = '{:.2f}'.format
tabla = pd.DataFrame(columns=["nro_fecha", "fecha", "id", "usd_x_cuota", "nro_cuotas", "desembolso", "paga", "pxq"])

n = 1
for date in pay_dates:
    for id in cohorts.keys():
        if date > cohorts[id]["fecha_entrada"] + relativedelta(months=3):
            meses_transcurridos = diff_month(date, cohorts[id]["fecha_entrada"])
            paga = True if meses_transcurridos % 3 == 0 else False
            cohorts[id]["desembolso_acum"] += cohorts[id]["nro_cuotas"] * cohorts[id]["usd_x_cuota"]
            desembolso = cohorts[id]["desembolso_acum"] * paga
            tabla.loc[len(tabla)] = [n, date, id, cohorts[id]["usd_x_cuota"], cohorts[id]["nro_cuotas"],
                                     cohorts[id]["nro_cuotas"] * cohorts[id]["usd_x_cuota"], paga, desembolso]
            if paga:
                cohorts[id]["desembolso_acum"] = 0
    n += 1
print(tabla.to_string())

# Usando data de clientes de ZED

start_date_2 = datetime.datetime(2022, 10, 7)
zed_ops = pd.read_excel("zed_ops.xlsx")
zed_ops["fecha"] = zed_ops.apply(lambda x: x["Transacción"] if x["Transacción"].day <= 20 else x["Transacción"] + relativedelta(days=12), axis=1)
zed_ops["cohorte"] = zed_ops.apply(lambda x: diff_month(x["fecha"], start_date_2), axis=1)
zed_ops["Signo"] = zed_ops.apply(lambda x: 1 if x["Tipo"] == "Compra" else -1, axis=1)
zed_ops["cuotas_neto"] = zed_ops.apply(lambda x: x["Cuotas"] * x["Signo"], axis=1)

zed_ops_net_by_client = zed_ops.groupby(['id', 'cohorte'])['cuotas_neto'].sum().reset_index()
zed_ops_net_by_client['cuotas_acum'] = zed_ops_net_by_client.groupby('id')['cuotas_neto'].cumsum()
zed_ops_net_by_client.rename(columns={"cuotas_neto": "cuotas", "cuotas_acum": "cuotas_neto"}, inplace=True)

zed_ops_net_by_cohort = zed_ops.groupby(['cohorte'])['cuotas_neto'].sum().reset_index()

zed_ops_net_by_cohort["fecha_entrada"] = zed_ops_net_by_cohort.apply(lambda x: start_date_2 + relativedelta(months=x["cohorte"]), axis=1)
zed_ops_net_by_cohort["cohorte"] = zed_ops_net_by_cohort.apply(lambda x: diff_month(x["fecha_entrada"], start_date_2), axis=1)

usd_por_cuota_mensual = pd.read_excel("usd_por_cuota_mensual.xlsx")
usd_por_cuota_mensual["cohorte"] = usd_por_cuota_mensual.apply(lambda x: diff_month(x["fecha"], start_date_2), axis=1)

zed_ops_by_cohort_full = pd.merge(zed_ops_net_by_cohort, usd_por_cuota_mensual, on="cohorte")
zed_ops_by_cohort_full = zed_ops_by_cohort_full[["cohorte", "fecha_entrada", "cuotas_neto", "usd_por_cuota"]]

print("Usando data de clientes de ZED")
print(zed_ops.to_string())
print("zed_ops_net_by_client")
print(zed_ops_net_by_client.to_string())
print("zed_ops_net_by_cohort")
print(zed_ops_net_by_cohort.to_string())
print("usd_por_cuota_mensual")
print(usd_por_cuota_mensual.to_string())
print("zed_ops_by_cohort_full")
print(zed_ops_by_cohort_full.to_string())

print("zed_ops_net_by_client_compras")
zed_ops_net_by_client_compras = zed_ops_net_by_client[zed_ops_net_by_client["cuotas"] >= 0]
print(zed_ops_net_by_client_compras.to_string())
print("zed_ops_net_by_client_ventas")
zed_ops_net_by_client_ventas = zed_ops_net_by_client[zed_ops_net_by_client["cuotas"] < 0]
print(zed_ops_net_by_client_ventas.to_string())


nro_de_cohortes = zed_ops["cohorte"].max()
"""
target_customer_id = "73203092"

print("this_clients_buy_pos")
this_clients_buy_pos = zed_ops_net_by_client_compras[zed_ops_net_by_client_compras["id"] == target_customer_id]
print(this_clients_buy_pos.to_string())

print("this_clients_sell_pos")
this_clients_sell_pos = zed_ops_net_by_client_ventas[zed_ops_net_by_client_ventas["id"] == target_customer_id]
this_clients_sell_pos = this_clients_sell_pos[this_clients_sell_pos["cohorte"] <= 12]
print(this_clients_sell_pos.to_string())
"""

def get_net_client_portfolios_by_date(compras_by_client_df, ventas_by_client_df, nro_cohorte):
    zed_ops_net_by_client_compras = compras_by_client_df[compras_by_client_df["cohorte"] <= nro_cohorte]
    zed_ops_net_by_client_ventas = ventas_by_client_df[ventas_by_client_df["cohorte"] <= nro_cohorte]
    # Filtrar zed_ops_net_by_client_ventas hasta la fecha correspondiente cada vez
    for i, row_i in zed_ops_net_by_client_ventas.iterrows():
        this_clients_buy_pos = zed_ops_net_by_client_compras[zed_ops_net_by_client_compras["id"] == row_i["id"]]
        this_clients_buy_pos = this_clients_buy_pos[this_clients_buy_pos["cohorte"] <= row_i["cohorte"]]
        sell_pos = -row_i["cuotas"]
        for j, row_j in this_clients_buy_pos.iterrows():
            if not sell_pos:
                break
            if row_j["cuotas"] >= sell_pos:
                this_clients_buy_pos.loc[j, "cuotas"] = this_clients_buy_pos.loc[j, "cuotas"] - sell_pos
                sell_pos = 0
            else:
                this_clients_buy_pos.loc[j, "cuotas"] = 0
                sell_pos -= row_j["cuotas"]

        for k, row in this_clients_buy_pos.iterrows():
            zed_ops_net_by_client_compras.loc[
                (zed_ops_net_by_client_compras["id"] == row["id"]) & (zed_ops_net_by_client_compras["cohorte"] == row["cohorte"]),
                "cuotas"
            ] = row["cuotas"]

    return zed_ops_net_by_client_compras


all_client_portfolios_by_date = pd.DataFrame(columns=["id", "cohorte", "cuotas", "cuotas_neto", "fecha_obs"])
for i in range(1, nro_de_cohortes + 1):
    these_portfolios = get_net_client_portfolios_by_date(zed_ops_net_by_client_compras, zed_ops_net_by_client_ventas, i)
    these_portfolios.loc[:, "fecha_obs"] = i + 2
    all_client_portfolios_by_date = pd.concat([all_client_portfolios_by_date, these_portfolios])
all_client_portfolios_by_date = all_client_portfolios_by_date[["fecha_obs", "id", "cohorte", "cuotas", "cuotas_neto"]]

print("all_client_portfolios_by_date")
all_client_portfolios_by_date.rename(columns={"fecha_obs": "nro_fecha"}, inplace=True)
#all_client_portfolios_by_date = all_client_portfolios_by_date[all_client_portfolios_by_date["nro_fecha"] == 15]
print(all_client_portfolios_by_date.head(500).to_string())

#zed_ops_net_by_client_compras = zed_ops_net_by_client_compras[zed_ops_net_by_client_compras["id"] == target_customer_id]
print("zed_ops_net_by_client_compras_nuevo")
print(zed_ops_net_by_client_compras.to_string())

cohorts_2 = {}
for index, row in zed_ops_by_cohort_full.iterrows():
    cohorts_2[row["cohorte"]] = {
        "fecha_entrada": row["fecha_entrada"].to_pydatetime(),
        "nro_cuotas": row["cuotas_neto"],
        "usd_x_cuota": 0,
        "usd_x_cuota_acum": 0,
    }

for k, v in cohorts_2.items():
    print(k, v)

pay_dates_2 = [start_date_2 + relativedelta(months=i, days=20) for i in range(1, 16)]
print("pay_dates_2")
for i in range(len(pay_dates_2)):
    print(i + 1, pay_dates_2[i])

tabla_2 = pd.DataFrame(columns=["nro_fecha", "fecha", "cohorte", "usd_x_cuota", "usd_x_cuota_acum", "nro_cuotas", "paga", "desembolso"])

# Hacer esto para nivel cohorte: nro_fecha, date,"usd_x_cuota", "usd_x_cuota_acum", paga]
nro_fecha = 1
for date in pay_dates_2:
    for id in cohorts_2.keys():
        if date > cohorts_2[id]["fecha_entrada"] + relativedelta(months=2):
            meses_transcurridos = diff_month(date, cohorts_2[id]["fecha_entrada"])
            paga = meses_transcurridos % 3 == 0
            #print(date)
            #print(usd_por_cuota_mensual.loc[usd_por_cuota_mensual['fecha'] == date, 'usd_por_cuota'].iloc[0])
            cohorts_2[id]["usd_x_cuota"] = usd_por_cuota_mensual.loc[usd_por_cuota_mensual['fecha'] == date, 'usd_por_cuota'].iloc[0]
            cohorts_2[id]["usd_x_cuota_acum"] += cohorts_2[id]["usd_x_cuota"]
            desembolso = cohorts_2[id]["usd_x_cuota_acum"] * cohorts_2[id]["nro_cuotas"] * paga
            tabla_2.loc[len(tabla_2)] = [nro_fecha, date, id, cohorts_2[id]["usd_x_cuota"], cohorts_2[id]["usd_x_cuota_acum"],
                                         cohorts_2[id]["nro_cuotas"], paga, desembolso]
            if paga:
                cohorts_2[id]["usd_x_cuota_acum"] = 0

            # cohorts_2[id]["usd_x_cuota"], cohorts_2[id]["usd_x_cuota_acum"], cohorts_2[id]["nro_cuotas"]
    nro_fecha += 1
print(f"desembolso_por_cohorte (todas las fechas)")
print(tabla_2.to_string())

print(f"desembolso_por_fecha_por_cohorte_por_cliente")
tabla_3 = pd.merge(tabla_2, all_client_portfolios_by_date, on=['nro_fecha', 'cohorte'], how='inner')
tabla_3["desembolso"] = tabla_3.apply(lambda x: x["usd_x_cuota_acum"] * x["cuotas"] * x["paga"], axis=1)
tabla_3 = tabla_3[["nro_fecha", "fecha", "id", "cohorte", "usd_x_cuota", "usd_x_cuota_acum", "cuotas", "paga", "desembolso"]]
tabla_3.sort_values(["nro_fecha", "id", "cohorte"],
               axis = 0, ascending = True,
               inplace = True)
print(tabla_3.to_string())
tabla_3.to_excel("desembolso_por_cliente_zdp_con_ventas.xlsx", index=False)
sys.exit()

# Finalmete, los pagos desagregados por cliente
#zed_ops_net_by_client.rename(columns={"Cohorte": "a", "B": "c"})

nro_fecha = 7
print(f"desembolso_por_cliente para la fecha {nro_fecha}")
desembolso_por_cliente = pd.merge(tabla_2, zed_ops_net_by_client, on="cohorte")
desembolso_por_cliente = desembolso_por_cliente[["nro_fecha", "fecha", "cohorte", "usd_x_cuota", "usd_x_cuota_acum",
                                                 "id", "cuotas_neto", "paga"]]
desembolso_por_cliente["desembolso"] = desembolso_por_cliente.apply(lambda x: x["usd_x_cuota_acum"] * x["cuotas_neto"] * x ["paga"], axis=1)

desembolso_por_cliente = desembolso_por_cliente[desembolso_por_cliente["nro_fecha"] == nro_fecha].reset_index()
desembolso_por_cliente.drop(columns=['index'], inplace=True)
print(desembolso_por_cliente.head(5000).to_string())

#desembolso_por_cliente.to_excel("desembolso_por_cliente_zdp.xlsx")
