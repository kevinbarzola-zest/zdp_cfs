import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

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

nro_fecha = 1
for date in pay_dates_2:
    for id in cohorts_2.keys():
        if date > cohorts_2[id]["fecha_entrada"] + relativedelta(months=3):
            meses_transcurridos = diff_month(date, cohorts_2[id]["fecha_entrada"])
            paga = meses_transcurridos % 3 == 0
            print(date)
            print(usd_por_cuota_mensual.loc[usd_por_cuota_mensual['fecha'] == date, 'usd_por_cuota'].iloc[0])
            cohorts_2[id]["usd_x_cuota"] = usd_por_cuota_mensual.loc[usd_por_cuota_mensual['fecha'] == date, 'usd_por_cuota'].iloc[0]
            cohorts_2[id]["usd_x_cuota_acum"] += cohorts_2[id]["usd_x_cuota"]
            desembolso = cohorts_2[id]["usd_x_cuota_acum"] * cohorts_2[id]["nro_cuotas"] * paga
            tabla_2.loc[len(tabla_2)] = [nro_fecha, date, id, cohorts_2[id]["usd_x_cuota"], cohorts_2[id]["usd_x_cuota_acum"],
                                         cohorts_2[id]["nro_cuotas"], paga, desembolso]
            if paga:
                cohorts_2[id]["usd_x_cuota_acum"] = 0
    nro_fecha += 1
print(f"desembolso_por_cohorte (todas las fechas)")
print(tabla_2.to_string())

# Finalmete, los pagos desagregados por cliente
#zed_ops_net_by_client.rename(columns={"Cohorte": "a", "B": "c"})

nro_fecha = 7
print(f"desembolso_por_cliente para la fecha {nro_fecha}")
desembolso_por_cliente = pd.merge(tabla_2, zed_ops_net_by_client, on="cohorte")
desembolso_por_cliente = desembolso_por_cliente[["nro_fecha", "fecha", "cohorte", "usd_x_cuota", "usd_x_cuota_acum",
                                                 "id", "cuotas_neto", "paga"]]
desembolso_por_cliente["desembolso"] = desembolso_por_cliente.apply(lambda x: x["usd_x_cuota_acum"] * x["cuotas_neto"] * x ["paga"], axis=1)

#desembolso_por_cliente = desembolso_por_cliente[desembolso_por_cliente["nro_fecha"] == nro_fecha].reset_index()
#desembolso_por_cliente.drop(columns=['index'], inplace=True)
print(desembolso_por_cliente.head(5000).to_string())

desembolso_por_cliente.to_excel("desembolso_por_cliente_zdp.xlsx")
