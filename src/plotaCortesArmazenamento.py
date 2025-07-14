import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from inewave.nwlistcf import Nwlistcfrel
from inewave.newave import Confhd
from inewave.newave import Hidr
from inewave.newave import Parpvaz
from inewave.newave import Vazinat
from inewave.newave.cortesh import Cortesh
from inewave.newave import Dger
import numpy as np
import plotly.graph_objects as go
import csv
import os
import logging
import time
import warnings
start = time.time()
warnings.filterwarnings("ignore")
pd.options.mode.chained_assignment = None
## PARAMETROS DE ENTRADA
usina_escolhida = 17
parametros_variacao_volume_util_mlt = {
    #PERCENTUAL VOLUME UTIL,     PERCENTUAL MLT
    "V1":[            20          ,   80            ],
    "V2":[            40          ,   80            ],
    "V3":[            60          ,   80            ],
    "V4":[            80          ,   80            ],
}
caminho_caso = "../"

### CONFIGURA LOG
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

### LEITURA DGER
caminho_dger = os.path.join(caminho_caso, "dger.dat")
if not os.path.exists(caminho_dger):
    raise FileNotFoundError(f"Arquivo não encontrado: {caminho_dger}. A pasta informada pode não ser um caso de NEWAVE.")
else:
    dados_Dger = Dger.read(caminho_dger)
    mes_inicio = dados_Dger.mes_inicio_estudo
    ano_inicio = dados_Dger.ano_inicio_estudo
    data_inicio = datetime(year=ano_inicio, month=mes_inicio, day=1)
    flag_parp = dados_Dger.consideracao_media_anual_afluencias
    logging.info(f"Lendo arquivo dger.dat")

### LEITURA HIDR
caminho_hidr = os.path.join(caminho_caso, "hidr.dat")
if not os.path.exists(caminho_hidr):
    raise FileNotFoundError(f"Arquivo não encontrado: {caminho_hidr}. A pasta informada pode não ser um caso de NEWAVE.")
else:
    df_hidr = Hidr.read(caminho_hidr).cadastro #####LEITURA DO HIDR.DAT
    logging.info(f"Lendo arquivo hidr.dat")

### LEITURA CONFHD
caminho_confhd = os.path.join(caminho_caso, "confhd.dat")
if not os.path.exists(caminho_confhd):
    raise FileNotFoundError(f"Arquivo não encontrado: {caminho_confhd}. A pasta informada pode não ser um caso de NEWAVE.")
else:
    df_confhd = Confhd.read(caminho_confhd).usinas ####Leitura do Confhd.dat
    logging.info(f"Lendo arquivo confhd.dat")


        


### LEITURA NWLISTCF.REL
caminho_nwlistcf = os.path.join("nwlistcf.csv")
if not os.path.exists(caminho_nwlistcf):
    raise FileNotFoundError(f"Arquivo não encontrado: {caminho_nwlistcf}. A pasta informada pode não ser um caso de NEWAVE.")
else:
    logging.info(f"Lendo arquivo nwlistcf.rel")
    #df = Nwlistcfrel.read(caminho_nwlistcf).cortes
    #df.head(1000).to_csv("nwlistcf.csv",index = False)
    df = pd.read_csv("nwlistcf_testes.csv")
    print(df)
    periodo = df["PERIODO"].unique()[0]
    periodo_real = mes_inicio - periodo
    data_real = data_inicio + relativedelta(months=periodo_real)
    mes_real = data_real.month
    lista_ireg = df["IREG"].unique()
    usinas = df["UHE"].unique()
    
    eco_primeiro_rhs = df.loc[(df["IREG"] == lista_ireg[0])].reset_index(drop = True)
    eco_primeiro_rhs.to_csv("eco_primeiro_rhs.csv")



### LEITURA PARPVAZ
caminho_mlt_incremental = os.path.join("mlt_incremental.csv")
if os.path.exists(caminho_mlt_incremental):
        logging.info(f"Lendo arquivo de MLT Incremental")
        mlt_incremental = pd.read_csv(caminho_caso+"/mlt_incremental.csv") 
else:
    caminho_parpvaz = os.path.join(caminho_caso, "parpvaz.dat")
    if not os.path.exists(caminho_parpvaz):
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_parpvaz}. A pasta informada pode não ser um caso de NEWAVE.")
    else:
        logging.info(f"Lendo arquivo parpvaz.dat")
        series_vazoes_incrementais = Parpvaz.read(caminho_caso+"/parpvaz.dat").series_vazoes_uhe 
        series_vazoes_incrementais['data'] = pd.to_datetime(series_vazoes_incrementais['data'])
        lista_df = []
        for usi in usinas:
            nome_usina = df_confhd.loc[(df_confhd["codigo_usina"] == usi)]["nome_usina"].iloc[0]
            serie_vazoes_usina = series_vazoes_incrementais.loc[(series_vazoes_incrementais["uhe"] == nome_usina)].reset_index(drop = True)
            primeiras_ocorrencias_por_data = serie_vazoes_usina.drop_duplicates(subset='data', keep='first').reset_index(drop = True)
            for mes in range(1,13):
                serie_usina_mes = primeiras_ocorrencias_por_data.loc[(primeiras_ocorrencias_por_data["data"].dt.month == mes)].reset_index(drop = True)
                MLT_INCREMENTAL = serie_usina_mes["valor"].mean()
                lista_df.append(
                    pd.DataFrame(
                        {
                            "codigo_usina":usi,
                            "nome_usina":nome_usina,
                            "mes":mes,
                            "MLT_INCR":MLT_INCREMENTAL
                        }
                    )
                )
        df_mlt_incremental = pd.concat(lista_df).reset_index(drop = True)
        df_mlt_incremental.to_csv(caminho_caso+"/mlt_incremental.csv", index = False)

end = time.time()
logging.info(f"Tempo de leitura dos dados de entrada: {end - start:.4f} segundos")
start_execucao = time.time()


#### PARÂMETROS DO RESERVATORIO DA USINA ESCOLHIDA
nome_usina_escolhida = df_confhd.loc[(df_confhd["codigo_usina"] == usina_escolhida)]["nome_usina"].iloc[0]
vmin_usina_escolhida = df_hidr.loc[(df_hidr["nome_usina"] == nome_usina_escolhida)]["volume_minimo"].iloc[0]
vmax_usina_escolhida = df_hidr.loc[(df_hidr["nome_usina"] == nome_usina_escolhida)]["volume_maximo"].iloc[0]
vutil_escolhida = vmax_usina_escolhida - vmin_usina_escolhida
x = np.linspace(int(0), int(vutil_escolhida), int(100))


logging.info(f"Calculando abatimento RHS todas as usinas consideradas constantes")
piafl_cols = [col for col in df.columns if col.startswith("PIAFL(")]
df_memoria_calculo = pd.DataFrame()
for parametro in parametros_variacao_volume_util_mlt:
    start_etapa = time.time()
    valores_percentuais = parametros_variacao_volume_util_mlt[parametro]
    logging.info(f"Relizando o cálculo do par %VU {valores_percentuais[0]} %MLT {valores_percentuais[1]}")
    df_ireg_rhs = df.loc[df["RHS"] != 0, ["PERIODO", "IREG", "RHS"]].reset_index(drop=True)
    df_ireg_rhs["RHS_CALC"] = df_ireg_rhs["RHS"]
    lista_df_contribuicao_volume = []
    lista_df_contribuicao_afluencia = []
    for usi in usinas:
        df_ireg_usi = df.loc[(df["UHE"] == usi)].reset_index(drop = True)

        #### PARÂMETROS DO RESERVATORIO DAS USINAS NÃO ESCOLHIDAS
        nome_usina = df_confhd.loc[(df_confhd["codigo_usina"] == usi)]["nome_usina"].iloc[0]
        vmin_usina_analisada = df_hidr.loc[(df_hidr["nome_usina"] == nome_usina)]["volume_minimo"].iloc[0]
        vmax_usina_analisada = df_hidr.loc[(df_hidr["nome_usina"] == nome_usina)]["volume_maximo"].iloc[0]
        vutil = vmax_usina_analisada - vmin_usina_analisada 
        vutil_acoplamento = vutil*(valores_percentuais[0]/100)
        df_memoria_calculo["PERC_VUTIL"] = vutil_acoplamento
        parcela_armazenamento_abatimento_rhs = df_ireg_usi["PIVARM"]*vutil_acoplamento

        rhs_ini_afl = df_ireg_rhs["RHS_CALC"].head(1).values
        if(usi != usina_escolhida):
            df_ireg_rhs["RHS_CALC"] = df_ireg_rhs["RHS_CALC"] + parcela_armazenamento_abatimento_rhs

 
        df_memoria_calculo_rhs = pd.DataFrame(
            {
                "MES_INICIO":[mes_real],
                "RHS_INICIAL":rhs_ini_afl,
                "USINA":[nome_usina],
                "VMAX_HIDR":[vmax_usina_analisada],
                "VMIN_HIDR":[vmin_usina_analisada],
                "VUTIL":[vutil],
                "PERC_VUTIL":[valores_percentuais[0]],
                "VUTIL_ACOPLA":[vutil_acoplamento],
                "PIV":df_ireg_usi["PIVARM"].head(1).values,
                "CALC_V":parcela_armazenamento_abatimento_rhs.head(1).values,
                "RHS_FINAL_VOL":df_ireg_rhs["RHS_CALC"].head(1).values,                
            }
        )
        lista_df_contribuicao_volume.append(df_memoria_calculo_rhs)

        ## VERIFICA QUAL A MLT DO MÊS EM QUESTÃO, CONVERTE PARA HM3 e TIRA QUAL SERÁ O PERCENTUAL DA MLT
        serie_vazoes_usina = series_vazoes_incrementais.loc[(series_vazoes_incrementais["uhe"] == nome_usina)].reset_index(drop = True)
        primeiras_ocorrencias_por_data = serie_vazoes_usina.drop_duplicates(subset='data', keep='first').reset_index(drop = True)

        contador_lags = 1
        for coluna_lag in piafl_cols:
            mes_lag = mes_real - contador_lags
            if(mes_lag <= 0):
                mes_lag = mes_real - contador_lags + 12
            #print("mes_real: ", mes_real, " mes_lag: ", mes_lag)
            periodo_mes_ocorrencia = primeiras_ocorrencias_por_data.loc[(primeiras_ocorrencias_por_data["data"].dt.month == mes_lag)].reset_index(drop = True)
            MLT = periodo_mes_ocorrencia["valor"].mean()
            PERC_MLT = (MLT*2.63)*(valores_percentuais[1]/100)
            parcela_afluencia_abatimento_rhs = df_ireg_usi[coluna_lag]*PERC_MLT
            rhs_ini_afl = df_ireg_rhs["RHS_CALC"].head(1).values
            df_ireg_rhs["RHS_CALC"] = df_ireg_rhs["RHS_CALC"] + parcela_afluencia_abatimento_rhs
            
            df_memoria_calculo_rhs_afl = pd.DataFrame(
            {
                "MES_INICIO":[mes_real],
                "RHS_FINAL_VOL":rhs_ini_afl,
                "USINA":[nome_usina],
                "LAG":[contador_lags],
                "MLT":[MLT],
                "FATOR":[2.63],
                "PERC_MLT":[valores_percentuais[1]],
                "CONTRIB":[PERC_MLT],
                "PIAFL":df_ireg_usi[coluna_lag].head(1).values,
                "CALC_AFL":parcela_afluencia_abatimento_rhs.head(1).values,                
                "RHS_FINAL":df_ireg_rhs["RHS_CALC"].head(1).values,                
            }
            )

            
            lista_df_contribuicao_afluencia.append(df_memoria_calculo_rhs_afl)
            contador_lags += 1


    #print(df_ireg_rhs)
    
    df_resultante_memoria_calculo_vol = pd.concat(lista_df_contribuicao_volume).reset_index(drop = True)
    df_resultante_memoria_calculo_vol.to_csv(f"memoria_de_calculo_RHS_VOL_{1}.csv", index  = False)

    df_resultante_memoria_calculo_afl = pd.concat(lista_df_contribuicao_afluencia).reset_index(drop = True)
    df_resultante_memoria_calculo_afl.to_csv(f"memoria_de_calculo_RHS_AFL_{1}.csv", index  = False)


    logging.info(f"Gerando gráfico do par %VU {valores_percentuais[0]} %MLT {valores_percentuais[1]}")
    fig = go.Figure()
    mapa_ireg_linha_y = {}
    for ireg in lista_ireg:
        df_ireg = df_ireg_rhs.loc[(df_ireg_rhs["IREG"] == ireg)].reset_index(drop = True)
        coef_usi = df.loc[(df["IREG"] == ireg) & (df["UHE"] == usina_escolhida)]["PIVARM"].iloc[0]    
        mapa_ireg_linha_y[ireg] = coef_usi*x + df_ireg["RHS_CALC"].iloc[0]
    keys = list(mapa_ireg_linha_y.keys())
    lists = list(mapa_ireg_linha_y.values())
    # PEGA QUAIS SAO OS CORTES COM MAIOR VALOR PARA CADA PONTO E QUAL O CORTE ATIVO RESPECTIVO
    max_values = []
    source_keys = []
    for items in zip(*lists):
        max_val = max(items)
        max_index = items.index(max_val)
        source_key = keys[max_index]
        max_values.append(max_val)
        source_keys.append(source_key)

    titulo_csv = f"cortes_ativos_{nome_usina_escolhida}_%VU_{valores_percentuais[0]}_%MLT_{valores_percentuais[1]}"
    #IMRPIME OS CORTES ATIVOS
    logging.info(f"Imprimindo cortes ativos")
    with open(f"{titulo_csv}.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["source_key", "max_value", "vUtil"])  # header
        for key, value, vutil in zip(source_keys, max_values, x):
            writer.writerow([key, value, vutil])

    ## PLOTA OS CORTES ATIVOS
    fig.add_trace(go.Scatter(x=x, y=max_values, showlegend=False, mode='lines', line=dict(color='red'), name=f'Cortes Ativos'))

    titulo = f"Cortes Ativos Usina {nome_usina_escolhida} %VU {valores_percentuais[0]} %MLT {valores_percentuais[1]}"
    fig.update_layout(
        title=titulo,
        xaxis_title="hm3",
        yaxis_title="$",
        showlegend=True
    )

    fig.write_html(f"{titulo}.html", include_plotlyjs='cdn') 

    end_etapa = time.time()
    logging.info(f"Gerou o gráfico {titulo} em {end_etapa - start_etapa:.4f} segundos")

end = time.time()
logging.info(f"Processo finalizado")
logging.info(f"Tempo de execução do cálculo e geração de gráficos: {end - start_execucao:.4f} segundos")
logging.info(f"Tempo de execução total: {end - start:.4f} segundos")

