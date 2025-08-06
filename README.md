# fcfnw
Ferramenta de análise da FCF do modelo Newave

O script tem como objetivo a visualização de cortes ativos dado o "congelamento" dos eixos de outras usinas da FCF. Nos parâmetros inicias, como "usina_escolhida", "caminho_caso" e "parametros_variacao_volume_util_mlt", é possível configurar o script. 

"usina_escolhida" define o código da usina que será utilizada como base para visualização, enquanto todas as demais usinas serão abatidas do RHS como valores constantes de volume útil para PIVs e volume incremental (após conversão da vazão incremental por 2.63) para PIAFLs.

"caminho_caso" define o caminho do caso em questão, o qual deve ter uma pasta nwlistcf com o arquivo "nwlistcf.rel" do período desejado de visualização da FCF.

"parametros_variacao_volume_util_mlt" define o % do volume útil e % da MLT que serão utilizados para multiplicação do PIV e PIAFL abatendo do RHS, conforme mostra o exemplo:

parametros_variacao_volume_util_mlt = {
    #PERCENTUAL VOLUME UTIL,     PERCENTUAL MLT
    "V1":[            20          ,   80            ],
    "V2":[            40          ,   80            ],
    "V3":[            60          ,   80            ],
    "V4":[            80          ,   80            ],
}

Por fim, é necessário executar o código por meio do seguinte comando (Por exemplo:)

python3 plotaCortesArmazenamento.py