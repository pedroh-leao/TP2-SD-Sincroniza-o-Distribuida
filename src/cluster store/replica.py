import socket
import time
import random
import json
import threading
from constants import *
import sys

maximo_inteiro = sys.maxsize

class Replica:
    def __init__(self, id_no, host, )
        






# Logica: (lembrando que o no replica funciona como um backup do no primario)

# 1- o no replica do cluster store estabele conexao com no primario (de escuta e de envio)

# 2- fica escutando para receber requisicao de algum elemento do cluster sync
    # 2.1 - aceitar conexao
    # 2.2 - receber a requisicao

# 3- caso a requisicao recebida seja de escrita:
    # 3.1- envia a requisicao de escrita para o no primario
    # 3.2- recebe do no primario atualizacao a ser feita
    # 3.3- retorna ao no primario um reconhecimento da atualizacao
    # 3.4- retorna ao elemento do cluster sync um reconhecimento que a escrita foi concluida

# 3- caso a requisicao recebida seja de leitura:
    # 3.1- retorna a leitura ao elemento do cluster sync

# 4- finaliza conexao com aquele elemento do cluster sync e fica escutando para receber alguma requisicao de outro elemento