# Logica: (lembrando que o no replica funciona como um backup do no primario)

# 1- os nos replicas do cluster store estabele conexao com no primario (de escuta e de envio) e o no primario estabelece essas respectivas conexoes

# 2- fica escutando para receber requisicao de algum elemento do cluster sync
    # 2.1 - aceitar conexao
    # 2.2 - receber a requisicao

# 3- caso a requisicao recebida seja de escrita:
    # 3.1- se o no que recebeu for o primario: 
        # 3.1.1- executa a escrita
        # 3.1.2- manda a atualização para os nos de backup
        # 3.1.3- recebe o retorno de reconhecimento da atualizacao dos nos de backup
        # 3.1.4- retorna ao elemento do cluster sync um reconhecimento que a escrita foi concluida

    # 3.2- se o no que recebeu for o de backup:
        # 3.2.1- envia a requisicao de escrita para o no primario
        # 3.2.2- recebe do no primario atualizacao a ser feita
        # 3.2.3- retorna ao no primario um reconhecimento da atualizacao
        # 3.2.4- retorna ao elemento do cluster sync um reconhecimento que a escrita foi concluida

# 3- caso a requisicao recebida seja de leitura:
    # 3.1- retorna a leitura ao elemento do cluster sync

# 4- finaliza conexao com aquele elemento do cluster sync e fica escutando para receber alguma requisicao de outro elemento, retornando ao #2


import threading
import random
import socket
from constants import * 

class  noClusterStore:
    def __init__(self, id, host, porta, proximo_no_porta1, proximo_no_host1, proximo_no_porta2 = None, proximo_no_host2 = None):
        self.id = id
        self.host = host
        self.porta = porta
        self.primario = True if id == 0 else False
        self.armazenamento = ""

        if self.pimario:
            self.porta_no_backup1 = proximo_no_porta1
            self.host_backup1 = proximo_no_host1

            self.porta_no_backup2 = proximo_no_porta2
            self.host_backup2 = proximo_no_host2

            self.no_backup1_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.no_backup2_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        else:
            self.porta_no_primario = proximo_no_porta1
            self.host_no_primario = proximo_no_host1
            
            self.no_primario_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.no_clusterSync_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            

    def estabeleConexoes(self):
        if self.primario:
            return
        
        else:
            